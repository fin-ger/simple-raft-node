// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate protobuf;
extern crate raft;
extern crate regex;

use std::collections::{HashMap, VecDeque};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{str, thread};
use std::thread::JoinHandle;

use protobuf::Message as PbMessage;
use raft::storage::MemStorage;
use raft::{prelude::*, StateRole};
use regex::Regex;

fn main() {
    env_logger::init();

    info!("Setting up test");

    // Create 5 mailboxes to send/receive messages. Every node holds a `Receiver` to receive
    // messages from others, and uses the respective `Sender` to send messages to others.
    let (mut tx_vec, mut rx_vec) = (Vec::new(), Vec::new());
    for _ in 0..5 {
        let (tx, rx) = mpsc::channel();
        tx_vec.push(tx);
        rx_vec.push(rx);
    }

    info!("Spawning nodes");
    let mut nodes = Vec::new();
    for (i, rx) in rx_vec.into_iter().enumerate() {
        // A map[peer_id -> sender]. In the example we create 5 nodes, with ids in [1, 5].
        let mailboxes = (1..6u64).zip(tx_vec.iter().cloned()).collect();
        let mut raft = Raft::new(i as u64, rx, mailboxes);
        raft.run();
        nodes.push(raft);
    }

    // Put 100 key-value pairs.
    (0..5u16)
        .filter(|i| {
            let (proposal, rx) = Proposal::normal(*i, "hello, world".to_owned());
            info!("Adding new proposal {}...", i);
            nodes[0].proposals.lock().unwrap().push_back(proposal);
            // After we got a response from `rx`, we can assume the put succeeded and following
            // `get` operations can find the key-value pair.
            let res = rx.recv().unwrap();
            info!("Proposal {} was {}", i, res);
            res
        })
        .count();

}

enum TransportItem {
    Proposal(Proposal),
    Message(Message),
}

struct Raft {
    proposals: Arc<Mutex<VecDeque<Proposal>>>,
    thread_handle: Option<JoinHandle<()>>,
    my_mailbox: Option<Receiver<TransportItem>>,
    mailboxes: Option<HashMap<u64, Sender<TransportItem>>>,
    id: u64,
}

impl Raft {
    fn new(id: u64, my_mailbox: Receiver<TransportItem>, mailboxes: HashMap<u64, Sender<TransportItem>>) -> Self {
        Self {
            proposals: Default::default(),
            thread_handle: None,
            my_mailbox: Some(my_mailbox),
            mailboxes: Some(mailboxes),
            id: id,
        }
    }

    fn run(&mut self) {
        // Tick the raft node per 100ms. So use an `Instant` to trace it.
        let mut t = Instant::now();

        let mut node = Node::create_raft_leader(
            self.id + 1,
            self.my_mailbox.take().unwrap(),
            self.mailboxes.take().unwrap(),
        );
        let proposals = Arc::clone(&self.proposals);

        // Here we spawn the node on a new thread and keep a handle so we can join on them later.
        let handle = thread::spawn(move || loop {
            thread::sleep(Duration::from_millis(10));
            loop {
                // Step raft messages.
                match node.my_mailbox.try_recv() {
                    Ok(TransportItem::Message(msg)) => node.step(msg),
                    Ok(TransportItem::Proposal(p)) => proposals.lock().unwrap().push_back(p),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return,
                }
            }

            let raft_group = match node.raft_group {
                Some(ref mut r) => r,
                // When Node::raft_group is `None` it means the node is not initialized.
                _ => continue,
            };

            if t.elapsed() >= Duration::from_millis(100) {
                // Tick the raft.
                raft_group.tick();
                t = Instant::now();
            }

            // Handle new proposals
            if raft_group.raft.state == StateRole::Leader {
                for p in proposals.lock().unwrap().iter_mut().skip_while(|p| p.proposed > 0) {
                    propose(raft_group, p);
                }
            } else {
                match node.mailboxes.get(&raft_group.status().ss.leader_id) {
                    Some(leader) => {
                        for p in proposals.lock().unwrap().drain(..) {
                            leader.send(TransportItem::Proposal(p)).unwrap();
                        }
                    },
                    None => {
                        //warn!("No leader available to process proposals...");
                    },
                }
            }

            // Handle readies from the raft.
            on_ready(raft_group, &mut node.kv_pairs, &node.mailboxes, &proposals);
        });

        self.thread_handle = Some(handle);
    }
}

struct Node {
    // None if the raft is not initialized.
    raft_group: Option<RawNode<MemStorage>>,
    my_mailbox: Receiver<TransportItem>,
    mailboxes: HashMap<u64, Sender<TransportItem>>,
    // Key-value pairs after applied. `MemStorage` only contains raft logs,
    // so we need an additional storage engine.
    kv_pairs: HashMap<u16, String>,
}

impl Node {
    // Create a raft leader only with itself in its configuration.
    fn create_raft_leader(
        id: u64,
        my_mailbox: Receiver<TransportItem>,
        mailboxes: HashMap<u64, Sender<TransportItem>>,
    ) -> Self {
        let mut cfg = example_config();
        cfg.id = id;
        cfg.peers = vec![1, 2, 3, 4, 5];
        cfg.tag = format!("peer_{}", id);

        let storage = MemStorage::new();
        let raft_group = Some(RawNode::new(&cfg, storage, vec![]).unwrap());
        Node {
            raft_group,
            my_mailbox,
            mailboxes,
            kv_pairs: Default::default(),
        }
    }

    // Create a raft follower.
    fn create_raft_follower(
        my_mailbox: Receiver<TransportItem>,
        mailboxes: HashMap<u64, Sender<TransportItem>>,
    ) -> Self {
        Node {
            raft_group: None,
            my_mailbox,
            mailboxes,
            kv_pairs: Default::default(),
        }
    }

    // Initialize raft for followers.
    fn initialize_raft_from_message(&mut self, msg: &Message) {
        if !is_initial_msg(msg) {
            return;
        }
        let mut cfg = example_config();
        cfg.id = msg.get_to();
        let storage = MemStorage::new();
        self.raft_group = Some(RawNode::new(&cfg, storage, vec![]).unwrap());
    }

    // Step a raft message, initialize the raft if need.
    fn step(&mut self, msg: Message) {
        if self.raft_group.is_none() {
            if is_initial_msg(&msg) {
                self.initialize_raft_from_message(&msg);
            } else {
                return;
            }
        }
        let raft_group = self.raft_group.as_mut().unwrap();
        let _ = raft_group.step(msg);
    }
}

fn on_ready(
    raft_group: &mut RawNode<MemStorage>,
    kv_pairs: &mut HashMap<u16, String>,
    mailboxes: &HashMap<u64, Sender<TransportItem>>,
    proposals: &Arc<Mutex<VecDeque<Proposal>>>,
) {
    if !raft_group.has_ready() {
        return;
    }
    // Get the `Ready` with `RawNode::ready` interface.
    let mut ready = raft_group.ready();

    // Persistent raft logs. It's necessary because in `RawNode::advance` we stabilize
    // raft logs to the latest position.
    if let Err(e) = raft_group.raft.raft_log.store.wl().append(ready.entries()) {
        error!("persist raft log fail: {:?}, need to retry or panic", e);
        return;
    }

    // Send out the messages come from the node.
    for msg in ready.messages.drain(..) {
        let to = msg.get_to();
        if mailboxes[&to].send(TransportItem::Message(msg)).is_err() {
            warn!("send raft message to {} fail, let raft retry it", to);
        }
    }

    // Apply all committed proposals.
    if let Some(committed_entries) = ready.committed_entries.take() {
        for entry in committed_entries {
            if entry.get_data().is_empty() {
                // From new elected leaders.
                continue;
            }
            if let EntryType::EntryConfChange = entry.get_entry_type() {
                // For conf change messages, make them effective.
                let mut cc = ConfChange::new();
                cc.merge_from_bytes(entry.get_data()).unwrap();
                let node_id = cc.get_node_id();
                match cc.get_change_type() {
                    ConfChangeType::AddNode => raft_group.raft.add_node(node_id).unwrap(),
                    ConfChangeType::RemoveNode => raft_group.raft.remove_node(node_id).unwrap(),
                    ConfChangeType::AddLearnerNode => raft_group.raft.add_learner(node_id).unwrap(),
                    ConfChangeType::BeginMembershipChange
                    | ConfChangeType::FinalizeMembershipChange => unimplemented!(),
                }
            } else {
                // For normal proposals, extract the key-value pair and then
                // insert them into the kv engine.
                let data = str::from_utf8(entry.get_data()).unwrap();
                let reg = Regex::new("put ([0-9]+) (.+)").unwrap();
                if let Some(caps) = reg.captures(&data) {
                    kv_pairs.insert(caps[1].parse().unwrap(), caps[2].to_string());
                }
            }
            if raft_group.raft.state == StateRole::Leader {
                // The leader should response to the clients, tell them if their proposals
                // succeeded or not.
                let proposal = proposals.lock().unwrap().pop_front().unwrap();
                proposal.propose_success.send(true).unwrap();
            }
        }
    }
    // Call `RawNode::advance` interface to update position flags in the raft.
    raft_group.advance(ready);
}

fn example_config() -> Config {
    Config {
        election_tick: 10,
        heartbeat_tick: 3,
        ..Default::default()
    }
}

// The message can be used to initialize a raft node or not.
fn is_initial_msg(msg: &Message) -> bool {
    let msg_type = msg.get_msg_type();
    msg_type == MessageType::MsgRequestVote
        || msg_type == MessageType::MsgRequestPreVote
        || (msg_type == MessageType::MsgHeartbeat && msg.get_commit() == 0)
}

struct Proposal {
    normal: Option<(u16, String)>, // key is an u16 integer, and value is a string.
    conf_change: Option<ConfChange>, // conf change.
    transfer_leader: Option<u64>,
    // If it's proposed, it will be set to the index of the entry.
    proposed: u64,
    propose_success: SyncSender<bool>,
}

impl Proposal {
    fn conf_change(cc: &ConfChange) -> (Self, Receiver<bool>) {
        let (tx, rx) = mpsc::sync_channel(1);
        let proposal = Proposal {
            normal: None,
            conf_change: Some(cc.clone()),
            transfer_leader: None,
            proposed: 0,
            propose_success: tx,
        };
        (proposal, rx)
    }

    fn normal(key: u16, value: String) -> (Self, Receiver<bool>) {
        let (tx, rx) = mpsc::sync_channel(1);
        let proposal = Proposal {
            normal: Some((key, value)),
            conf_change: None,
            transfer_leader: None,
            proposed: 0,
            propose_success: tx,
        };
        (proposal, rx)
    }
}

fn propose(raft_group: &mut RawNode<MemStorage>, proposal: &mut Proposal) {
    let last_index1 = raft_group.raft.raft_log.last_index() + 1;
    if let Some((ref key, ref value)) = proposal.normal {
        let data = format!("put {} {}", key, value).into_bytes();
        let _ = raft_group.propose(vec![], data);
    } else if let Some(ref cc) = proposal.conf_change {
        let _ = raft_group.propose_conf_change(vec![], cc.clone());
    } else if let Some(_tranferee) = proposal.transfer_leader {
        // TODO: implement tranfer leader.
        unimplemented!();
    }

    let last_index2 = raft_group.raft.raft_log.last_index() + 1;
    if last_index2 == last_index1 {
        // Propose failed, don't forget to respond to the client.
        proposal.propose_success.send(false).unwrap();
    } else {
        proposal.proposed = last_index1;
    }
}
