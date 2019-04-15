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
#![feature(vec_remove_item)]

#[macro_use]
extern crate log;
extern crate env_logger;
extern crate protobuf;
extern crate raft;
extern crate regex;

use std::collections::{HashMap};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::time::{Duration, Instant};
use std::thread::{self, JoinHandle};
use std::convert::{TryInto, TryFrom};

use protobuf::Message as PbMessage;
use raft::storage::MemStorage;
use raft::{prelude::*, StateRole};
use regex::Regex;
use serde_derive::{Deserialize, Serialize};

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
        let raft = AsyncSimpleNode::new(i as u64 + 1, rx, mailboxes);
        nodes.push(raft);
    }

    // Put 100 key-value pairs.
    (0..5u64)
        .filter(|i| {
            let proposal = Proposal::new(
                *i,
                ProposalKind::StateChange(*i as u16, "hello, world".to_string())
            );
            info!("Adding new proposal {}...", i);
            let res = nodes[0].propose(proposal);
            info!("Proposal {} was {}", i, res);
            res
        })
        .count();

    for node in nodes {
        node.finalize();
    }
}

enum TransportItem {
    Proposal(Proposal),
    Message(Message),
}

struct AsyncSimpleNode {
    thread_handle: JoinHandle<()>,
    proposals_tx: Sender<Proposal>,
    answers_rx: Receiver<Answer>,
}

impl AsyncSimpleNode {
    fn new(
        id: u64,
        my_mailbox: Receiver<TransportItem>,
        mailboxes: HashMap<u64, Sender<TransportItem>>,
    ) -> Self {
        let (proposals_tx, proposals_rx) = mpsc::channel();
        let (answers_tx, answers_rx) = mpsc::channel();
        let node = SimpleNode::new(id, my_mailbox, mailboxes, proposals_rx, answers_tx);
        // Here we spawn the node on a new thread and keep a handle so we can join on them later.
        let handle = thread::spawn(|| {
            node.run();
        });

        Self {
            thread_handle: handle,
            proposals_tx,
            answers_rx,
        }
    }

    fn propose(&mut self, proposal: Proposal) -> bool {
        let id = proposal.context.proposal_id;
        self.proposals_tx.send(proposal).unwrap();
        let answer = self.answers_rx.recv().unwrap();

        if answer.id != id {
            error!("Proposal id not identical to answer id!");
            return false;
        }

        return answer.value;
    }

    fn finalize(self) {
        drop(self.proposals_tx);
        drop(self.answers_rx);
        self.thread_handle.join().unwrap();
    }
}

struct SimpleNode {
    id: u64,
    // None if the raft is not initialized.
    raft_group: Option<RawNode<MemStorage>>,
    my_mailbox: Receiver<TransportItem>,
    mailboxes: HashMap<u64, Sender<TransportItem>>,
    proposed: Vec<u64>,
    proposals: Receiver<Proposal>,
    answers: Sender<Answer>,
    // Key-value pairs after applied. `MemStorage` only contains raft logs,
    // so we need an additional storage engine.
    kv_pairs: HashMap<u16, String>,
}

impl SimpleNode {
    // Create a raft leader only with itself in its configuration.
    fn new(
        id: u64,
        my_mailbox: Receiver<TransportItem>,
        mailboxes: HashMap<u64, Sender<TransportItem>>,
        proposals: Receiver<Proposal>,
        answers: Sender<Answer>,
    ) -> Self {
        let cfg = Config {
            election_tick: 10,
            heartbeat_tick: 3,
            id,
            peers: vec![1, 2, 3, 4, 5],
            tag: format!("peer_{}", id),
            ..Default::default()
        };

        let storage = MemStorage::new();
        let raft_group = Some(RawNode::new(&cfg, storage, vec![]).unwrap());
        Self {
            id,
            raft_group,
            my_mailbox,
            mailboxes,
            proposed: Default::default(),
            proposals,
            answers,
            kv_pairs: Default::default(),
        }
    }

    // Initialize raft for followers.
    fn initialize_raft_from_message(&mut self, msg: &Message) {
        if !is_initial_msg(msg) {
            return;
        }
        let id = msg.get_to();
        let cfg = Config {
            election_tick: 10,
            heartbeat_tick: 3,
            id,
            tag: format!("peer_{}", id),
            ..Default::default()
        };
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

    fn run(mut self) {
        // Tick the raft node per 100ms. So use an `Instant` to trace it.
        let mut t = Instant::now();
        let mut proposals = Vec::new();

        'cycle: loop {
            thread::sleep(Duration::from_millis(10));

            loop {
                // Step raft messages.
                match self.my_mailbox.try_recv() {
                    Ok(TransportItem::Message(msg)) => self.step(msg),
                    Ok(TransportItem::Proposal(p)) => proposals.push(p),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break 'cycle,
                }
            }

            let raft_group = match self.raft_group {
                Some(ref mut r) => r,
                // When Node::raft_group is `None` it means the node is not initialized.
                _ => continue,
            };

            loop {
                // Get all new proposal requests
                match self.proposals.try_recv() {
                    Ok(proposal) => proposals.push(Proposal {
                        context: Context {
                            node_id: raft_group.raft.id,
                            ..proposal.context
                        },
                        ..proposal
                    }),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break 'cycle,
                }
            }

            if t.elapsed() >= Duration::from_millis(100) {
                // Tick the raft.
                raft_group.tick();
                t = Instant::now();
            }

            // Handle new proposals
            if raft_group.raft.state == StateRole::Leader {
                for proposal in proposals.drain(..) {
                    let id = proposal.context.proposal_id;
                    let node_id = proposal.context.node_id;
                    if proposal.apply_on(raft_group) {
                        if node_id == raft_group.raft.id {
                            self.proposed.push(id);
                        }
                    } else {
                        self.answers.send(Answer {
                            id,
                            value: false,
                        }).unwrap();
                    }
                }
            } else {
                match self.mailboxes.get(&raft_group.status().ss.leader_id) {
                    Some(leader) => {
                        for proposal in proposals.drain(..) {
                            self.proposed.push(proposal.context.proposal_id);
                            leader.send(TransportItem::Proposal(proposal)).unwrap();
                        }
                    },
                    None => {
                        //warn!("No leader available to process proposals...");
                    },
                }
            }

            // Handle readies from the raft.
            self.on_ready();
        }

        for (key, value) in self.kv_pairs {
            info!("[{}] {}: {}", self.id, key, value);
        }
    }

    fn on_ready(&mut self) {
        let raft_group = match self.raft_group {
            Some(ref mut raft_group) => raft_group,
            None => return,
        };

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
            if self.mailboxes[&to].send(TransportItem::Message(msg)).is_err() {
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
                    let data = std::str::from_utf8(entry.get_data()).unwrap();
                    let reg = Regex::new("put ([0-9]+) (.+)").unwrap();
                    if let Some(caps) = reg.captures(&data) {
                        self.kv_pairs.insert(caps[1].parse().unwrap(), caps[2].to_string());
                    }
                }

                let Context { node_id, proposal_id } = match entry.get_context().try_into() {
                    Ok(context) => context,
                    Err(_) => continue,
                };

                if node_id == raft_group.raft.id && self.proposed.contains(&proposal_id) {
                    self.answers.send(Answer {
                        id: proposal_id,
                        value: true,
                    }).unwrap();
                    self.proposed.remove_item(&proposal_id);
                }
            }
        }
        // Call `RawNode::advance` interface to update position flags in the raft.
        raft_group.advance(ready);
    }
}

// The message can be used to initialize a raft node or not.
fn is_initial_msg(msg: &Message) -> bool {
    let msg_type = msg.get_msg_type();
    msg_type == MessageType::MsgRequestVote
        || msg_type == MessageType::MsgRequestPreVote
        || (msg_type == MessageType::MsgHeartbeat && msg.get_commit() == 0)
}

#[derive(Serialize, Deserialize)]
struct Context {
    proposal_id: u64,
    node_id: u64,
}

impl TryInto<Vec<u8>> for Context {
    type Error = bincode::Error;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        bincode::serialize(&self)
    }
}

impl TryFrom<&[u8]> for Context {
    type Error = bincode::Error;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        bincode::deserialize(&data)
    }
}

enum ProposalKind {
    StateChange(u16, String),
    ConfChange(ConfChange),
    TransferLeader(u64),
}

struct Proposal {
    context: Context,
    kind: ProposalKind,
}

impl Proposal {
    pub fn new(id: u64, kind: ProposalKind) -> Self {
        Self {
            context: Context {
                proposal_id: id,
                node_id: 0,
            },
            kind,
        }
    }

    pub fn apply_on<T: raft::Storage>(self, raft_group: &mut RawNode<T>) -> bool {
        let last_index1 = raft_group.raft.raft_log.last_index() + 1;
        let context = self.context.try_into().unwrap();
        match self.kind {
            ProposalKind::StateChange(ref key, ref value) => {
                let data = format!("put {} {}", key, value).into_bytes();
                let _ = raft_group.propose(context, data);
            },
            ProposalKind::ConfChange(ref conf_change) => {
                let _ = raft_group.propose_conf_change(context, conf_change.clone());
            },
            ProposalKind::TransferLeader(ref _transferee) => {
                // TODO: implement tranfer leader.
                unimplemented!();
            },
        };
        let last_index2 = raft_group.raft.raft_log.last_index() + 1;
        return last_index2 != last_index1;
    }
}

struct Answer {
    id: u64,
    value: bool,
}
