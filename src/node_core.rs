use std::collections::{HashMap};
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::time::{Duration, Instant};
use std::thread;
use std::convert::TryInto;
use std::fmt::Debug;

use protobuf::Message as PbMessage;
use raft::{StateRole, RawNode, Config, storage::MemStorage};
use raft::eraftpb::{Message, MessageType, EntryType, ConfChange, ConfChangeType};
use regex::Regex;
use log::{info, warn, error};

use crate::proposals::{Proposal, Answer, Context};
use crate::transport::TransportItem;

pub struct NodeCore {
    id: u64,
    // None if the raft is not initialized.
    raft_group: Option<RawNode<MemStorage>>,
    my_mailbox: Receiver<TransportItem>,
    mailboxes: HashMap<u64, Sender<TransportItem>>,
    proposed: Vec<u64>,
    proposals: Receiver<Proposal>,
    answers: Sender<Answer>,
    machine: HashMap<u16, String>,
}

/*
 * TODO:
 *  - cleanup config handling
 *  - replace bincode with protobuf
 *  - add state change type and remove regex
 *  - add state machine type and replace hashmap
 *  - make storage configurable and remove MemStorage
 *  - replace Senders and Receivers of TransportItem with Transport trait
 */

impl NodeCore {
    // Create a raft leader only with itself in its configuration.
    pub fn new(
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

        let storage = Default::default();
        let raft_group = Some(RawNode::new(&cfg, storage, vec![]).unwrap());
        Self {
            id,
            raft_group,
            my_mailbox,
            mailboxes,
            proposed: Default::default(),
            proposals,
            answers,
            machine: Default::default(),
        }
    }

    // Initialize raft for followers.
    fn initialize_raft_from_message(&mut self, msg: &Message) {
        if self.raft_group.is_some() {
            return;
        }

        // if not initial message
        let msg_type = msg.get_msg_type();
        if msg_type != MessageType::MsgRequestVote
            && msg_type != MessageType::MsgRequestPreVote
            && !(msg_type == MessageType::MsgHeartbeat && msg.get_commit() == 0) {
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
        let storage = Default::default();
        self.raft_group = Some(RawNode::new(&cfg, storage, vec![]).unwrap());
    }

    // Step a raft message, initialize the raft if need.
    fn step(&mut self, msg: Message) {
        self.initialize_raft_from_message(&msg);
        self.raft_group
            .as_mut()
            .unwrap()
            .step(msg)
            .unwrap();
    }

    pub fn run(mut self) {
        // Tick the raft node per 100ms. So use an `Instant` to trace it.
        let mut t = Instant::now();
        let mut proposals = Vec::new();

        'cycle: loop {
            thread::sleep(Duration::from_millis(10));

            loop {
                // step raft messages and save forwarded proposals
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
                // save all new proposal requests
                match self.proposals.try_recv() {
                    Ok(mut proposal) => {
                        proposal.set_origin(raft_group.raft.id);
                        proposals.push(proposal);
                    },
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break 'cycle,
                }
            }

            if t.elapsed() >= Duration::from_millis(100) {
                // tick the raft.
                raft_group.tick();
                t = Instant::now();
            }

            // handle saved proposals if we are leader
            if raft_group.raft.state == StateRole::Leader {
                // drain the proposals (consume all)
                for proposal in proposals.drain(..) {
                    let id = proposal.id();
                    let node_id = proposal.origin();
                    // if applying was successful:
                    // add to proposed vector when proposal originated by us
                    // TODO: handle apply_on errors
                    if proposal.apply_on(raft_group) {
                        if node_id == raft_group.raft.id {
                            self.proposed.push(id);
                        }
                    } else {
                        // if applying was not successful, tell the client
                        // FIXME: this is broken when proposal was forwarded as node_id of
                        //        proposal is not ours
                        self.answers.send(Answer {
                            id,
                            value: false,
                        }).unwrap();
                    }
                }
            } else {
                // if we know some leader
                match self.mailboxes.get(&raft_group.status().ss.leader_id) {
                    Some(leader) => {
                        // forward proposals to leader
                        for proposal in proposals.drain(..) {
                            let id = proposal.id();
                            leader.send(TransportItem::Proposal(proposal)).unwrap();
                            // proposal was forwarded and can therefore be put in our proposed
                            // list to prepare for client answer
                            self.proposed.push(id);
                        }
                    },
                    None => {
                        // FIXME: display the following warning only after first leader election
                        //warn!("No leader available to process proposals...");
                    },
                }
            }

            // handle readies from the raft.
            self.on_ready();
        }

        // print key-value store (machine) for debug purposes
        // FIXME: remove when debugging done
        info!("[{}] {:#?}", self.id, self.machine);
    }

    fn on_ready(&mut self) {
        // FIXME: return should never be reached
        let raft_group = match self.raft_group {
            Some(ref mut raft_group) => raft_group,
            None => return,
        };

        // if raft is not initialized, return
        if !raft_group.has_ready() {
            return;
        }

        // get the `Ready` with `RawNode::ready` interface.
        let mut ready = raft_group.ready();

        // persistent raft logs. It's necessary because in `RawNode::advance` we stabilize
        // raft logs to the latest position.
        if let Err(e) = raft_group.raft.raft_log.store.wl().append(ready.entries()) {
            // FIXME: handle
            error!("persist raft log fail: {:?}, need to retry or panic", e);
            return;
        }

        // send out the messages from this node
        for msg in ready.messages.drain(..) {
            let to = msg.get_to();
            if self.mailboxes[&to].send(TransportItem::Message(msg)).is_err() {
                warn!("send raft message to {} fail, let raft retry it", to);
            }
        }

        // apply all committed proposals
        if let Some(committed_entries) = ready.committed_entries.take() {
            for entry in committed_entries {
                if entry.get_data().is_empty() {
                    // from new elected leaders.
                    continue;
                }
                if let EntryType::EntryConfChange = entry.get_entry_type() {
                    // apply configuration changes
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
                    // for state change proposals, extract the key-value pair and then
                    // insert them into the machine.
                    // TODO: use state change interface of machine here and remove regex
                    let data = std::str::from_utf8(entry.get_data()).unwrap();
                    let reg = Regex::new("put ([0-9]+) (.+)").unwrap();
                    if let Some(caps) = reg.captures(&data) {
                        self.machine.insert(caps[1].parse().unwrap(), caps[2].to_string());
                    }
                }

                // check if the proposal had a context attached to it
                let Context { node_id, proposal_id } = match entry.get_context().try_into() {
                    Ok(context) => context,
                    Err(_) => continue,
                };

                // if the context contains our node_id and one of our proposed proposals
                // answer the client
                // FIXME: this might also be a failure as proposed proposals can still fail after
                //        forwarding
                if node_id == raft_group.raft.id && self.proposed.contains(&proposal_id) {
                    self.answers.send(Answer {
                        id: proposal_id,
                        value: true,
                    }).unwrap();
                    self.proposed.remove_item(&proposal_id);
                }
            }
        }
        
        // call `RawNode::advance` interface to update position flags in the raft.
        raft_group.advance(ready);
    }
}
