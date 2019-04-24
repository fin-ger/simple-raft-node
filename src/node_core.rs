use std::collections::{HashMap};
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::time::{Duration, Instant};
use std::thread;
use std::convert::TryInto;

use protobuf::Message as PbMessage;
use raft::{StateRole, RawNode, Config};
use raft::eraftpb::{Message, MessageType, EntryType, ConfChange, ConfChangeType};
use regex::Regex;
use log::warn;
use lazy_static::lazy_static;

use crate::{
    TransportItem,
    Transport,
    TransportError,
    Machine,
    Storage,
    WrappedStorage,
    NodeResult,
    NodeError,
    CommitError,
    Proposal,
    Answer,
    Context,
};

lazy_static! {
    static ref ID_RE: Regex = Regex::new("^.*(\\d+)$").unwrap();
}

pub struct NodeCore<M: Machine, T: Transport<M>, S: Storage> {
    name: String,
    // None if the raft is not initialized.
    base_config: Config,
    raft_group: Option<RawNode<WrappedStorage<S>>>,
    transports: HashMap<u64, T>,
    proposed: Vec<u64>,
    proposals: Receiver<Proposal<M>>,
    answers: Sender<Answer>,
    machine: M,
}

impl<M: Machine, T: Transport<M>, S: Storage> NodeCore<M, T, S> {
    // Create a raft leader only with itself in its configuration.
    pub fn new<StringLike: Into<String>>(
        name: StringLike,
        base_config: Config,
        mut machine: M,
        mut storage: S,
        mut node_transports: Vec<T>,
        proposals: Receiver<Proposal<M>>,
        answers: Sender<Answer>,
    ) -> NodeResult<Self> {
        let string_name = name.into();
        let id = match ID_RE.captures(&string_name) {
            Some(caps) => {
                caps
                    .get(1)
                    .unwrap()
                    .as_str()
                    .parse::<u64>()
                    .map_err(|e| NodeError::InvalidIdInName {
                        name: string_name.clone(),
                        cause: e,
                    })?
            },
            None => {
                return Err(NodeError::NoIdInName {
                    name: string_name,
                });
            },
        };

        let cfg = Config {
            id,
            tag: string_name.clone(),
            ..base_config.clone()
        };

        let nodes: Vec<_> = node_transports.iter()
            .map(|t| t.dest())
            .chain(std::iter::once(id))
            .collect();

        let transports = node_transports
            .drain(..)
            .map(|t| (t.dest(), t))
            .collect();

        storage.init_with_conf_state(string_name.clone(), (nodes, vec![]))
            .map_err(|_| NodeError::StorageInit)?;
        let wrapped_storage = WrappedStorage::new(storage);

        let raft_group = Some(
            RawNode::new(&cfg, wrapped_storage)
                .map_err(|e| NodeError::Raft { cause: e })?
        );

        machine.initialize(string_name.clone());

        Ok(Self {
            name: string_name,
            base_config,
            raft_group,
            transports,
            proposed: Default::default(),
            proposals,
            answers,
            machine,
        })
    }

    // Initialize raft for followers.
    fn initialize_raft_from_message(&mut self, msg: &Message) -> NodeResult<()> {
        if self.raft_group.is_some() {
            return Ok(());
        }

        // if not initial message
        let msg_type = msg.get_msg_type();
        if msg_type != MessageType::MsgRequestVote
            && msg_type != MessageType::MsgRequestPreVote
            && !(msg_type == MessageType::MsgHeartbeat && msg.get_commit() == 0) {
                return Ok(());
            }

        let id = msg.get_to();
        // TODO: check if this code is really needed
        let cfg = Config {
            id,
            tag: self.name.clone(),
            ..self.base_config.clone()
        };
        let storage = Default::default();
        self.raft_group = Some(
            RawNode::new(&cfg, storage)
                .map_err(|e| NodeError::Raft {
                    cause: e,
                })?
        );

        Ok(())
    }

    // Step a raft message, initialize the raft if need.
    fn step(&mut self, msg: Message) -> NodeResult<()> {
        self.initialize_raft_from_message(&msg)?;
        self.raft_group
            .as_mut()
            // this option will never be `None` as it gets initialized above
            .unwrap()
            .step(msg)
            .map_err(|e| NodeError::Raft {
                cause: e,
            })
    }

    pub fn run(mut self) -> NodeResult<()> {
        // Tick the raft node per 100ms. So use an `Instant` to trace it.
        let mut t = Instant::now();
        // NOTE: these vectors are a possible source for memory leak
        let mut proposals = Vec::new();
        let mut messages = Vec::new();
        let mut answers = Vec::new();

        'cycle: loop {
            thread::sleep(Duration::from_millis(10));

            for (_, transport) in &self.transports {
                loop {
                    // step raft messages and save forwarded proposals
                    match transport.try_recv() {
                        Ok(TransportItem::Message(msg)) => messages.push(msg),
                        Ok(TransportItem::Proposal(p)) => proposals.push(p),
                        Ok(TransportItem::Answer(a)) => answers.push(a),
                        Err(TransportError::Empty) => break,
                        Err(TransportError::Disconnected) => break 'cycle,
                    }
                }
            }

            for msg in messages.drain(..) {
                self.step(msg)?;
            }

            for answer in answers.drain(..) {
                self.answers
                    .send(answer)
                    .map_err(|e| NodeError::AnswerDelivery {
                        node_name: self.name.clone(),
                        cause: e,
                    })?;
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
                    match proposal.apply_on(raft_group) {
                        Ok(()) => {
                            // if applying was successful:
                            // add to proposed vector when proposal originated by us
                            if node_id == raft_group.raft.id {
                                self.proposed.push(id);
                            }
                        },
                        Err(err) => {
                            warn!("Failed to apply proposal: {}", err);
                            let answer = Answer {
                                id,
                                value: false,
                            };
                            let name = self.name.clone();
                            // the client who initiated the proposal is connected to us
                            if node_id == raft_group.raft.id {
                                // if applying was not successful, tell the client
                                self.answers
                                    .send(answer)
                                    .map_err(|e| NodeError::AnswerDelivery {
                                        node_name: name,
                                        cause: e,
                                    })?;
                            } else {
                                self.transports
                                    .get(&node_id)
                                    .ok_or(NodeError::NoTransportForNode {
                                        other_node: node_id,
                                        this_node: raft_group.raft.id,
                                    })
                                    .and_then(|transport| {
                                        transport.send(TransportItem::Answer(answer))
                                            .map_err(|e| NodeError::AnswerForwarding {
                                                origin_node: node_id,
                                                this_node: raft_group.raft.id,
                                                cause: e,
                                            })
                                    })?;
                            }
                        }
                    }
                }
            } else {
                // if we know some leader
                match self.transports.get(&raft_group.raft.leader_id) {
                    Some(leader) => {
                        // forward proposals to leader
                        for proposal in proposals.drain(..) {
                            let id = proposal.id();
                            leader.send(TransportItem::Proposal(proposal))
                                .map_err(|e| NodeError::ProposalForwarding {
                                    node_name: self.name.clone(),
                                    cause: e,
                                })?;
                            // proposal was forwarded and can therefore be put in our proposed
                            // list to prepare for client answer
                            self.proposed.push(id);
                        }
                    },
                    None => {
                        // display the warning only after first leader election
                        if raft_group.raft.leader_id > 0 {
                            warn!("Transport of leader not available, retrying on next tick...");
                        }
                    },
                }
            }

            // handle readies from the raft.
            self.on_ready()?;
        }

        Ok(())
    }

    fn on_ready(&mut self) -> NodeResult<()> {
        let raft_group = match self.raft_group {
            Some(ref mut raft_group) => raft_group,
            None => unreachable!(),
        };

        // TODO: implement full processing-the-ready-state chapter from raft documentation

        // if raft is not initialized, return
        if !raft_group.has_ready() {
            return Ok(());
        }

        // get the `Ready` with `RawNode::ready` interface.
        let mut ready = raft_group.ready();

        // persistent raft logs. It's necessary because in `RawNode::advance` we stabilize
        // raft logs to the latest position.
        raft_group.raft.raft_log.store
            .writable()
            .append(ready.entries())
            .map_err(|e| NodeError::StorageAppend {
                cause: e,
            })?;

        // send out the messages from this node
        for msg in ready.messages.drain(..) {
            let to = msg.get_to();
            if self.transports[&to].send(TransportItem::Message(msg)).is_err() {
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
                match entry.get_entry_type() {
                    EntryType::EntryConfChange => {
                        // apply configuration changes
                        let mut cc = ConfChange::new();
                        let name = self.name.clone();
                        cc.merge_from_bytes(entry.get_data())
                            .map_err(|e| CommitError::ConfChangeDeserialization { cause: e })
                            .and_then(|_| {
                                let node_id = cc.get_node_id();
                                match cc.get_change_type() {
                                    ConfChangeType::AddNode => {
                                        raft_group.raft.add_node(node_id)
                                    },
                                    ConfChangeType::RemoveNode => {
                                        raft_group.raft.remove_node(node_id)
                                    },
                                    ConfChangeType::AddLearnerNode => {
                                        raft_group.raft.add_learner(node_id)
                                    },
                                    ConfChangeType::BeginMembershipChange
                                        | ConfChangeType::FinalizeMembershipChange => {
                                            unimplemented!()
                                        },
                                }.map_err(|e| CommitError::ConfChange { cause: e })
                            })
                            .map_err(|e| NodeError::ProposalCommit {
                                node_name: name,
                                cause: e,
                            })?;
                    },
                    EntryType::EntryNormal => {
                        // for state change proposals, tell the machine to change its state.
                        let name = self.name.clone();
                        let state_change = bincode::deserialize(entry.get_data())
                            .map_err(|e| NodeError::ProposalCommit {
                                node_name: name,
                                cause: CommitError::StateChangeDeserialization {
                                    cause: e,
                                },
                            })?;
                        self.machine.apply(state_change);
                    },
                }

                // check if the proposal had a context attached to it
                let Context { node_id, proposal_id } = match entry.get_context().try_into() {
                    Ok(context) => context,
                    Err(_) => continue,
                };

                // if the context contains our node_id and one of our proposed proposals
                // answer the client
                if node_id == raft_group.raft.id && self.proposed.contains(&proposal_id) {
                    let name = self.name.clone();
                    self.answers.send(Answer {
                        id: proposal_id,
                        value: true, // entry committed so, it's a success
                    }).map_err(|e| NodeError::AnswerDelivery {
                        node_name: name,
                        cause: e,
                    })?;
                    self.proposed.remove_item(&proposal_id);
                }
            }
        }

        // call `RawNode::advance` interface to update position flags in the raft.
        raft_group.advance(ready);

        Ok(())
    }
}
