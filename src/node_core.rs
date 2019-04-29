use std::collections::{HashMap};
use std::time::{Duration, Instant};
use std::convert::TryInto;
use std::task::Waker;
use std::thread;

use crossbeam::channel::{Receiver, Sender};
use protobuf::Message as PbMessage;
use raft::{StateRole, RawNode, Config};
use raft::eraftpb::{Message, MessageType, EntryType, ConfChange, ConfChangeType};
use regex::Regex;
use lazy_static::lazy_static;

use crate::{
    TransportItem,
    Transport,
    TransportError,
    MachineCore,
    Storage,
    WrappedStorage,
    NodeResult,
    NodeError,
    CommitError,
    Proposal,
    Answer,
    AnswerKind,
    Context,
    Request,
    RequestKind,
    Response,
    StateChangeResult,
    StateRetrievalResult,
};

lazy_static! {
    static ref ID_RE: Regex = Regex::new("^.*(\\d+)$").unwrap();
}

pub struct NodeCore<M: MachineCore, T: Transport<M>, S: Storage> {
    name: String,
    // None if the raft is not initialized.
    base_config: Config,
    raft_group: Option<RawNode<WrappedStorage<S>>>,
    transports: HashMap<u64, T>,
    proposed: Vec<u64>,
    proposal_id: u64,
    timer: Instant,
    // NOTE: these vectors are a possible source for memory leak
    messages: Vec<Message>,
    proposals: Vec<Proposal<M>>,
    answers: Vec<Answer>,
    request_rx: Receiver<Request<M>>,
    response_txs: HashMap<u64, Sender<Response<M>>>,
    response_wakers: HashMap<u64, Waker>,
    machine: M,
}

impl<M: MachineCore, T: Transport<M>, S: Storage> NodeCore<M, T, S> {
    // Create a raft leader only with itself in its configuration.
    pub fn new<StringLike: Into<String>>(
        name: StringLike,
        base_config: Config,
        machine: M,
        mut storage: S,
        mut node_transports: Vec<T>,
        request_rx: Receiver<Request<M>>,
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

        Ok(Self {
            name: string_name,
            base_config,
            raft_group,
            transports,
            proposed: Default::default(),
            proposal_id: 0,
            timer: Instant::now(),
            messages: Default::default(),
            proposals: Default::default(),
            answers: Default::default(),
            request_rx,
            response_txs: Default::default(),
            response_wakers: Default::default(),
            machine,
        })
    }

    // Initialize raft for followers.
    fn initialize_raft_from_message(
        raft_group: &mut Option<RawNode<WrappedStorage<S>>>,
        name: &String,
        base_config: &Config,
        msg: &Message,
    ) -> NodeResult<()> {
        if raft_group.is_some() {
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
            tag: name.clone(),
            ..base_config.clone()
        };
        let storage = Default::default();
        *raft_group = Some(
            RawNode::new(&cfg, storage)
                .map_err(|e| NodeError::Raft {
                    cause: e,
                })?
        );

        Ok(())
    }

    // Step a raft message, initialize the raft if need.
    fn step(
        raft_group: &mut Option<RawNode<WrappedStorage<S>>>,
        name: &String,
        base_config: &Config,
        msg: Message,
    ) -> NodeResult<()> {
        Self::initialize_raft_from_message(raft_group, name, base_config, &msg)?;
        raft_group
            .as_mut()
            // this option will never be `None` as it gets initialized above
            .unwrap()
            .step(msg)
            .map_err(|e| NodeError::Raft {
                cause: e,
            })
    }

    fn send_response(
        response_txs: &mut HashMap<u64, Sender<Response<M>>>,
        response_wakers: &mut HashMap<u64, Waker>,
        node_name: String,
        answer: Answer,
    ) -> NodeResult<()> {
        let response = match answer.kind {
            AnswerKind::Success => Response::StateChange(StateChangeResult::Success),
            AnswerKind::Fail => Response::StateChange(StateChangeResult::Fail),
        };
        response_txs
            .remove(&answer.id)
            .ok_or(NodeError::AnswerDelivery { node_name: node_name.clone() })
            .and_then(|tx| {
                tx.send(response)
                    .map_err(|_| NodeError::AnswerDelivery { node_name: node_name.clone() })
            })?;
        response_wakers
            .remove(&answer.id)
            .ok_or(NodeError::AnswerDelivery { node_name: node_name.clone() })
            .and_then(|waker| {
                Ok(waker.wake())
            })
    }

    fn get_proposals_from_requests(&mut self, timeout: &Instant) -> NodeResult<Vec<Proposal<M>>> {
        let mut proposals = Vec::new();
        loop {
            let node_id = match self.raft_group {
                Some(ref raft_group) => raft_group.raft.id,
                None => break,
            };

            // receive new requests from the user
            let request = match self.request_rx.try_recv() {
                Ok(request) => request,
                Err(_) => break,
            };

            match request.kind {
                RequestKind::StateChange(state_change) => {
                    let id = self.proposal_id;
                    self.proposal_id += 1;
                    self.response_txs.insert(id, request.response_tx);
                    self.response_wakers.insert(id, request.waker);
                    proposals.push(Proposal::state_change(id, node_id, state_change));
                },
                RequestKind::StateRetrieval(identifier) => {
                    let response = match self.machine.retrieve(&identifier) {
                        // make the value a snapshot (a copy)
                        Ok(value) => Response::StateRetrieval(
                            StateRetrievalResult::Found(value.clone())
                        ),
                        Err(_) => Response::StateRetrieval(StateRetrievalResult::NotFound),
                    };
                    request.response_tx.send(response)
                        .map_err(|_| NodeError::AnswerDelivery { node_name: self.name.clone() })?;
                    request.waker.wake();
                }
            }

            if timeout.elapsed() >= Duration::from_millis(10) {
                break;
            }
        }

        Ok(proposals)
    }

    pub fn advance(&mut self) -> NodeResult<()> {
        let timeout = Instant::now();
        'transports: for (_, transport) in &self.transports {
            loop {
                // step raft messages and save forwarded proposals
                match transport.try_recv() {
                    Ok(TransportItem::Message(msg)) => self.messages.push(msg),
                    Ok(TransportItem::Proposal(p)) => self.proposals.push(p),
                    Ok(TransportItem::Answer(a)) => self.answers.push(a),
                    Err(TransportError::Empty) => break,
                    Err(TransportError::Disconnected) => {
                        //log::warn!("host for raft {} is down!", transport.dest());
                        break;
                    },
                }

                if timeout.elapsed() >= Duration::from_millis(5) {
                    break 'transports;
                }
            }
        }

        for msg in self.messages.drain(..) {
            Self::step(&mut self.raft_group, &self.name, &self.base_config, msg)?;
        }

        for answer in self.answers.drain(..) {
            let proposal_id = answer.id;
            Self::send_response(&mut self.response_txs, &mut self.response_wakers, self.name.clone(), answer)?;
            self.proposed.remove_item(&proposal_id);
        }

        let mut new_proposals = self.get_proposals_from_requests(&timeout)?;
        self.proposals.append(&mut new_proposals);

        let raft_group = match self.raft_group {
            Some(ref mut r) => r,
            // When Node::raft_group is `None` it means the node is not initialized.
            _ => return Ok(()),
        };

        if self.timer.elapsed() >= Duration::from_millis(100) {
            // tick the raft.
            raft_group.tick();
            self.timer = Instant::now();
        }

        // handle saved proposals if we are leader
        if raft_group.raft.state == StateRole::Leader {
            // drain the proposals (consume all)
            for proposal in self.proposals.drain(..) {
                let id = proposal.id();
                let node_id = proposal.origin();
                let answer = match proposal.apply_on(raft_group) {
                    None => {
                        // if applying was successful:
                        // add to proposed vector when proposal originated by us
                        if node_id == raft_group.raft.id {
                            self.proposed.push(id);
                        }
                        continue;
                    },
                    Some(answer) => answer,
                };
                // the client who initiated the proposal is connected to us
                if node_id == raft_group.raft.id {
                    // if applying was not successful, tell the client
                    Self::send_response(&mut self.response_txs, &mut self.response_wakers, self.name.clone(), answer)?;
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
        } else {
            // if we know some leader
            match self.transports.get(&raft_group.raft.leader_id) {
                Some(leader) => {
                    let name = &self.name;
                    // forward proposals to leader
                    for proposal in self.proposals.drain(..) {
                        let id = proposal.id();
                        leader.send(TransportItem::Proposal(proposal))
                            .map_err(|e| NodeError::ProposalForwarding {
                                node_name: name.clone(),
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
                        log::warn!("Transport of leader not available, retrying on next tick...");
                    }
                },
            }
        }

        // handle readies from the raft.
        self.on_ready()?;

        if let Some(duration) = Duration::from_millis(10).checked_sub(timeout.elapsed()) {
            thread::sleep(duration);
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
                log::warn!("send raft message to {} fail, let raft retry it", to);
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
                    log::info!("committed proposal {}", proposal_id);
                    Self::send_response(&mut self.response_txs, &mut self.response_wakers, self.name.clone(), Answer {
                        id: proposal_id,
                        kind: AnswerKind::Success,
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
