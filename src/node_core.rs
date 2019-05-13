use std::collections::{HashMap};
use std::time::{Duration, Instant};
use std::convert::TryInto;
use std::task::Waker;
use std::thread;

use failure::Backtrace;
use crossbeam::channel::{Receiver, Sender};
use protobuf::Message as PbMessage;
use raft::{StateRole, RawNode, Config};
use raft::eraftpb::{
    Message,
    MessageType,
    EntryType,
    ConfChange,
    ConfChangeType,
    Snapshot,
    HardState,
    ConfState,
};

use crate::{
    TransportItem,
    Transport,
    TransportError,
    MachineCore,
    Storage,
    WrappedStorage,
    NodeResult,
    NodeError,
    Proposal,
    ProposeError,
    Answer,
    AnswerKind,
    Context,
    Request,
    RequestKind,
    Response,
    GeneralResult,
    StateRetrievalResult,
    ConnectionManager,
};

// TODO change name to node_id (u64)
pub struct NodeCore<M: MachineCore, C: ConnectionManager<M>, S: Storage> {
    id: u64,
    base_config: Config,
    // None if the raft is not initialized.
    raft_group: Option<RawNode<WrappedStorage<S>>>,
    connection_manager: C,
    new_transports: Vec<C::Transport>,
    transports: HashMap<u64, C::Transport>,
    proposed: Vec<u64>,
    proposals: Vec<Proposal<M>>,
    proposal_id: u64,
    timer: Instant,
    request_rx: Receiver<Request<M>>,
    response_txs: HashMap<u64, Sender<Response<M>>>,
    response_wakers: HashMap<u64, Waker>,
    machine: M,
}

impl<M: MachineCore, C: ConnectionManager<M>, S: Storage> NodeCore<M, C, S> {
    // Create a raft leader only with itself in its configuration.
    pub fn new(
        base_config: Config,
        gateway: Option<<C::Transport as Transport<M>>::Address>,
        machine: M,
        mut storage: S,
        mut connection_manager: C,
        request_rx: Receiver<Request<M>>,
    ) -> NodeResult<Self> {
        log::debug!("creating core for node {} with gateway {:?}...", base_config.id, gateway);

        let mut new_transports = Vec::new();
        let conf_state = if let Some(gateway) = gateway {
            let mut new_transport = connection_manager.connect(&gateway)
                .map_err(|e| NodeError::GatewayConnect {
                    node_id: base_config.id,
                    address: Box::new(gateway.clone()),
                    cause: Box::new(e),
                    backtrace: Backtrace::new(),
                })?;
            new_transport.send(TransportItem::Hello(base_config.id))
                .map_err(|e| NodeError::GatewayConnect {
                    node_id: base_config.id,
                    address: Box::new(gateway.clone()),
                    cause: Box::new(e),
                    backtrace: Backtrace::new(),
                })?;
            new_transports.push(new_transport);
            (vec![], vec![])
        } else {
            (vec![base_config.id], vec![])
        };

        storage.init_with_conf_state(base_config.id, conf_state)
            .map_err(|e| NodeError::Storage {
                node_id: base_config.id,
                cause: Box::new(e),
                backtrace: Backtrace::new(),
            })?;
        let wrapped_storage = WrappedStorage::new(storage);

        let raft_group = Some(
            RawNode::new(&base_config, wrapped_storage)
                .map_err(|e| NodeError::Raft {
                    node_id: base_config.id,
                    cause: e,
                    backtrace: Backtrace::new(),
                })?
        );

        Ok(Self {
            id: base_config.id,
            base_config,
            raft_group,
            connection_manager,
            new_transports,
            transports: HashMap::new(),
            proposed: Default::default(),
            proposals: Default::default(),
            proposal_id: 0,
            timer: Instant::now(),
            request_rx,
            response_txs: Default::default(),
            response_wakers: Default::default(),
            machine,
        })
    }

    pub fn id(&self) -> u64 {
        self.id
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
            tag: format!("node_{}", id),
            ..self.base_config.clone()
        };
        let storage = Default::default();
        self.raft_group = Some(
            RawNode::new(&cfg, storage)
                .map_err(|e| NodeError::Raft {
                    node_id: self.id,
                    cause: e,
                    backtrace: Backtrace::new(),
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
                node_id: self.id,
                cause: e,
                backtrace: Backtrace::new(),
            })
    }

    fn send_response(&mut self, answer: Answer) -> NodeResult<()> {
        let response = match answer.kind {
            AnswerKind::Success => Response::StateChange(GeneralResult::Success),
            AnswerKind::Fail => Response::StateChange(GeneralResult::Fail),
        };
        self.response_txs
            .remove(&answer.id)
            .ok_or(NodeError::AnswerDelivery {
                node_id: self.id,
                backtrace: Backtrace::new(),
            })
            .and_then(|tx| {
                tx.send(response)
                    .map_err(|_| NodeError::AnswerDelivery {
                        node_id: self.id,
                        backtrace: Backtrace::new(),
                    })
            })?;
        self.response_wakers
            .remove(&answer.id)
            .ok_or(NodeError::AnswerDelivery {
                node_id: self.id,
                backtrace: Backtrace::new(),
            })
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
                    log::trace!("received new state-change request, adding to proposals...");
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
                        .map_err(|_| NodeError::AnswerDelivery {
                            node_id: self.id,
                            backtrace: Backtrace::new(),
                        })?;
                    request.waker.wake();
                },
            }

            if timeout.elapsed() >= Duration::from_millis(10) {
                break;
            }
        }

        Ok(proposals)
    }

    pub fn advance(&mut self) -> NodeResult<()> {
        if let Some(raft_group) = self.raft_group.as_mut() {
            let leader = raft_group.raft.leader_id;
            let nodes = &raft_group.mut_store().writable().conf_state().nodes;
            log::debug!("raft-state on node {}: {{ leader: {}, nodes: {:?} }}", self.id, leader, nodes);
        }

        log::trace!("advancing node {}...", self.id);

        let timeout = Instant::now();

        let mut messages = Vec::new();
        let mut answers = Vec::new();

        let node_id = self.id;

        'connection_manager: loop {
            if let Some(mut transport) = self.connection_manager.accept() {
                log::debug!("accepted new connection on node {} from {:?}", node_id, transport.dest());
                let conf_state = self.raft_group.as_ref().unwrap()
                    .get_store()
                    .readable()
                    .snapshot_metadata()
                    .get_conf_state();
                if let Ok(()) = transport.send(TransportItem::Welcome(
                    node_id, conf_state.nodes.clone(), conf_state.learners.clone()
                )) {
                    self.new_transports.push(transport);
                } else {
                    log::error!(
                        "failed to send welcome to node {} from node {}",
                        transport.dest(),
                        node_id,
                    );
                }
            }

            if timeout.elapsed() >= Duration::from_millis(2) {
                break 'connection_manager;
            }
        }

        {
            let mut i = 0;
            'new_transports: while i < self.new_transports.len() {
                let transport = self.new_transports.get_mut(i).unwrap();
                match transport.try_recv() {
                    Ok(TransportItem::Hello(new_node_id)) => {
                        log::debug!("received hello on node {} from node {}", node_id, new_node_id);
                        let context = bincode::serialize(&transport.dest())
                            .map_err(|e| NodeError::NodeAdd {
                                node_id,
                                other_node: new_node_id,
                                cause: Box::new(e),
                                backtrace: Backtrace::new(),
                            })?;
                        let mut conf_change = ConfChange::new();
                        conf_change.set_node_id(new_node_id);
                        conf_change.set_change_type(ConfChangeType::AddNode);
                        conf_change.set_context(context);
                        let id = self.proposal_id;
                        self.proposal_id += 1;
                        self.proposals.push(Proposal::conf_change(id, node_id, conf_change));
                        let transport = self.new_transports.remove(i);
                        self.transports.insert(new_node_id, transport);
                    },
                    Ok(TransportItem::Welcome(new_node_id, nodes, learners)) => {
                        log::debug!("received welcome on node {} from node {}", node_id, new_node_id);
                        let transport = self.new_transports.remove(i);
                        self.transports.insert(new_node_id, transport);
                        let raft_group = self.raft_group.as_mut().unwrap();
                        for node in nodes {
                            raft_group.raft.add_node(node)
                                .map_err(|e| NodeError::NodeAdd {
                                    node_id,
                                    other_node: new_node_id,
                                    cause: Box::new(e),
                                    backtrace: Backtrace::new(),
                                })?;
                        }

                        for learner in learners {
                            raft_group.raft.add_learner(learner)
                                .map_err(|e| NodeError::NodeAdd {
                                    node_id,
                                    other_node: new_node_id,
                                    cause: Box::new(e),
                                    backtrace: Backtrace::new(),
                                })?;
                        }
                    },
                    Ok(other) => {
                        log::warn!("new transport sent invalid message: {:?}", other);
                        log::warn!("disconnecting from misbehaving transport...");
                        self.new_transports.remove(i);
                    },
                    Err(TransportError::Empty(_)) => {
                        i += 1;
                    },
                    Err(TransportError::Disconnected(_)) => {
                        log::debug!(
                            "new transport from {:?} disconnected before sending hello or welcome on node {}, forgetting...",
                            transport.dest(),
                            node_id,
                        );
                        self.new_transports.remove(i);
                    }
                }

                if timeout.elapsed() >= Duration::from_millis(5) {
                    break 'new_transports;
                }
            }
        }

        let mut to_disconnect = Vec::new();
        'transports: for (tp_id, transport) in self.transports.iter_mut() {
            loop {
                log::trace!("receiving new items from transport {}...", tp_id);
                // receive new messages from transport
                match transport.try_recv() {
                    Ok(TransportItem::Message(msg)) => {
                        log::trace!("received message {:?} on transport {}", msg, tp_id);
                        messages.push(msg);
                    },
                    Ok(TransportItem::Proposal(p)) => {
                        log::trace!("received proposal {:?} on transport {}", p, tp_id);
                        self.proposals.push(p);
                    },
                    Ok(TransportItem::Answer(a)) => {
                        log::trace!("received answer {:?} on transport {}", a, tp_id);
                        answers.push(a);
                    },
                    Ok(TransportItem::Hello(_)) => {
                        log::error!(
                            "hello received from node {} on node {} when already part of raft!",
                            tp_id,
                            node_id,
                        );
                        to_disconnect.push(*tp_id);
                    },
                    Ok(TransportItem::Welcome(..)) => {
                        log::error!(
                            "welcome received from node {} on node {} when already part of raft!",
                            tp_id,
                            node_id,
                        );
                        to_disconnect.push(*tp_id);
                    },
                    Err(TransportError::Empty(_)) => break,
                    Err(TransportError::Disconnected(_)) => {
                        log::trace!("host for node {} is down!", transport.dest());
                        break;
                    },
                }

                if timeout.elapsed() >= Duration::from_millis(5) {
                    break 'transports;
                }
            }
        }

        for id in to_disconnect.drain(..) {
            self.transports.remove(&id);
        }

        for answer in answers.drain(..) {
            let proposal_id = answer.id;
            self.send_response(answer)?;
            self.proposed.remove_item(&proposal_id);
        }

        for msg in messages.drain(..) {
            self.step(msg)?;
        }

        if self.raft_group.is_none() {
            log::trace!("raft is not initialized yet on node {}", self.id);
            return Ok(());
        }

        if self.timer.elapsed() >= Duration::from_millis(100) {
            // tick the raft.
            self.raft_group.as_mut().unwrap().tick();
            self.timer = Instant::now();
        }

        let mut new_proposals = self.get_proposals_from_requests(&timeout)?;
        self.proposals.append(&mut new_proposals);

        // handle saved proposals if we are leader
        if self.raft_group.as_ref().unwrap().raft.state == StateRole::Leader {
            log::trace!("node {} is leader and handling proposals", self.id);
            let mut my_answers = Vec::new();
            // FIXME: only reading first 110 proposals, as otherwise the raft
            //        is unable to always process them. This needs further
            //        investigation and is definitely a bug.
            // THIS IS A HACK
            let magic_number_nobody_will_ever_understand = 110;
            let mut end = self.proposals.len();
            if end > magic_number_nobody_will_ever_understand {
                end = magic_number_nobody_will_ever_understand;
            }
            let mut retry = Vec::new();
            // drain the proposals (consume all)
            for proposal in self.proposals.drain(..end) {
                let id = proposal.id();
                let origin = proposal.origin();
                let answer = match proposal.apply_on(self.raft_group.as_mut().unwrap()) {
                    Ok(None) => {
                        // if applying was successful:
                        // add to proposed vector when proposal originated by us
                        if node_id == origin {
                            self.proposed.push(id);
                        }
                        continue;
                    },
                    Ok(Some(answer)) => answer,
                    Err(ProposeError::AlreadyPending) => {
                        retry.push(proposal);
                        continue;
                    },
                };
                // the client who initiated the proposal is connected to us
                if node_id == origin {
                    // if applying was not successful, prepare to tell the client
                    my_answers.push(answer);
                } else {
                    if let Some(transport) = self.transports.get_mut(&origin) {
                        log::trace!("forwarding answer to node {}", origin);
                        transport.send(TransportItem::Answer(answer))
                            .map_err(|e| NodeError::AnswerForwarding {
                                origin_node: origin,
                                this_node: node_id,
                                cause: e,
                                backtrace: Backtrace::new(),
                            })?;
                    } else {
                        log::warn!(
                            "Could not forward answer to node {} as transport is unavailable",
                            origin,
                        );
                    }
                }
            }

            for answer in my_answers.drain(..) {
                self.send_response(answer)?;
            }

            self.proposals.append(&mut retry);
        } else {
            log::trace!("node {} is follower", self.id);
            // if we know some leader
            match self.transports.get_mut(&self.raft_group.as_ref().unwrap().raft.leader_id) {
                Some(leader) => {
                    log::trace!("forwarding proposals to leader node {}", leader.dest());
                    // forward proposals to leader
                    for proposal in self.proposals.drain(..) {
                        let id = proposal.id();
                        log::trace!("forwarding proposal {}...", id);
                        leader.send(TransportItem::Proposal(proposal))
                            .map_err(|e| NodeError::ProposalForwarding {
                                node_id: node_id,
                                cause: e,
                                backtrace: Backtrace::new(),
                            })?;
                        // proposal was forwarded and can therefore be put in our proposed
                        // list to prepare for client answer
                        self.proposed.push(id);
                    }
                },
                None => {
                    // display the warning only after first leader election
                    if self.raft_group.as_ref().unwrap().raft.leader_id > 0 {
                        log::warn!("Transport of leader not available, retrying on next tick...");
                    }
                },
            }
        }

        // handle readies from the raft.
        self.on_ready()?;

        if let Some(duration) = Duration::from_millis(10).checked_sub(timeout.elapsed()) {
            log::trace!("node {} is awaiting rest of cycle time", self.id);
            thread::sleep(duration);
        }

        Ok(())
    }

    fn on_ready(&mut self) -> NodeResult<()> {
        // if raft is not initialized, return
        if !self.raft_group.as_ref().unwrap().has_ready() {
            return Ok(());
        }

        let node_id = self.id;

        // get the `Ready` with `RawNode::ready` interface.
        let mut ready = self.raft_group.as_mut().unwrap().ready();

        {
            let store = &mut self.raft_group.as_mut().unwrap().raft.raft_log.store;

            log::trace!("persisting raft logs...");
            // persistent raft logs. It's necessary because in `RawNode::advance` we stabilize
            // raft logs to the latest position.
            store.writable()
                .append(ready.entries())
                .map_err(|e| NodeError::Storage {
                    node_id: node_id,
                    cause: Box::new(e),
                    backtrace: Backtrace::new(),
                })?;

            log::trace!("applying pending snapshot");
            // apply the snapshot. It's necessary because in `RawNode::advance`
            // we stabilize the snapshot.
            if *ready.snapshot() != Snapshot::new_() {
                let s = ready.snapshot().clone();
                store.writable()
                    .set_snapshot(s)
                    .map_err(|e| NodeError::Storage {
                        node_id: node_id,
                        cause: Box::new(e),
                        backtrace: Backtrace::new(),
                    })?;
            }
        }

        log::trace!("sending pending messages to other nodes");
        // send out the messages from this node
        for msg in ready.messages.drain(..) {
            let to = msg.get_to();
            if self.transports.get_mut(&to).unwrap().send(TransportItem::Message(msg)).is_err() {
                log::warn!("send raft message to {} fail, let raft retry it", to);
            }
        }

        // apply all committed proposals
        if let Some(committed_entries) = ready.committed_entries.take() {
            for entry in &committed_entries {
                if entry.get_data().is_empty() {
                    log::debug!("received commit from new elected leader on node {}", self.id);
                    // from new elected leaders.
                    continue;
                }
                match entry.get_entry_type() {
                    EntryType::EntryConfChange => {
                        log::debug!("received conf-change on node {}", self.id);
                        // apply configuration changes
                        let mut cc = ConfChange::new();
                        cc.merge_from_bytes(entry.get_data())
                            .map_err(|e| NodeError::ConfChange {
                                node_id,
                                cause: Box::new(e),
                                backtrace: Backtrace::new(),
                            })?;

                        let node_id = cc.get_node_id();
                        match cc.get_change_type() {
                            ConfChangeType::AddNode => {
                                if node_id != self.id {
                                    log::debug!("conf-change adds node {} to raft", node_id);
                                    let address: <C::Transport as Transport<M>>::Address =
                                        bincode::deserialize(cc.get_context())
                                        .map_err(|e| NodeError::ConfChange {
                                            node_id: self.id,
                                            cause: Box::new(e),
                                            backtrace: Backtrace::new(),
                                        })?;

                                    let in_transports = self.transports
                                        .values()
                                        .find(|t| t.dest() == address);
                                    let in_new_transports = self.new_transports
                                        .iter()
                                        .find(|t| t.dest() == address);
                                    if in_transports.is_none() && in_new_transports.is_none() {
                                        log::debug!(
                                            "trying to connect to {:?} on node {}...",
                                            address,
                                            self.id,
                                        );
                                        let mut transport = self.connection_manager.connect(&address)
                                            .map_err(|e| NodeError::ConfChange {
                                                node_id: self.id,
                                                cause: Box::new(e),
                                                backtrace: Backtrace::new(),
                                            })?;

                                        transport.send(TransportItem::Welcome(self.id, vec![], vec![]))
                                            .map_err(|e| NodeError::ConfChange {
                                                node_id: self.id,
                                                cause: Box::new(e),
                                                backtrace: Backtrace::new(),
                                            })?;
                                        self.new_transports.push(transport);
                                    };
                                }
                                self.raft_group.as_mut().unwrap().raft.add_node(node_id)
                            },
                            ConfChangeType::RemoveNode => {
                                log::debug!("conf-change removes node {} from raft", node_id);
                                self.transports.remove(&node_id);
                                self.raft_group.as_mut().unwrap().raft.remove_node(node_id)
                            },
                            ConfChangeType::AddLearnerNode => {
                                log::debug!("conf-change adds learner node {} to raft", node_id);
                                self.raft_group.as_mut().unwrap().raft.add_learner(node_id)
                            },
                            ConfChangeType::BeginMembershipChange
                                | ConfChangeType::FinalizeMembershipChange => {
                                    unimplemented!()
                                },
                        }.map_err(|e| NodeError::ConfChange {
                            node_id,
                            cause: Box::new(e),
                            backtrace: Backtrace::new(),
                        })?;

                        let cs = ConfState::from(
                            self.raft_group
                                .as_ref()
                                .unwrap()
                                .raft.prs()
                                .configuration()
                                .clone()
                        );
                        log::debug!(
                            "writing new conf-state from conf-change to storage on node {}...",
                            self.id,
                        );
                        self.raft_group.as_mut().unwrap().raft.raft_log.store
                            .writable()
                            .set_conf_state(cs)
                            .map_err(|e| NodeError::Storage {
                                node_id,
                                cause: Box::new(e),
                                backtrace: Backtrace::new(),
                            })?;
                    },
                    EntryType::EntryNormal => {
                        log::debug!("received state-change entry on node {}", self.id);
                        // for state change proposals, tell the machine to change its state.
                        let state_change = bincode::deserialize(entry.get_data())
                            .map_err(|e| NodeError::ConfChange {
                                node_id,
                                cause: Box::new(e),
                                backtrace: Backtrace::new(),
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
                if node_id == self.raft_group.as_ref().unwrap().raft.id
                    && self.proposed.contains(&proposal_id)
                {
                    log::debug!("delivering answer of proposal {} on node {}", proposal_id, self.id);
                    // ignore when sending the answer was not successful
                    let _ = self.send_response(Answer {
                        id: proposal_id,
                        kind: AnswerKind::Success,
                    });
                    self.proposed.remove_item(&proposal_id);
                }
            }

            if let Some(last_committed) = committed_entries.last() {
                log::debug!("writing new hard-state on node {} to storage...", self.id);
                let store = self.raft_group
                    .as_mut()
                    .unwrap()
                    .raft
                    .raft_log
                    .store
                    .writable();

                store.set_hard_state(HardState {
                    commit: last_committed.get_index(),
                    term: last_committed.get_term(),
                    ..store.hard_state().clone()
                }).map_err(|e| NodeError::Storage {
                    node_id,
                    cause: Box::new(e),
                    backtrace: Backtrace::new(),
                })?;
            }
        }

        log::trace!("finished processing the ready state, advancing raft...");
        // call `RawNode::advance` interface to update position flags in the raft.
        self.raft_group.as_mut().unwrap().advance(ready);

        Ok(())
    }
}
