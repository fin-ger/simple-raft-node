use std::collections::{HashMap};
use std::time::{Duration, Instant};
use std::convert::TryInto;
use std::task::Waker;
use std::thread;

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
        id: u64,
        base_config: Config,
        machine: M,
        mut storage: S,
        connection_manager: C,
        request_rx: Receiver<Request<M>>,
    ) -> NodeResult<Self> {
        let cfg = Config {
            id,
            tag: format!("node_{}", id),
            ..base_config.clone()
        };

        storage.init_with_conf_state(id, (vec![id], vec![]))
            .map_err(|e| NodeError::Storage {
                node_id: id,
                cause: Box::new(e),
            })?;
        let wrapped_storage = WrappedStorage::new(storage);

        let raft_group = Some(
            RawNode::new(&cfg, wrapped_storage)
                .map_err(|e| NodeError::Raft { node_id: id, cause: e })?
        );

        Ok(Self {
            id,
            base_config,
            raft_group,
            connection_manager,
            new_transports: Vec::new(),
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
            })
    }

    fn send_response(&mut self, answer: Answer) -> NodeResult<()> {
        let response = match answer.kind {
            AnswerKind::Success => Response::StateChange(GeneralResult::Success),
            AnswerKind::Fail => Response::StateChange(GeneralResult::Fail),
        };
        self.response_txs
            .remove(&answer.id)
            .ok_or(NodeError::AnswerDelivery { node_id: self.id })
            .and_then(|tx| {
                tx.send(response)
                    .map_err(|_| NodeError::AnswerDelivery { node_id: self.id })
            })?;
        self.response_wakers
            .remove(&answer.id)
            .ok_or(NodeError::AnswerDelivery { node_id: self.id })
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
                        .map_err(|_| NodeError::AnswerDelivery { node_id: self.id })?;
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
        let timeout = Instant::now();

        let mut messages = Vec::new();
        let mut answers = Vec::new();

        let node_id = self.id;

        'connection_manager: loop {
            if let Some(transport) = self.connection_manager.accept() {
                self.new_transports.push(transport);
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
                        log::debug!("Received hello on node {} from node {}", node_id, new_node_id);
                        let context = bincode::serialize(&transport.dest_addr())
                            .map_err(|e| NodeError::NodeAdd {
                                node_id,
                                other_node: new_node_id,
                                cause: Box::new(e),
                            })?;
                        let mut conf_change = ConfChange::new();
                        conf_change.set_node_id(new_node_id);
                        conf_change.set_change_type(ConfChangeType::AddNode);
                        conf_change.set_context(context);
                        let id = self.proposal_id;
                        self.proposal_id += 1;
                        self.proposals.push(Proposal::conf_change(id, node_id, conf_change));
                    },
                    Ok(other) => {
                        log::warn!("new transport sent invalid message: {:?}", other);
                        log::warn!("disconnecting from misbehaving transport...");
                        self.new_transports.remove(i);
                        continue;
                    },
                    Err(TransportError::Empty) => {},
                    Err(TransportError::Disconnected) => {
                        self.new_transports.remove(i);
                        continue;
                    }
                }

                i += 1;

                if timeout.elapsed() >= Duration::from_millis(5) {
                    break 'new_transports;
                }
            }
        }

        let mut to_disconnect = Vec::new();
        'transports: for (tp_id, transport) in self.transports.iter_mut() {
            loop {
                // step raft messages and save forwarded proposals
                match transport.try_recv() {
                    Ok(TransportItem::Message(msg)) => messages.push(msg),
                    Ok(TransportItem::Proposal(p)) => self.proposals.push(p),
                    Ok(TransportItem::Answer(a)) => answers.push(a),
                    Ok(TransportItem::Hello(_)) => {
                        log::error!(
                            "hello received from node {} when already part of raft!",
                            tp_id,
                        );
                        to_disconnect.push(*tp_id);
                    }
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
            // drain the proposals (consume all)
            for proposal in self.proposals.drain(..end) {
                let id = proposal.id();
                let origin = proposal.origin();
                let answer = match proposal.apply_on(self.raft_group.as_mut().unwrap()) {
                    None => {
                        // if applying was successful:
                        // add to proposed vector when proposal originated by us
                        if node_id == origin {
                            self.proposed.push(id);
                        }
                        continue;
                    },
                    Some(answer) => answer,
                };
                // the client who initiated the proposal is connected to us
                if node_id == origin {
                    // if applying was not successful, prepare to tell the client
                    my_answers.push(answer);
                } else {
                    if let Some(transport) = self.transports.get_mut(&origin) {
                        transport.send(TransportItem::Answer(answer))
                            .map_err(|e| NodeError::AnswerForwarding {
                                origin_node: origin,
                                this_node: node_id,
                                cause: e,
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
        } else {
            // if we know some leader
            match self.transports.get_mut(&self.raft_group.as_ref().unwrap().raft.leader_id) {
                Some(leader) => {
                    // forward proposals to leader
                    for proposal in self.proposals.drain(..) {
                        let id = proposal.id();
                        leader.send(TransportItem::Proposal(proposal))
                            .map_err(|e| NodeError::ProposalForwarding {
                                node_id: node_id,
                                cause: e,
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

            // persistent raft logs. It's necessary because in `RawNode::advance` we stabilize
            // raft logs to the latest position.
            store.writable()
                .append(ready.entries())
                .map_err(|e| NodeError::Storage {
                    node_id: node_id,
                    cause: Box::new(e),
                })?;

            // apply the snapshot. It's necessary because in `RawNode::advance` we stabilize the snapshot.
            if *ready.snapshot() != Snapshot::new_() {
                let s = ready.snapshot().clone();
                store.writable()
                    .set_snapshot(s)
                    .map_err(|e| NodeError::Storage {
                        node_id: node_id,
                        cause: Box::new(e),
                    })?;
            }
        }

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
                    // from new elected leaders.
                    continue;
                }
                match entry.get_entry_type() {
                    EntryType::EntryConfChange => {
                        // apply configuration changes
                        let mut cc = ConfChange::new();
                        cc.merge_from_bytes(entry.get_data())
                            .map_err(|e| NodeError::ConfChange {
                                node_id,
                                cause: Box::new(e),
                            })?;

                        let node_id = cc.get_node_id();
                        match cc.get_change_type() {
                            ConfChangeType::AddNode => {
                                let address: <C::Transport as Transport<M>>::Address =
                                    bincode::deserialize(cc.get_context())
                                    .map_err(|e| NodeError::ConfChange {
                                        node_id,
                                        cause: Box::new(e),
                                    })?;

                                let pos = self.new_transports
                                    .iter()
                                    .position(|t| t.dest_addr() == address);
                                let transport = if let Some(pos) = pos {
                                    self.new_transports.remove(pos)
                                } else {
                                    self.connection_manager.connect(address)
                                        .map_err(|e| NodeError::ConfChange {
                                            node_id,
                                            cause: Box::new(e),
                                        })?
                                };

                                self.transports.insert(node_id, transport);

                                self.raft_group.as_mut().unwrap().raft.add_node(node_id)
                            },
                            ConfChangeType::RemoveNode => {
                                self.transports.remove(&node_id);
                                self.raft_group.as_mut().unwrap().raft.remove_node(node_id)
                            },
                            ConfChangeType::AddLearnerNode => {
                                self.raft_group.as_mut().unwrap().raft.add_learner(node_id)
                            },
                            ConfChangeType::BeginMembershipChange
                                | ConfChangeType::FinalizeMembershipChange => {
                                    unimplemented!()
                                },
                        }.map_err(|e| NodeError::ConfChange {
                            node_id,
                            cause: Box::new(e),
                        })?;

                        let cs = ConfState::from(
                            self.raft_group
                                .as_ref()
                                .unwrap()
                                .raft.prs()
                                .configuration()
                                .clone()
                        );
                        self.raft_group.as_mut().unwrap().raft.raft_log.store
                            .writable()
                            .set_conf_state(cs)
                            .map_err(|e| NodeError::Storage {
                                node_id,
                                cause: Box::new(e),
                            })?;
                    },
                    EntryType::EntryNormal => {
                        // for state change proposals, tell the machine to change its state.
                        let state_change = bincode::deserialize(entry.get_data())
                            .map_err(|e| NodeError::ConfChange {
                                node_id,
                                cause: Box::new(e),
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
                    self.send_response(Answer {
                        id: proposal_id,
                        kind: AnswerKind::Success,
                    })?;
                    self.proposed.remove_item(&proposal_id);
                }
            }

            if let Some(last_committed) = committed_entries.last() {
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
                })?;
            }
        }

        // call `RawNode::advance` interface to update position flags in the raft.
        self.raft_group.as_mut().unwrap().advance(ready);

        Ok(())
    }
}
