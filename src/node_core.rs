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
    NodeRemovalContext,
    Request,
    RequestKind,
    Response,
    GeneralResult,
    StateRetrievalResult,
    ConnectionManager,
};

pub struct NodeCore<M: MachineCore, C: ConnectionManager<M>, S: Storage> {
    id: u64,
    raft_node: RawNode<WrappedStorage<S>>,
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
        gateway: <C::Transport as Transport<M>>::Address,
        machine: M,
        mut storage: S,
        mut connection_manager: C,
        request_rx: Receiver<Request<M>>,
    ) -> NodeResult<Self> {
        log::debug!("creating core for node {} with gateway {:?}...", base_config.id, gateway);

        let mut new_transports = Vec::new();
        let conf_state = if !connection_manager.is_this_node(&gateway) {
            let mut new_transport = connection_manager.connect(&gateway)
                .map_err(|e| NodeError::GatewayConnect {
                    node_id: base_config.id,
                    address: format!("{}", gateway),
                    cause: Box::new(e),
                    backtrace: Backtrace::new(),
                })?;
            new_transport.send(TransportItem::Hello(
                base_config.id,
                connection_manager.listener_addr(),
            )).map_err(|e| NodeError::GatewayConnect {
                node_id: base_config.id,
                address: format!("{}", gateway),
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

        let raft_node = RawNode::new(&base_config, wrapped_storage)
            .map_err(|e| NodeError::Raft {
                node_id: base_config.id,
                cause: e,
                backtrace: Backtrace::new(),
            })?;

        Ok(Self {
            id: base_config.id,
            raft_node,
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
            let node_id = self.raft_node.raft.id;

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
                    let response = match self.machine.retrieve(identifier) {
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

    fn handle_disconnect(&self) -> NodeResult<()> {
        if self.transports.len() == 0 && self.new_transports.len() == 0 {
            return Err(NodeError::AllTransportsClosed {
                node_id: self.id(),
                backtrace: Backtrace::new(),
            });
        }

        Ok(())
    }

    pub fn advance(&mut self) -> NodeResult<()> {
        let leader = self.raft_node.raft.leader_id;
        let nodes = &self.raft_node.mut_store().writable().conf_state().nodes;
        log::trace!("raft-state on node {}: {{ leader: {}, nodes: {:?} }}", self.id, leader, nodes);

        log::trace!("advancing node {}...", self.id);

        let timeout = Instant::now();

        let mut messages = Vec::new();
        let mut answers = Vec::new();

        let node_id = self.id;

        'connection_manager: loop {
            if let Some(transport) = self.connection_manager.accept() {
                log::debug!(
                    "accepted new connection on node {} from {:?}",
                    node_id,
                    transport.dest().ok(),
                );
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
                    Ok(TransportItem::Hello(new_node_id, peer_addr)) => {
                        log::debug!("received hello on node {} from node {}", node_id, new_node_id);

                        if self.raft_node.raft.prs().get(new_node_id).is_some() {
                            log::info!(
                                concat!(
                                    "removing existing node {} from the raft as a new node ",
                                    "with the same id is about to join",
                                ),
                                new_node_id,
                            );
                            // remove existing node from cluster as the newly connected
                            // node can be (although it has the same id) another physical
                            // cluster node. Before we can add the new physical cluster node
                            // to the raft, we have to safely remove the old node from the
                            // cluster. In order to add the new node to the cluster after
                            // successful removal, we have to carry a context containing all
                            // needed information for adding the new node to the cluster.
                            let context = bincode::serialize(&NodeRemovalContext::AddNewNode {
                                node_id: new_node_id,
                                address: peer_addr,
                            }).map_err(|e| NodeError::ConfChange {
                                node_id,
                                cause: Box::new(e),
                                backtrace: Backtrace::new(),
                            })?;
                            let mut conf_change = ConfChange::new();
                            conf_change.set_node_id(new_node_id);
                            conf_change.set_change_type(ConfChangeType::RemoveNode);
                            conf_change.set_context(context);
                            let id = self.proposal_id;
                            self.proposal_id += 1;
                            self.proposals.push(Proposal::conf_change(id, node_id, conf_change));
                        } else {
                            let context = bincode::serialize(&peer_addr)
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
                        }

                        let mut transport = self.new_transports.remove(i);

                        let conf_state = self.raft_node
                            .get_store()
                            .readable()
                            .snapshot_metadata()
                            .get_conf_state();
                        if transport.send(TransportItem::Welcome(
                            node_id, conf_state.nodes.clone(), conf_state.learners.clone()
                        )).is_err() {
                            log::error!(
                                "failed to send welcome to node {:?} from node {}",
                                transport.dest().ok(),
                                node_id,
                            );
                        }

                        // close this transport as the connection will be reopened from the raft
                        // to the new node when ConfChange::AddNode was successful with a new
                        // welcome message from each node (containing no nodes or learners).
                        transport.close();
                    },
                    Ok(TransportItem::Welcome(new_node_id, nodes, learners)) => {
                        log::debug!("received welcome on node {} from node {}", node_id, new_node_id);
                        let transport = self.new_transports.remove(i);
                        self.transports.insert(new_node_id, transport);
                        for node in nodes {
                            self.raft_node.raft.add_node(node)
                                .map_err(|e| NodeError::NodeAdd {
                                    node_id,
                                    other_node: new_node_id,
                                    cause: Box::new(e),
                                    backtrace: Backtrace::new(),
                                })?;
                        }

                        for learner in learners {
                            self.raft_node.raft.add_learner(learner)
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
                        self.new_transports.remove(i).close();
                    },
                    Err(TransportError::Empty(_)) => {
                        i += 1;
                    },
                    Err(TransportError::Disconnected(_)) => {
                        log::debug!(
                            "new transport from {:?} disconnected before sending hello or welcome on node {}, forgetting...",
                            transport.dest().ok(),
                            node_id,
                        );
                        self.new_transports.remove(i).close();
                        self.handle_disconnect()?;
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
                    Ok(TransportItem::Hello(..)) => {
                        log::error!(
                            "hello received from node {} on node {} when already part of raft!",
                            tp_id,
                            node_id,
                        );
                        to_disconnect.push(*tp_id);

                        let context = bincode::serialize(
                            &NodeRemovalContext::<<C::Transport as Transport<M>>::Address>::None
                        ).map_err(|e| NodeError::ConfChange {
                            node_id,
                            cause: Box::new(e),
                            backtrace: Backtrace::new(),
                        })?;
                        let mut conf_change = ConfChange::new();
                        conf_change.set_node_id(*tp_id);
                        conf_change.set_change_type(ConfChangeType::RemoveNode);
                        conf_change.set_context(context);
                        let id = self.proposal_id;
                        self.proposal_id += 1;
                        self.proposals.push(Proposal::conf_change(id, node_id, conf_change));
                    },
                    Ok(TransportItem::Welcome(..)) => {
                        log::error!(
                            "welcome received from node {} on node {} when already part of raft!",
                            tp_id,
                            node_id,
                        );
                        to_disconnect.push(*tp_id);

                        let context = bincode::serialize(
                            &NodeRemovalContext::<<C::Transport as Transport<M>>::Address>::None
                        ).map_err(|e| NodeError::ConfChange {
                            node_id,
                            cause: Box::new(e),
                            backtrace: Backtrace::new(),
                        })?;
                        let mut conf_change = ConfChange::new();
                        conf_change.set_node_id(*tp_id);
                        conf_change.set_change_type(ConfChangeType::RemoveNode);
                        conf_change.set_context(context);
                        let id = self.proposal_id;
                        self.proposal_id += 1;
                        self.proposals.push(Proposal::conf_change(id, node_id, conf_change));
                    },
                    Err(TransportError::Empty(_)) => break,
                    Err(TransportError::Disconnected(_)) => {
                        log::trace!("host for node {:?} is down!", transport.dest().ok());
                        to_disconnect.push(*tp_id);
                        break;
                    },
                }

                if timeout.elapsed() >= Duration::from_millis(5) {
                    break 'transports;
                }
            }
        }

        for id in to_disconnect.drain(..) {
            self.transports.remove(&id).map(|t| t.close());
            self.handle_disconnect()?;
        }

        for answer in answers.drain(..) {
            let proposal_id = answer.id;
            self.send_response(answer)?;
            self.proposed.remove_item(&proposal_id);
        }

        for msg in messages.drain(..) {
            self.raft_node.step(msg)
                .map_err(|e| NodeError::Raft {
                    node_id,
                    cause: e,
                    backtrace: Backtrace::new(),
                })?;
        }

        if self.timer.elapsed() >= Duration::from_millis(100) {
            // tick the raft.
            self.raft_node.tick();
            self.timer = Instant::now();
        }

        let mut new_proposals = self.get_proposals_from_requests(&timeout)?;
        self.proposals.append(&mut new_proposals);

        // handle saved proposals if we are leader
        if self.raft_node.raft.state == StateRole::Leader {
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
                let answer = match proposal.apply_on(&mut self.raft_node) {
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
            match self.transports.get_mut(&self.raft_node.raft.leader_id) {
                Some(leader) => {
                    log::trace!("forwarding proposals to leader node {:?}", leader.dest().ok());
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
                    if self.raft_node.raft.leader_id > 0 {
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
        if !self.raft_node.has_ready() {
            return Ok(());
        }

        let node_id = self.id;

        // get the `Ready` with `RawNode::ready` interface.
        let mut ready = self.raft_node.ready();

        {
            let store = &mut self.raft_node.raft.raft_log.store;

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

                                    // always reconnect to the new node and wait for the old
                                    // transport in new_transports to be disconnected from the
                                    // remove end
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
                                    // we have to send another welcome that will essentially do
                                    // nothing but move the transport from new_transports to the
                                    // transports map on the remote end.
                                    if transport.send(TransportItem::Welcome(
                                        self.id, vec![], vec![]
                                    )).is_err() {
                                        log::error!(
                                            "failed to send welcome to node {:?} from node {}",
                                            transport.dest().ok(),
                                            node_id,
                                        );
                                    }
                                    self.transports.insert(node_id, transport);
                                }
                                self.raft_node.raft.add_node(node_id)
                            },
                            ConfChangeType::RemoveNode => {
                                log::debug!("conf-change removes node {} from raft", node_id);
                                let ctx: NodeRemovalContext<<C::Transport as Transport<M>>::Address> =
                                    bincode::deserialize(cc.get_context())
                                    .map_err(|e| NodeError::ConfChange {
                                        node_id: self.id,
                                        cause: Box::new(e),
                                        backtrace: Backtrace::new(),
                                    })?;

                                self.transports.remove(&node_id).map(|t| t.close());
                                let res = self.raft_node.raft.remove_node(node_id);

                                // TODO: only do this as leader
                                let node_id = self.id;
                                if let NodeRemovalContext::AddNewNode {
                                    node_id: new_node_id,
                                    address
                                } = ctx {
                                    log::info!("adding new node {} to the raft", new_node_id);
                                    let context = bincode::serialize(&address)
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
                                }

                                res
                            },
                            ConfChangeType::AddLearnerNode => {
                                log::debug!("conf-change adds learner node {} to raft", node_id);
                                self.raft_node.raft.add_learner(node_id)
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
                            self.raft_node
                                .raft.prs()
                                .configuration()
                                .clone()
                        );
                        log::debug!(
                            "writing new conf-state from conf-change to storage on node {}...",
                            self.id,
                        );
                        self.raft_node.raft.raft_log.store
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
                            .map_err(|e| NodeError::StateChange {
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
                if node_id == self.raft_node.raft.id
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
                let store = self.raft_node
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
        self.raft_node.advance(ready);

        Ok(())
    }
}
