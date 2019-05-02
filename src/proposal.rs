use std::convert::{TryInto, TryFrom};

use serde::{Deserialize, Serialize};
use raft::{eraftpb::ConfChange, RawNode};

use crate::serde_polyfill::ConfChangePolyfill;
use crate::MachineCore;

#[derive(Serialize, Deserialize)]
pub enum AnswerKind {
    Success,
    Fail,
}

#[derive(Serialize, Deserialize)]
pub struct Answer {
    pub id: u64,
    pub kind: AnswerKind,
}

#[derive(Serialize, Deserialize)]
pub struct Context {
    pub proposal_id: u64,
    pub node_id: u64,
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

#[derive(Serialize, Deserialize)]
pub enum ProposalKind<M: MachineCore> {
    #[serde(bound(deserialize = "M::StateChange: Deserialize<'de>"))]
    #[serde(bound(serialize = "M::StateChange: Serialize"))]
    StateChange(M::StateChange),
    ConfChange(#[serde(with = "ConfChangePolyfill")] ConfChange),
    TransferLeader(u64),
}

#[derive(Serialize, Deserialize)]
pub struct Proposal<M: MachineCore> {
    context: Context,
    #[serde(bound(deserialize = "ProposalKind<M>: Deserialize<'de>"))]
    #[serde(bound(serialize = "ProposalKind<M>: Serialize"))]
    kind: ProposalKind<M>,
}

impl<M: MachineCore> Proposal<M> {
    pub fn state_change(id: u64, node_id: u64, change: M::StateChange) -> Self {
        Self {
            context: Context {
                proposal_id: id,
                node_id,
            },
            kind: ProposalKind::StateChange(change),
        }
    }

    pub fn conf_change(id: u64, node_id: u64, conf_change: ConfChange) -> Self {
        Self {
            context: Context {
                proposal_id: id,
                node_id,
            },
            kind: ProposalKind::ConfChange(conf_change),
        }
    }

    pub fn transfer_leader(id: u64, node_id: u64, transferee: u64) -> Self {
        Self {
            context: Context {
                proposal_id: id,
                node_id,
            },
            kind: ProposalKind::TransferLeader(transferee),
        }
    }

    pub fn origin(&self) -> u64 {
        self.context.node_id
    }

    pub fn id(&self) -> u64 {
        self.context.proposal_id
    }

    pub fn apply_on<T: raft::Storage>(
        self,
        raft_group: &mut RawNode<T>,
    ) -> Option<Answer> {
        let id = self.id();
        let context = match self.context.try_into() {
            Ok(context) => context,
            Err(err) => {
                log::error!("Failed to deserialize context of proposal: {}", err);
                return Some(Answer {
                    id,
                    kind: AnswerKind::Fail,
                });
            },
        };

        let last_index1 = raft_group.raft.raft_log.last_index();
        match self.kind {
            ProposalKind::StateChange(ref change) => {
                let data = match bincode::serialize(change) {
                    Ok(data) => data,
                    Err(err) => {
                        log::error!("Failed to serialize state-change of proposal: {}", err);
                        return Some(Answer {
                            id,
                            kind: AnswerKind::Fail,
                        });
                    },
                };

                match raft_group.propose(context, data) {
                    Ok(()) => {},
                    Err(err) => {
                        log::error!("Failed to process state-change in raft: {}", err);
                        return Some(Answer {
                            id,
                            kind: AnswerKind::Fail,
                        });
                    },
                };
            },
            ProposalKind::ConfChange(ref conf_change) => {
                match raft_group.propose_conf_change(context, conf_change.clone()) {
                    Ok(()) => {},
                    Err(err) => {
                        log::error!("Failed to process conf-change in raft: {}", err);
                        return Some(Answer {
                            id,
                            kind: AnswerKind::Fail,
                        });
                    },
                };
            },
            ProposalKind::TransferLeader(ref _transferee) => {
                // TODO: implement transfer leader.
                unimplemented!();
            },
        };

        if raft_group.raft.raft_log.last_index() == last_index1 {
            log::error!("No progress was made while processing proposal!");
            return Some(Answer {
                id,
                kind: AnswerKind::Fail,
            });
        }

        None
    }
}
