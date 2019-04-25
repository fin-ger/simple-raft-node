use std::convert::{TryInto, TryFrom};

use serde::{Deserialize, Serialize};
use raft::{eraftpb::ConfChange, RawNode};
use failure::Fail;

use crate::serde_polyfill::ConfChangePolyfill;
use crate::MachineCore;

#[derive(Debug, Fail)]
pub enum ProposalError {
    #[fail(display = "Some data of this proposal could not be serialized to binary")]
    Serialization {
        #[cause]
        cause: bincode::Error,
    },
    #[fail(display = "Processing of proposal failed")]
    Processing {
        #[cause]
        cause: raft::Error,
    },
    #[fail(display = "No progress made after proposal was applied")]
    NoProgress,
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
enum ProposalKind<M: MachineCore> {
    StateChange(M::StateChange),
    ConfChange(#[serde(with = "ConfChangePolyfill")] ConfChange),
    TransferLeader(u64),
    StateRetrieval(M::StateIdentifier),
}

#[derive(Serialize, Deserialize)]
pub struct Proposal<M: MachineCore> {
    context: Context,
    kind: ProposalKind<M>,
}

impl<M: MachineCore> Proposal<M> {
    pub fn state_change(id: u64, change: M::StateChange) -> Self {
        Self {
            context: Context {
                proposal_id: id,
                node_id: 0,
            },
            kind: ProposalKind::StateChange(change),
        }
    }

    pub fn state_retrieval(id: u64, identifier: M::StateIdentifier) -> Self {
        Self {
            context: Context {
                proposal_id: id,
                node_id: 0,
            },
            kind: ProposalKind::StateRetrieval(identifier),
        }
    }

    pub fn conf_change(id: u64, conf_change: ConfChange) -> Self {
        Self {
            context: Context {
                proposal_id: id,
                node_id: 0,
            },
            kind: ProposalKind::ConfChange(conf_change),
        }
    }

    pub fn transfer_leader(id: u64, transferee: u64) -> Self {
        Self {
            context: Context {
                proposal_id: id,
                node_id: 0,
            },
            kind: ProposalKind::TransferLeader(transferee),
        }
    }

    pub fn origin(&self) -> u64 {
        self.context.node_id
    }

    pub fn set_origin(&mut self, node_id: u64) {
        self.context.node_id = node_id;
    }

    pub fn id(&self) -> u64 {
        self.context.proposal_id
    }

    pub fn apply_on<T: raft::Storage>(
        self,
        raft_group: &mut RawNode<T>,
        machine: &M,
    ) -> Option<Answer<M>> {
        let id = self.id();
        let kind = self.kind;
        let result = self.context
            .try_into()
            .map_err(|e| ProposalError::Serialization { cause: e })
            .and_then(|context| {
                let last_index1 = raft_group.raft.raft_log.last_index();
                match kind {
                    ProposalKind::StateChange(ref change) => {
                        let data = bincode::serialize(change)
                            .map_err(|e| ProposalError::Serialization { cause: e })?;
                        raft_group.propose(context, data)
                            .map_err(|e| ProposalError::Processing { cause: e })?;
                    },
                    ProposalKind::ConfChange(ref conf_change) => {
                        raft_group.propose_conf_change(context, conf_change.clone())
                            .map_err(|e| ProposalError::Processing { cause: e })?;
                    },
                    ProposalKind::TransferLeader(ref _transferee) => {
                        // TODO: implement transfer leader.
                        unimplemented!();
                    },
                    ProposalKind::StateRetrieval(ref identifier) => {
                        let retrieval = match machine.retrieve(identifier) {
                            // make the value a snapshot (a copy)
                            Ok(value) => RetrievalResult::Found(value.clone()),
                            Err(_) => RetrievalResult::NotFound,
                        };
                        return Ok(Some(Answer {
                            id,
                            result: AnswerResult::Retrieval(retrieval),
                        }));
                    },
                };

                if raft_group.raft.raft_log.last_index() == last_index1 {
                    // no progress made during proposal
                    return Err(ProposalError::NoProgress);
                }

                Ok(None)
            });

        match result {
            Ok(option) => option,
            Err(err) => {
                // NOTE: we do not provide further error information in the answer
                //       as the programmer is most likely unable to handle the
                //       error on the answer receiver side. Instead we log it as
                //       an error to enable the developer to hand in this
                //       misbehavior e.g. as a bug report.
                log::error!("Failed to apply proposal: {}", err);
                Some(Answer {
                    id,
                    result: AnswerResult::Change(ChangeResult::Failed),
                })
            },
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum ChangeResult {
    Successful,
    Failed,
}

#[derive(Serialize, Deserialize)]
pub enum RetrievalResult<M: MachineCore> {
    Found(M::StateValue),
    NotFound,
}

#[derive(Serialize, Deserialize)]
pub enum AnswerResult<M: MachineCore> {
    Change(ChangeResult),
    Retrieval(RetrievalResult<M>),
}

#[derive(Serialize, Deserialize)]
pub struct Answer<M: MachineCore> {
    pub id: u64,
    pub result: AnswerResult<M>,
}
