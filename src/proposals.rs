use std::convert::{TryInto, TryFrom};
use serde::{Deserialize, Serialize};
use raft::{eraftpb::ConfChange, RawNode};

use crate::serde_polyfill::ConfChangePolyfill;

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

/* TODO:
 *  - make state change generic
 *  - replace bincode with protobuf
 */

#[derive(Serialize, Deserialize)]
enum ProposalKind {
    StateChange(u16, String),
    ConfChange(#[serde(with = "ConfChangePolyfill")] ConfChange),
    TransferLeader(u64),
}

#[derive(Serialize, Deserialize)]
pub struct Proposal {
    context: Context,
    kind: ProposalKind,
}

impl Proposal {
    pub fn state_change(id: u64, key: u16, value: String) -> Self {
        Self {
            context: Context {
                proposal_id: id,
                node_id: 0,
            },
            kind: ProposalKind::StateChange(key, value),
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

    pub fn apply_on<T: raft::Storage>(self, raft_group: &mut RawNode<T>) -> bool {
        let last_index1 = raft_group.raft.raft_log.last_index() + 1;
        let context = self.context.try_into().unwrap();
        match self.kind {
            ProposalKind::StateChange(ref key, ref value) => {
                let data = format!("put {} {}", key, value).into_bytes();
                raft_group.propose(context, data).unwrap();
            },
            ProposalKind::ConfChange(ref conf_change) => {
                raft_group.propose_conf_change(context, conf_change.clone()).unwrap();
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

#[derive(Debug, Serialize, Deserialize)]
pub struct Answer {
    pub id: u64,
    pub value: bool,
}
