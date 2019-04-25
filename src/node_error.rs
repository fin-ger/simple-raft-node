use failure::Fail;
use crate::{TransportError, EntryWriteError};

#[derive(Debug, Fail)]
pub enum NodeError {
    #[fail(display = "The name must end with a number that identifies the node: {}", name)]
    NoIdInName {
        name: String,
    },
    #[fail(display = "The given id in the name could not be parsed to u64: {}", name)]
    InvalidIdInName {
        name: String,
        #[cause]
        cause: <u64 as std::str::FromStr>::Err,
    },
    #[fail(display = "A raft operation failed")]
    Raft {
        #[cause]
        cause: raft::Error,
    },
    #[fail(display = "No transport registered for node {} on node {}", other_node, this_node)]
    NoTransportForNode {
        other_node: u64,
        this_node: u64,
    },
    #[fail(display = "Failed to deliver answer for proposal on node {}", node_name)]
    AnswerDelivery {
        node_name: String,
    },
    #[fail(display = "Failed to forward answer to origin node {} from node {}", origin_node, this_node)]
    AnswerForwarding {
        origin_node: u64,
        this_node: u64,
        #[cause]
        cause: TransportError,
    },
    #[fail(display = "Failed to forward proposal to leader on node {}", node_name)]
    ProposalForwarding {
        node_name: String,
        #[cause]
        cause: TransportError,
    },
    #[fail(display = "Failed to commit proposal on node {}", node_name)]
    ProposalCommit {
        node_name: String,
        #[cause]
        cause: CommitError,
    },
    #[fail(display = "Failed to append to storage")]
    StorageAppend {
        #[cause]
        cause: EntryWriteError,
    },
    // TODO: add Storage::InitError as cause
    #[fail(display = "Failed to initialize storage")]
    StorageInit,
}

#[derive(Debug, Fail)]
pub enum CommitError {
    #[fail(display = "Failed to deserialize config change payload")]
    ConfChangeDeserialization {
        #[cause]
        cause: protobuf::ProtobufError,
    },
    // NOTE: this error can be handled in NodeCore by retrying,
    //       so this should not be propagated...
    #[fail(display = "Failed to apply config change")]
    ConfChange {
        #[cause]
        cause: raft::Error,
    },
    #[fail(display = "Failed to deserialize state change payload")]
    StateChangeDeserialization {
        #[cause]
        cause: bincode::Error,
    },
    #[fail(display = "Failed to apply state change")]
    StateChange {
        // NOTE: will be completed when machine trait is refactored
    },
}

pub type NodeResult<T> = std::result::Result<T, NodeError>;
