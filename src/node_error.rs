use failure::Fail;
use crate::{TransportError, EntryWriteError, SnapshotWriteError, WriteError};

#[derive(Debug, Fail)]
pub enum NodeError {
    #[fail(display = "A raft operation failed on node {}", node_id)]
    Raft {
        node_id: u64,
        #[cause]
        cause: raft::Error,
    },
    #[fail(display = "No transport registered for node {} on node {}", other_node, this_node)]
    NoTransportForNode {
        other_node: u64,
        this_node: u64,
    },
    #[fail(display = "Failed to deliver answer for proposal on node {}", node_id)]
    AnswerDelivery {
        node_id: u64,
    },
    #[fail(display = "Failed to forward answer to origin node {} from node {}", origin_node, this_node)]
    AnswerForwarding {
        origin_node: u64,
        this_node: u64,
        #[cause]
        cause: TransportError,
    },
    #[fail(display = "Failed to forward proposal to leader on node {}", node_id)]
    ProposalForwarding {
        node_id: u64,
        #[cause]
        cause: TransportError,
    },
    #[fail(display = "Failed to append to storage on node {}", node_id)]
    StorageAppend {
        node_id: u64,
        #[cause]
        cause: EntryWriteError,
    },
    #[fail(display = "Failed to apply snapshot to storage on node {}", node_id)]
    StorageSnapshot {
        node_id: u64,
        #[cause]
        cause: SnapshotWriteError,
    },
    #[fail(display = "Failed to write state to storage on node {}", node_id)]
    StorageState {
        node_id: u64,
        #[cause]
        cause: WriteError,
    },
    // TODO: add Storage::InitError as cause
    #[fail(display = "Failed to initialize storage")]
    StorageInit,
    #[fail(display = "A serialization or deserialization with bincode failed on node {}", node_id)]
    Bincode {
        node_id: u64,
        #[cause]
        cause: bincode::Error,
    },
    #[fail(display = "A serialization or deserialization with protobuf failed on node {}", node_id)]
    Protobuf {
        node_id: u64,
        #[cause]
        cause: protobuf::ProtobufError,
    },
    // NOTE: this error can be handled in NodeCore by retrying,
    //       so this should not be propagated...
    #[fail(display = "Failed to apply config change on node {}", node_id)]
    ConfChange {
        node_id: u64,
        #[cause]
        cause: raft::Error,
    },
}

pub type NodeResult<T> = std::result::Result<T, NodeError>;
