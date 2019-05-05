use failure::Fail;
use crate::TransportError;

#[derive(Debug, Fail)]
pub enum NodeError {
    #[fail(display = "A raft operation failed on node {}", node_id)]
    Raft {
        node_id: u64,
        #[cause]
        cause: raft::Error,
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
    #[fail(display = "Failed to add node {} to cluster on node {}", other_node, node_id)]
    NodeAdd {
        node_id: u64,
        other_node: u64,
        #[cause]
        cause: Box<Fail>,
    },
    #[fail(display = "A storage failure occurred on node {}", node_id)]
    Storage {
        node_id: u64,
        #[cause]
        cause: Box<Fail>,
    },
    // NOTE: this error can be handled in NodeCore by retrying,
    //       so this should not be propagated...
    #[fail(display = "Failed to apply config change on node {}", node_id)]
    ConfChange {
        node_id: u64,
        #[cause]
        cause: Box<Fail>,
    },
}

pub type NodeResult<T> = std::result::Result<T, NodeError>;
