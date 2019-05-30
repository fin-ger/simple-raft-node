use failure::{Fail, Backtrace};
use crate::TransportError;

#[derive(Debug, Fail)]
pub enum NodeError {
    #[fail(display = "A raft operation failed on node {}", node_id)]
    Raft {
        node_id: u64,
        #[cause]
        cause: raft::Error,
        backtrace: Backtrace,
    },
    #[fail(display = "Failed to deliver answer for proposal on node {}", node_id)]
    AnswerDelivery {
        node_id: u64,
        backtrace: Backtrace,
    },
    #[fail(display = "Failed to forward answer to origin node {} from node {}", origin_node, this_node)]
    AnswerForwarding {
        origin_node: u64,
        this_node: u64,
        #[cause]
        cause: TransportError,
        backtrace: Backtrace,
    },
    #[fail(display = "Failed to forward proposal to leader on node {}", node_id)]
    ProposalForwarding {
        node_id: u64,
        #[cause]
        cause: TransportError,
        backtrace: Backtrace,
    },
    #[fail(display = "Failed to add node {} to cluster on node {}", other_node, node_id)]
    NodeAdd {
        node_id: u64,
        other_node: u64,
        #[cause]
        cause: Box<Fail>,
        backtrace: Backtrace,
    },
    #[fail(display = "A storage failure occurred on node {}", node_id)]
    Storage {
        node_id: u64,
        #[cause]
        cause: Box<Fail>,
        backtrace: Backtrace,
    },
    // NOTE: this error can be handled in NodeCore by retrying,
    //       so this should not be propagated...
    #[fail(display = "Failed to apply config change on node {}", node_id)]
    ConfChange {
        node_id: u64,
        #[cause]
        cause: Box<Fail>,
        backtrace: Backtrace,
    },
    #[fail(display = "Failed to apply state change on node {}", node_id)]
    StateChange {
        node_id: u64,
        #[cause]
        cause: Box<Fail>,
        backtrace: Backtrace,
    },
    #[fail(display = "Failed to connect to gateway server at address {:?} on node {}", address, node_id)]
    GatewayConnect {
        node_id: u64,
        address: String,
        #[cause]
        cause: Box<Fail>,
        backtrace: Backtrace,
    },
    #[fail(display = "The node {} has no active connections to the cluster", node_id)]
    AllTransportsClosed {
        node_id: u64,
        backtrace: Backtrace,
    },
}

pub type NodeResult<T> = std::result::Result<T, NodeError>;
