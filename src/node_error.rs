use failure::{Fail, Backtrace};
use crate::TransportError;

/* Hello again, dear reader!
 *
 * This file contains the definition for all errors that can occur during the
 * operation of a `Node`. Technically, it would be ideal to have no error
 * wrapping in here, as the error itself should be descriptive to the failure.
 * However, it is unnecessary noise during prototyping of an application to
 * adjust the error types with every minor change in the `advance` algorithm
 * of the `Node`. This is why errors like `Raft` are not descriptive. For a
 * stable API, this must be fixed.
 */

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
        cause: Box<dyn Fail>,
        backtrace: Backtrace,
    },
    #[fail(display = "A storage failure occurred on node {}", node_id)]
    Storage {
        node_id: u64,
        #[cause]
        cause: Box<dyn Fail>,
        backtrace: Backtrace,
    },
    // NOTE: this error can be handled in NodeCore by retrying,
    //       so this should not be propagated...
    #[fail(display = "Failed to apply config change on node {}", node_id)]
    ConfChange {
        node_id: u64,
        #[cause]
        cause: Box<dyn Fail>,
        backtrace: Backtrace,
    },
    #[fail(display = "Failed to apply state change on node {}", node_id)]
    StateChange {
        node_id: u64,
        #[cause]
        cause: Box<dyn Fail>,
        backtrace: Backtrace,
    },
    #[fail(display = "Failed to broadcast message on node {}", node_id)]
    Broadcast {
        node_id: u64,
        #[cause]
        cause: TransportError,
        backtrace: Backtrace,
    },
    #[fail(display = "Failed to connect to gateway server at address {:?} on node {}", address, node_id)]
    GatewayConnect {
        node_id: u64,
        address: String,
        #[cause]
        cause: Box<dyn Fail>,
        backtrace: Backtrace,
    },
    #[fail(display = "The node {} cannot reach the leader {} of the cluster", node_id, leader_id)]
    LeaderNotReachable {
        node_id: u64,
        leader_id: u64,
        backtrace: Backtrace,
    },
}

pub type NodeResult<T> = std::result::Result<T, NodeError>;
