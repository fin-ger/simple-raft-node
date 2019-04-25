use raft::eraftpb::Message;
use serde::{Serialize, Deserialize};
use failure::Fail;

use crate::serde_polyfill::MessagePolyfill;
use crate::{Proposal, Answer, MachineCore};

#[derive(Serialize, Deserialize)]
pub enum TransportItem<M: MachineCore> {
    Proposal(Proposal<M>),
    Answer(Answer<M>),
    Message(#[serde(with = "MessagePolyfill")] Message),
}

#[derive(Debug, Fail)]
pub enum TransportError {
    #[fail(display = "Failed to receive or send item over transport, as the transport is disconnected")]
    Disconnected,
    #[fail(display = "Failed to receive item over transport, as the transport is empty")]
    Empty,
}

/**
 * This trait describes a single transport (e.g. TCP socket) to one node in the cluster.
 */
pub trait Transport<M: MachineCore>: Send {
    fn send(&self, item: TransportItem<M>) -> Result<(), TransportError>;
    fn recv(&self) -> Result<TransportItem<M>, TransportError>;
    fn try_recv(&self) -> Result<TransportItem<M>, TransportError>;
    fn src(&self) -> u64;
    fn dest(&self) -> u64;
}
