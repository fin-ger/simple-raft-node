use raft::eraftpb::Message;
use serde::{Serialize, Deserialize};
use failure::Fail;

use crate::serde_polyfill::MessagePolyfill;
use crate::{Proposal, Answer, AnswerKind, MachineCore};

#[derive(Serialize, Deserialize)]
pub enum TransportItem<M: MachineCore> {
    #[serde(bound(deserialize = "Proposal<M>: Deserialize<'de>"))]
    #[serde(bound(serialize = "Proposal<M>: Serialize"))]
    Proposal(Proposal<M>),
    Answer(Answer),
    Message(#[serde(with = "MessagePolyfill")] Message),
}

impl<M: MachineCore> Default for TransportItem<M> {
    fn default() -> Self {
        TransportItem::Answer(Answer { id: 0, kind: AnswerKind::Success })
    }
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
    fn send(&mut self, item: TransportItem<M>) -> Result<(), TransportError>;
    fn recv(&mut self) -> Result<TransportItem<M>, TransportError>;
    fn try_recv(&mut self) -> Result<TransportItem<M>, TransportError>;
    fn src(&self) -> u64;
    fn dest(&self) -> u64;
}
