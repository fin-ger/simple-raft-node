use raft::eraftpb::Message;
use serde::{Serialize, Deserialize, de::DeserializeOwned};
use failure::{Fail, Backtrace};

use crate::serde_polyfill::MessagePolyfill;
use crate::{Proposal, Answer, MachineCore};

#[derive(Debug, Serialize, Deserialize)]
pub enum TransportItem<M: MachineCore, A: Address> {
    #[serde(bound(deserialize = "Proposal<M>: Deserialize<'de>"))]
    #[serde(bound(serialize = "Proposal<M>: Serialize"))]
    Proposal(Proposal<M>),
    Answer(Answer),
    Message(#[serde(with = "MessagePolyfill")] Message),
    Hello(u64, A),
    Welcome(u64, Vec<u64>, Vec<u64>),
    Broadcast(Vec<u8>),
}

#[derive(Debug, Fail)]
pub enum TransportError {
    #[fail(display = "Failed to receive or send item over transport, as the transport is disconnected")]
    Disconnected(Backtrace),
    #[fail(display = "Failed to receive item over transport, as the transport is empty")]
    Empty(Backtrace),
}

#[derive(Debug, Fail)]
#[fail(display = "Failed to establish a connection")]
pub struct ConnectError(#[cause] pub Box<dyn Fail>, pub Backtrace);

#[derive(Debug, Fail)]
pub enum AddressError {
    #[fail(display = "The transport address is currently not available")]
    NotAvailable(Backtrace),
}

pub trait ConnectionManager<M: MachineCore>: Send {
    type Transport: Transport<M>;

    fn listener_addr(&self) -> <Self::Transport as Transport<M>>::Address;
    fn is_this_node(&self, addr: &<Self::Transport as Transport<M>>::Address) -> bool;
    fn accept(&mut self) -> Option<Self::Transport>;
    fn connect(
        &mut self,
        address: &<Self::Transport as Transport<M>>::Address,
    ) -> Result<Self::Transport, ConnectError>;
}

pub trait Address =
    std::fmt::Debug
    + std::fmt::Display
    + Send
    + Sync;

/**
 * This trait describes a single transport (e.g. TCP socket) to one node in the cluster.
 */
pub trait Transport<M: MachineCore>: Send {
    type Address: Address + Serialize + DeserializeOwned + Clone + Sized + PartialEq + 'static;

    fn send(&mut self, item: TransportItem<M, Self::Address>) -> Result<(), TransportError>;
    fn try_recv(&mut self) -> Result<TransportItem<M, Self::Address>, TransportError>;
    fn src(&self) -> Result<Self::Address, AddressError>;
    fn dest(&self) -> Result<Self::Address, AddressError>;
    fn close(self);
}
