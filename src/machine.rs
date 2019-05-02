use std::future::Future;
use std::task::{Poll, Context, Waker};
use std::pin::Pin;

use crossbeam::channel::{self, Sender, Receiver, TryRecvError};
use serde::{Serialize, de::DeserializeOwned};
use failure::Fail;

pub type MachineResult<T> = Result<T, MachineError>;

#[derive(Debug, Fail)]
pub enum MachineError {
    #[fail(display = "The proposal channels are not available")]
    ChannelsUnavailable,
    #[fail(display = "The proposed state change failed")]
    StateChange,
    #[fail(display = "The proposed state retrieval failed")]
    StateRetrieval,
}

#[derive(Debug, Fail)]
pub enum MachineCoreError {
    #[fail(display = "Deserialization of machine core failed")]
    Deserialization,
    #[fail(display = "Serialization of machine core failed")]
    Serialization,
}

#[derive(Clone, Debug)]
pub enum RequestKind<M: MachineCore> {
    StateChange(M::StateChange),
    StateRetrieval(M::StateIdentifier),
}

#[derive(Debug)]
pub struct Request<M: MachineCore> {
    pub response_tx: Sender<Response<M>>,
    pub waker: Waker,
    pub kind: RequestKind<M>,
}

#[derive(Debug)]
pub enum StateChangeResult {
    Success,
    Fail,
}

#[derive(Debug)]
pub enum StateRetrievalResult<M: MachineCore> {
    Found(M::StateValue),
    NotFound,
}

#[derive(Debug)]
pub enum Response<M: MachineCore> {
    StateChange(StateChangeResult),
    StateRetrieval(StateRetrievalResult<M>),
}

#[derive(Debug, Clone)]
pub struct RequestManager<M: MachineCore> {
    tx: Sender<Request<M>>,
}

struct RequestFuture<M: MachineCore, T: Sized + 'static, F: Fn(Response<M>) -> MachineResult<T> + Unpin> {
    kind: RequestKind<M>,
    response_handler: F,
    request_sent: bool,
    request_tx: Sender<Request<M>>,
    response_tx: Sender<Response<M>>,
    response_rx: Receiver<Response<M>>,
}

impl<M: MachineCore, T: Sized + 'static, F: Fn(Response<M>) -> MachineResult<T> + Unpin> RequestFuture<M, T, F> {
    fn new(kind: RequestKind<M>, response_handler: F, request_tx: Sender<Request<M>>) -> Self {
        let (response_tx, response_rx) = channel::unbounded();
        Self {
            kind,
            response_handler,
            request_tx,
            response_tx,
            response_rx,
            request_sent: false,
        }
    }
}

impl<M: MachineCore, T: Sized + 'static, F: Fn(Response<M>) -> MachineResult<T> + Unpin>
    Future for RequestFuture<M, T, F>
{
    type Output = MachineResult<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let fut = self.get_mut();

        if !fut.request_sent {
            fut.request_tx.send(Request {
                response_tx: fut.response_tx.clone(),
                waker: cx.waker().clone(),
                kind: fut.kind.clone(),
            }).map_err(|_| MachineError::ChannelsUnavailable)?;
            fut.request_sent = true;
        }

        match fut.response_rx.try_recv() {
            Ok(response) => Poll::Ready(fut.response_handler.call((response,))),
            Err(TryRecvError::Empty) => Poll::Pending,
            Err(TryRecvError::Disconnected) => Poll::Ready(Err(MachineError::ChannelsUnavailable)),
        }
    }
}

impl<M: MachineCore> RequestManager<M> {
    pub fn new(tx: Sender<Request<M>>) -> Self {
        Self {
            tx,
        }
    }

    fn request<T: Sized + 'static, F>(
        &self,
        kind: RequestKind<M>,
        response_handler: F,
    ) -> RequestFuture<M, T, F> where F: Fn(Response<M>) -> MachineResult<T> + Unpin {
        RequestFuture::new(kind, response_handler, self.tx.clone())
    }

    pub async fn apply(&self, state_change: M::StateChange) -> MachineResult<()> {
        await!(self.request(
            RequestKind::StateChange(state_change),
            |result| {
                match result {
                    Response::StateChange(StateChangeResult::Success) => Ok(()),
                    _ => Err(MachineError::StateChange),
                }
            },
        ))
    }

    pub async fn retrieve(
        &self, state_identifier: M::StateIdentifier,
    ) -> MachineResult<M::StateValue> {
        await!(self.request(
            RequestKind::StateRetrieval(state_identifier),
            |result| {
                match result {
                    Response::StateRetrieval(StateRetrievalResult::Found(value)) => Ok(value),
                    _ => Err(MachineError::StateRetrieval),
                }
            },
        ))
    }
}

pub trait Machine: Send + Clone + std::fmt::Debug {
    type Core: MachineCore;

    fn init(&mut self, request_manager: RequestManager<Self::Core>);
    fn core(&self) -> Self::Core;
}

pub trait MachineItem = std::fmt::Debug + Clone + Send + Unpin;

// the core has to own all its data
pub trait MachineCore: std::fmt::Debug + Clone + Send +'static {
    type StateChange: MachineItem + Serialize + DeserializeOwned;
    type StateIdentifier: MachineItem;
    type StateValue: MachineItem;

    fn deserialize(&mut self, data: Vec<u8>) -> Result<(), MachineCoreError>;
    fn serialize(&self) -> Result<Vec<u8>, MachineCoreError>;
    fn apply(&mut self, state_change: Self::StateChange);
    fn retrieve(
        &self,
        state_identifier: &Self::StateIdentifier,
    ) -> Result<&Self::StateValue, MachineError>;
}
