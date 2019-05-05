use std::future::Future;
use std::task::{Poll, Context, Waker};
use std::pin::Pin;

use failure::Fail;
use crossbeam::channel::{self, Sender, Receiver, TryRecvError};

use crate::{MachineCore};

#[derive(Debug, Fail)]
pub enum RequestError {
    #[fail(display = "The proposal channels are not available")]
    ChannelsUnavailable,
    #[fail(display = "The proposed state change failed")]
    StateChange,
    #[fail(display = "The proposed state retrieval failed")]
    StateRetrieval,
}

pub type RequestResult<T> = Result<T, RequestError>;

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
pub enum Response<M: MachineCore> {
    StateChange(GeneralResult),
    StateRetrieval(StateRetrievalResult<M>),
    ConfChange(GeneralResult),
}

#[derive(Debug)]
pub enum GeneralResult {
    Success,
    Fail,
}

#[derive(Debug)]
pub enum StateRetrievalResult<M: MachineCore> {
    Found(M::StateValue),
    NotFound,
}

pub trait RequestHandler<M: MachineCore, V: Sized + Unpin + 'static>
    = Fn(Response<M>) -> RequestResult<V> + Unpin;

pub struct RequestFuture<M: MachineCore, V: Sized + Unpin + 'static, F: RequestHandler<M, V>> {
    kind: RequestKind<M>,
    response_handler: F,
    request_sent: bool,
    request_tx: Sender<Request<M>>,
    response_tx: Sender<Response<M>>,
    response_rx: Receiver<Response<M>>,
    phantom: std::marker::PhantomData<V>,
}

impl<M: MachineCore, V: Sized + Unpin + 'static, F: RequestHandler<M, V>>
    RequestFuture<M, V, F>
{
    fn new(kind: RequestKind<M>, response_handler: F, request_tx: Sender<Request<M>>) -> Self {
        let (response_tx, response_rx) = channel::unbounded();
        Self {
            kind,
            response_handler,
            request_tx,
            response_tx,
            response_rx,
            request_sent: false,
            phantom: Default::default(),
        }
    }
}

impl<M: MachineCore, V: Sized + Unpin + 'static, F: RequestHandler<M, V>>
    Future for RequestFuture<M, V, F>
{
    type Output = RequestResult<V>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let fut = self.get_mut();

        if !fut.request_sent {
            fut.request_tx.send(Request {
                response_tx: fut.response_tx.clone(),
                waker: cx.waker().clone(),
                kind: fut.kind.clone(),
            }).map_err(|_| RequestError::ChannelsUnavailable)?;
            fut.request_sent = true;
        }

        match fut.response_rx.try_recv() {
            Ok(response) => Poll::Ready(fut.response_handler.call((response,))),
            Err(TryRecvError::Empty) => Poll::Pending,
            Err(TryRecvError::Disconnected) => Poll::Ready(Err(RequestError::ChannelsUnavailable)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RequestManager<M: MachineCore> {
    tx: Sender<Request<M>>,
}

impl<M: MachineCore> RequestManager<M> {
    pub fn new(tx: Sender<Request<M>>) -> Self {
        Self {
            tx,
        }
    }

    pub fn request<V: Sized + Unpin + 'static, F: Fn(Response<M>) -> RequestResult<V> + Unpin>(
        &self,
        kind: RequestKind<M>,
        response_handler: F,
    ) -> RequestFuture<M, V, F> {
        RequestFuture::new(kind, response_handler, self.tx.clone())
    }
}
