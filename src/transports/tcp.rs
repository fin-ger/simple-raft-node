use std::net::{TcpStream, TcpListener, Shutdown, SocketAddr};
use std::io::{Read, Write, ErrorKind};

use failure::{Fail, Backtrace};

use crate::{Transport, TransportItem, TransportError, ConnectionManager, ConnectError, MachineCore};

#[derive(Debug, Fail)]
#[fail(display = "Failed to bind listener for TCP transport on {}", _0)]
pub struct BindError(pub SocketAddr, #[cause] pub Box<Fail>, pub Backtrace);

pub struct TcpConnectionManager<M: MachineCore> {
    listener: TcpListener,
    phantom: std::marker::PhantomData<M>,
}

impl<M: MachineCore> TcpConnectionManager<M> {
    pub fn new(address: SocketAddr) -> Result<Self, BindError> {
        let listener = TcpListener::bind(address)
            .map_err(|e| BindError(address, Box::new(e), Backtrace::new()))?;
        listener.set_nonblocking(true)
            .map_err(|e| BindError(address, Box::new(e), Backtrace::new()))?;

        Ok(Self {
            listener,
            phantom: Default::default(),
        })
    }
}

impl<M: MachineCore> ConnectionManager<M> for TcpConnectionManager<M> {
    type Transport = TcpTransport<M>;

    fn accept(&mut self) -> Option<TcpTransport<M>> {
        match self.listener.accept() {
            Ok((stream, _)) => {
                log::debug!(
                    "accepting new connection on {} from {}",
                    stream.local_addr().unwrap(),
                    stream.peer_addr().unwrap(),
                );
                Some(TcpTransport::from_stream(stream))
            },
            Err(ref err) if err.kind() == ErrorKind::WouldBlock => None,
            Err(err) => {
                log::error!("failed to accept connection: {}", err);
                None
            }
        }
    }

    fn connect(
        &mut self,
        address: &<Self::Transport as Transport<M>>::Address,
    ) -> Result<TcpTransport<M>, ConnectError> {
        log::debug!("connecting to address {}", address);
        TcpTransport::from_addr(*address)
    }
}

pub struct TcpTransport<M: MachineCore> {
    stream: TcpStream,
    buf: Vec<u8>,
    phantom: std::marker::PhantomData<M>,
}

impl<M: MachineCore> TcpTransport<M> {
    pub fn from_addr(addr: SocketAddr) -> Result<Self, ConnectError> {
        let stream = TcpStream::connect(addr)
            .map_err(|e| ConnectError(Box::new(e), Backtrace::new()))?;

        Ok(Self::from_stream(stream))
    }

    pub fn from_stream(stream: TcpStream) -> Self {
        stream.set_nonblocking(true).unwrap();

        Self {
            stream,
            buf: vec![0 as u8; item_size as usize],
            phantom: Default::default(),
        }
    }
}

impl<M: MachineCore> Transport<M> for TcpTransport<M> {
    type Address = SocketAddr;

    fn send(&mut self, item: TransportItem<M>) -> Result<(), TransportError> {
        log::debug!("sending item {:?} over TCP transport", item);
        let data = bincode::serialize(&item).map_err(|e| {
            log::error!("failed to serialize transport item: {}", e);
            let _ = self.stream.shutdown(Shutdown::Both);
            TransportError::Disconnected(Backtrace::new())
        })?;
        log::debug!("sending buffer data {:?}", data.len());
        self.stream.write_all(&data)
            .map_err(|e| {
                log::error!("failed to write serialized transport item to TCP stream: {}", e);
                let _ = self.stream.shutdown(Shutdown::Both);
                TransportError::Disconnected(Backtrace::new())
            })
    }

    fn try_recv(&mut self) -> Result<TransportItem<M>, TransportError> {
        log::trace!("trying to receive item on TCP transport {}", self.src());

        match self.stream.read_exact(&mut self.buf) {
            Ok(()) => {},
            Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                return Err(TransportError::Empty(Backtrace::new()));
            },
            Err(err) => {
                log::error!("failed to read from TCP transport: {}", err);
                let _ = self.stream.shutdown(Shutdown::Both);
                return Err(TransportError::Disconnected(Backtrace::new()))
            },
        };

        log::debug!("got buffer {:?}", self.buf.len());

        bincode::deserialize(&self.buf)
            .map_err(|e| {
                log::error!("failed to deserialize transport item from TCP stream: {}", e);
                let _ = self.stream.shutdown(Shutdown::Both);
                TransportError::Disconnected(Backtrace::new())
            })
    }

    fn src(&self) -> Self::Address {
        self.stream.local_addr().unwrap()
    }

    fn dest(&self) -> Self::Address {
        self.stream.peer_addr().unwrap()
    }
}
