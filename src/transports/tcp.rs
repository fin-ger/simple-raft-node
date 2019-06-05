use std::net::{TcpStream, TcpListener, Shutdown, SocketAddr};
use std::io::{BufReader, Cursor, BufRead, Write, ErrorKind};

use failure::{Fail, Backtrace};
use get_if_addrs::get_if_addrs;

use crate::{
    utils,
    Transport,
    TransportItem,
    TransportError,
    ConnectionManager,
    ConnectError,
    AddressError,
    MachineCore,
};

#[derive(Debug, Fail)]
#[fail(display = "Failed to bind listener for TCP transport on {}", _0)]
pub struct BindError(pub SocketAddr, #[cause] pub Box<dyn Fail>, pub Backtrace);

pub struct TcpConnectionManager<M: MachineCore> {
    listener: TcpListener,
    local_addr: SocketAddr,
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
            local_addr: address,
            phantom: Default::default(),
        })
    }
}

impl<M: MachineCore> ConnectionManager<M> for TcpConnectionManager<M> {
    type Transport = TcpTransport<M>;

    fn listener_addr(&self) -> <Self::Transport as Transport<M>>::Address {
        self.local_addr
    }

    fn is_this_node(&self, addr: &<Self::Transport as Transport<M>>::Address) -> bool {
        get_if_addrs()
            .unwrap()
            .iter()
            .any(|iface| iface.ip() == addr.ip() && addr.port() == self.local_addr.port())
    }

    fn accept(&mut self) -> Option<TcpTransport<M>> {
        match self.listener.accept() {
            Ok((stream, _)) => {
                log::debug!(
                    "accepting new connection on {} from {}",
                    self.local_addr,
                    stream.peer_addr().unwrap(),
                );
                Some(TcpTransport::from_stream(self.local_addr, None, stream))
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
        TcpTransport::from_addr(self.local_addr, *address)
    }
}

pub struct TcpTransport<M: MachineCore> {
    local_addr: SocketAddr,
    peer_addr: Option<SocketAddr>,
    buffer: BufReader<TcpStream>,
    phantom: std::marker::PhantomData<M>,
}

impl<M: MachineCore> TcpTransport<M> {
    pub fn from_addr(local_addr: SocketAddr, peer_addr: SocketAddr) -> Result<Self, ConnectError> {
        let stream = TcpStream::connect(peer_addr)
            .map_err(|e| ConnectError(Box::new(e), Backtrace::new()))?;

        Ok(Self::from_stream(local_addr, Some(peer_addr), stream))
    }

    pub fn from_stream(
        local_addr: SocketAddr,
        peer_addr: Option<SocketAddr>,
        stream: TcpStream,
    ) -> Self {
        stream.set_nonblocking(true).unwrap();

        Self {
            local_addr,
            peer_addr,
            buffer: BufReader::new(stream),
            phantom: Default::default(),
        }
    }
}

impl<M: MachineCore> Transport<M> for TcpTransport<M> {
    type Address = SocketAddr;

    fn send(&mut self, item: TransportItem<M, Self::Address>) -> Result<(), TransportError> {
        log::trace!("sending item {:?} over TCP transport", item);
        let data = utils::serialize(&item).map_err(|e| {
            log::error!("failed to serialize transport item: {}", e);
            let _ = self.buffer.get_ref().shutdown(Shutdown::Both);
            TransportError::Disconnected(Backtrace::new())
        })?;
        self.buffer.get_ref().write_all(&data)
            .map_err(|e| {
                log::error!("failed to write serialized transport item to TCP stream: {}", e);
                let _ = self.buffer.get_ref().shutdown(Shutdown::Both);
                TransportError::Disconnected(Backtrace::new())
            })
    }

    fn try_recv(&mut self) -> Result<TransportItem<M, Self::Address>, TransportError> {
        log::trace!("trying to receive item on TCP transport {:?}", self.src());

        match self.buffer.get_ref().take_error() {
            Ok(err) => {
                if err.is_some() {
                    log::error!("TCP transport has error: {:?}", err);
                    let _ = self.buffer.get_ref().shutdown(Shutdown::Both);
                    return Err(TransportError::Disconnected(Backtrace::new()));
                }
            },
            Err(err) => {
                log::error!("failed to check TCP transport for errors: {}", err);
                let _ = self.buffer.get_ref().shutdown(Shutdown::Both);
                return Err(TransportError::Disconnected(Backtrace::new()));
            }
        }

        let item;
        let position = match self.buffer.fill_buf() {
            Ok(buf) => {
                let mut cur = Cursor::new(buf);
                item = match utils::deserialize_from(&mut cur) {
                    Ok(item) => item,
                    Err(_) => return Err(TransportError::Empty(Backtrace::new())),
                };
                cur.position() as usize
            },
            Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                return Err(TransportError::Empty(Backtrace::new()));
            },
            Err(err) => {
                log::error!("failed to read from TCP transport: {}", err);
                let _ = self.buffer.get_ref().shutdown(Shutdown::Both);
                return Err(TransportError::Disconnected(Backtrace::new()));
            },
        };

        log::trace!("got new item on TCP transport {:?}", self.src());
        self.buffer.consume(position);
        Ok(item)
    }

    fn src(&self) -> Result<Self::Address, AddressError> {
        Ok(self.local_addr)
    }

    fn dest(&self) -> Result<Self::Address, AddressError> {
        self.peer_addr.ok_or(AddressError::NotAvailable(Backtrace::new()))
    }

    fn close(self) {
        // The only error that can occur during shutdown is a NotConnected error.
        // When the TCP stream is already closed, this is fine for us -> ignore
        let _ = self.buffer.into_inner().shutdown(Shutdown::Both);
    }
}
