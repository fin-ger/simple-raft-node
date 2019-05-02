use std::net::{TcpStream, Shutdown};
use std::io::{Read, Write};

use crate::{Transport, TransportItem, TransportError, MachineCore};

pub struct TcpTransport<M: MachineCore> {
    stream: TcpStream,
    buf: Vec<u8>,
    src: u64,
    dest: u64,
    phantom: std::marker::PhantomData<M>,
}

impl<M: MachineCore> TcpTransport<M> {
    pub fn new(stream: TcpStream, src: u64, dest: u64) -> Self {
        let item_size = bincode::serialized_size(&<TransportItem<M> as Default>::default()).unwrap();

        Self {
            stream,
            buf: Vec::with_capacity(item_size as usize),
            src,
            dest,
            phantom: Default::default(),
        }
    }
}

impl<M: MachineCore> Transport<M> for TcpTransport<M> {
    fn send(&mut self, item: TransportItem<M>) -> Result<(), TransportError> {
        let data = bincode::serialize(&item).map_err(|e| {
            log::error!("failed to serialize transport item: {}", e);
            self.stream.shutdown(Shutdown::Both);
            TransportError::Disconnected
        })?;
        self.stream.write_all(&data)
            .map_err(|e| {
                log::error!("failed to write serialized transport item to TCP stream: {}", e);
                self.stream.shutdown(Shutdown::Both);
                TransportError::Disconnected
            })
    }

    fn recv(&mut self) -> Result<TransportItem<M>, TransportError> {
        self.stream.read_exact(&mut self.buf)
            .map_err(|e| {
                log::error!("failed to read from TCP transport: {}", e);
                self.stream.shutdown(Shutdown::Both);
                TransportError::Disconnected
            })?;

        bincode::deserialize(&self.buf)
            .map_err(|e| {
                log::error!("failed to deserialize transport item from TCP stream: {}", e);
                self.stream.shutdown(Shutdown::Both);
                TransportError::Disconnected
            })
    }

    fn try_recv(&mut self) -> Result<TransportItem<M>, TransportError> {
        let size = self.stream.peek(&mut self.buf)
            .map_err(|e| {
                log::error!("failed to peek on TCP transport: {}", e);
                self.stream.shutdown(Shutdown::Both);
                TransportError::Disconnected
            })?;

        if size >= self.buf.capacity() {
            return self.recv();
        }

        Err(TransportError::Empty)
    }

    fn src(&self) -> u64 {
        self.src
    }

    fn dest(&self) -> u64 {
        self.dest
    }
}
