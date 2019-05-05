use std::sync::{Arc, Mutex};
use std::sync::mpsc::{self, Sender, Receiver, TryRecvError};
use std::collections::HashMap;

use failure::Backtrace;

use crate::{Transport, TransportItem, TransportError, ConnectionManager, ConnectError, MachineCore};

#[derive(Clone)]
pub struct MpscChannelConnectionManager<M: MachineCore> {
    transports: Arc<Mutex<HashMap<u64, Vec<MpscChannelTransport<M>>>>>,
    node_id: u64,
}

impl<M: MachineCore> MpscChannelConnectionManager<M> {
    pub fn new_managers(node_ids: Vec<u64>) -> Vec<Self> {
        let mut map: HashMap<_, _> = node_ids.iter()
            .map(|i| (*i, Vec::new()))
            .collect();

        for src_id in &node_ids {
            for dest_id in node_ids.iter().skip(*src_id as usize) {
                if src_id != dest_id {
                    let src_channel = mpsc::channel();
                    let dest_channel = mpsc::channel();

                    src_channel.0.send(TransportItem::Hello(*dest_id)).unwrap();
                    dest_channel.0.send(TransportItem::Hello(*src_id)).unwrap();
                    map.get_mut(src_id).unwrap().push(MpscChannelTransport {
                        src_id: *src_id,
                        dest_id: *dest_id,
                        send: dest_channel.0,
                        recv: src_channel.1,
                    });
                    map.get_mut(dest_id).unwrap().push(MpscChannelTransport {
                        src_id: *dest_id,
                        dest_id: *src_id,
                        send: src_channel.0,
                        recv: dest_channel.1,
                    });
                }
            }
        }

        let transports = Arc::new(Mutex::new(map));
        node_ids.iter()
            .map(|id| Self { transports: transports.clone(), node_id: *id })
            .collect::<Vec<_>>()
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }
}

impl<M: MachineCore> ConnectionManager<M> for MpscChannelConnectionManager<M> {
    type Transport = MpscChannelTransport<M>;

    fn accept(&self) -> Option<MpscChannelTransport<M>> {
        let opt = self.transports
            .lock().unwrap()
            .get_mut(&self.node_id).unwrap()
            .pop();
        if let Some(transport) = opt {
            return Some(transport);
        }
        None
    }

    fn connect(
        &self,
        address: <Self::Transport as Transport<M>>::Address,
    ) -> Result<MpscChannelTransport<M>, ConnectError> {
        self.transports
            .lock().unwrap()
            .get_mut(&self.node_id).unwrap()
            .drain_filter(|t| t.dest_addr() == address)
            .next()
            .ok_or(ConnectError(
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "connection not available",
                )),
                Backtrace::new(),
            ))
    }
}

pub struct MpscChannelTransport<M: MachineCore> {
    src_id: u64,
    dest_id: u64,
    send: Sender<TransportItem<M>>,
    recv: Receiver<TransportItem<M>>,
}

impl<M: MachineCore> Transport<M> for MpscChannelTransport<M> {
    type Address = u64;

    fn send(&mut self, item: TransportItem<M>) -> Result<(), TransportError> {
        self.send.send(item).map_err(|_| TransportError::Disconnected(Backtrace::new()))
    }

    fn try_recv(&mut self) -> Result<TransportItem<M>, TransportError> {
        self.recv.try_recv().map_err(|e| {
            match e {
                TryRecvError::Empty => TransportError::Empty(Backtrace::new()),
                TryRecvError::Disconnected => TransportError::Disconnected(Backtrace::new()),
            }
        })
    }

    fn src_id(&self) -> u64 {
        self.src_id
    }

    fn dest_id(&self) -> u64 {
        self.dest_id
    }

    fn src_addr(&self) -> u64 {
        self.src_id
    }

    fn dest_addr(&self) -> u64 {
        self.dest_id
    }
}
