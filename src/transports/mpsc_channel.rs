use std::sync::{Arc, Mutex};
use std::sync::mpsc::{self, Sender, Receiver, TryRecvError};
use std::collections::HashMap;

use failure::Backtrace;

use crate::{
    Transport,
    TransportItem,
    TransportError,
    ConnectionManager,
    ConnectError,
    AddressError,
    MachineCore,
};

pub struct MpscChannelConnectionManager<M: MachineCore> {
    transports: Arc<Mutex<HashMap<u64, Vec<MpscChannelTransport<M>>>>>,
    node_id: u64,
}

impl<M: MachineCore> MpscChannelConnectionManager<M> {
    pub fn new_managers(node_ids: Vec<u64>) -> Vec<Self> {
        log::debug!("creating {} new MPSC-channel connection managers", node_ids.len());

        let mut map: HashMap<_, _> = node_ids.iter()
            .map(|i| (*i, Vec::new()))
            .collect();

        for src_id in &node_ids {
            for dest_id in node_ids.iter().skip(*src_id as usize) {
                if src_id != dest_id {
                    let src_channel = mpsc::channel();
                    let dest_channel = mpsc::channel();

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
            .map(|id| Self {
                transports: transports.clone(),
                node_id: *id,
            })
            .collect::<Vec<_>>()
    }
}

impl<M: MachineCore> ConnectionManager<M> for MpscChannelConnectionManager<M> {
    type Transport = MpscChannelTransport<M>;

    fn listener_addr(&self) -> <Self::Transport as Transport<M>>::Address {
        self.node_id
    }

    fn is_this_node(&self, addr: &<Self::Transport as Transport<M>>::Address) -> bool {
        *addr == self.node_id
    }

    fn accept(&mut self) -> Option<MpscChannelTransport<M>> {
        let dests: Vec<u64> = self.transports.lock().unwrap()
            .iter()
            .flat_map(|(src, transports)| {
                if *src != self.node_id {
                    if !transports.iter().any(|t| t.dest().unwrap() == self.node_id) {
                        return vec![*src];
                    }
                }

                Vec::new()
            })
            .collect();

        for dest in dests {
            let new_transport = self.transports.lock().unwrap()
                .get_mut(&self.node_id)
                .unwrap()
                .drain_filter(|t| t.dest().unwrap() == dest)
                .next();

            if let Some(transport) = new_transport {
                log::debug!(
                    "accepting new MPSC-channel connection on node {} from {}",
                    self.node_id,
                    dest,
                );
                return Some(transport);
            }
        }

        None
    }

    fn connect(
        &mut self,
        address: &<Self::Transport as Transport<M>>::Address,
    ) -> Result<MpscChannelTransport<M>, ConnectError> {
        log::debug!("attempting to connect from {} to {}", self.node_id, address);
        self.transports.lock().unwrap()
            .get_mut(&self.node_id).unwrap()
            .drain_filter(|t| t.dest().unwrap() == *address)
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
    send: Sender<TransportItem<M, u64>>,
    recv: Receiver<TransportItem<M, u64>>,
}

impl<M: MachineCore> Transport<M> for MpscChannelTransport<M> {
    type Address = u64;

    fn send(&mut self, item: TransportItem<M, Self::Address>) -> Result<(), TransportError> {
        log::trace!("sending item {:?} over MPSC-channel transport", item);
        self.send.send(item).map_err(|_| TransportError::Disconnected(Backtrace::new()))
    }

    fn try_recv(&mut self) -> Result<TransportItem<M, Self::Address>, TransportError> {
        self.recv.try_recv().map_err(|e| {
            match e {
                TryRecvError::Empty => TransportError::Empty(Backtrace::new()),
                TryRecvError::Disconnected => TransportError::Disconnected(Backtrace::new()),
            }
        })
    }

    fn src(&self) -> Result<u64, AddressError> {
        Ok(self.src_id)
    }

    fn dest(&self) -> Result<u64, AddressError> {
        Ok(self.dest_id)
    }
}
