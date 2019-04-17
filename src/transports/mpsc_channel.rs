use std::sync::mpsc::{self, Sender, Receiver, TryRecvError};
use std::collections::HashMap;

use crate::{Transport, TransportItem, TransportError};

pub struct MpscChannelTransport {
    src: u64,
    dest: u64,
    send: Sender<TransportItem>,
    recv: Receiver<TransportItem>,
}

impl MpscChannelTransport {
    pub fn create_transports(node_ids: Vec<u64>) -> HashMap<u64, Vec<Self>> {
        let mut map: HashMap<_, _> = node_ids.iter()
            .map(|i| (*i, Vec::new()))
            .collect();

        for src in &node_ids {
            for dest in node_ids.iter().skip(*src as usize) {
                if src != dest {
                    let src_channel = mpsc::channel();
                    let dest_channel = mpsc::channel();

                    map.get_mut(src).unwrap().push(Self {
                        src: *src,
                        dest: *dest,
                        send: dest_channel.0,
                        recv: src_channel.1,
                    });
                    map.get_mut(dest).unwrap().push(Self {
                        src: *dest,
                        dest: *src,
                        send: src_channel.0,
                        recv: dest_channel.1,
                    });
                }
            }
        }

        map
    }
}

impl Transport for MpscChannelTransport {
    fn send(&self, item: TransportItem) -> Result<(), TransportError> {
        self.send.send(item).map_err(|_| TransportError::Disconnected)
    }

    fn recv(&self) -> Result<TransportItem, TransportError> {
        self.recv.recv().map_err(|_| TransportError::Disconnected)
    }

    fn try_recv(&self) -> Result<TransportItem, TransportError> {
        self.recv.try_recv().map_err(|e| {
            match e {
                TryRecvError::Empty => TransportError::Empty,
                TryRecvError::Disconnected => TransportError::Disconnected,
            }
        })
    }

    fn src(&self) -> u64 {
        self.src
    }

    fn dest(&self) -> u64 {
        self.dest
    }
}
