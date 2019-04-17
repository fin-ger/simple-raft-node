// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
#![feature(vec_remove_item)]
#![feature(trait_alias)]

mod proposals;
mod node_core;
mod serde_polyfill;
mod machine;
mod machines;
mod transport;
mod transports;

pub use machine::*;
pub use machines::*;
pub use transport::*;
pub use transports::*;

use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};

use log::{info, error};
use raft::{Config, storage::MemStorage};

use proposals::{Proposal, Answer};
use node_core::NodeCore;

// TODO: convert to lib and create example

fn main() {
    env_logger::init();

    info!("Spawning nodes");
    let mut nodes: Vec<_> = MpscChannelTransport::create_transports(vec![1, 2, 3, 4, 5])
        .drain()
        .map(|(node_id, transports)| {
            AsyncSimpleNode::<HashMapMachine<u16, String>>::new(
                node_id,
                Default::default(),
                Default::default(),
                transports,
            )
        })
        .collect();

    // Put 5 key-value pairs.
    (0..5u64)
        .filter(|i| {
            let proposal = Proposal::state_change(*i, HashMapStateChange {
                key: *i as u16,
                value: format!("hello, world {}", *i),
            });
            info!("Adding new proposal {}...", i);
            let res = nodes[0].propose(proposal);
            info!("Proposal {} was {}", i, res);
            res
        })
        .count();

    for node in nodes {
        node.finalize();
    }
}

struct AsyncSimpleNode<M: Machine> {
    thread_handle: JoinHandle<()>,
    proposals_tx: Sender<Proposal<M>>,
    answers_rx: Receiver<Answer>,
}

// TODO: is 'static really needed / correct?
impl<M: Machine + 'static> AsyncSimpleNode<M> {
    fn new<T: Transport<M> + 'static>(
        id: u64,
        machine: M,
        storage: MemStorage,
        transports: Vec<T>,
    ) -> Self {
        let (proposals_tx, proposals_rx) = mpsc::channel();
        let (answers_tx, answers_rx) = mpsc::channel();
        let config = Config {
            election_tick: 10,
            heartbeat_tick: 3,
            ..Default::default()
        };
        let node = NodeCore::new(
            format!("node_{}", id),
            config,
            machine,
            storage,
            transports,
            proposals_rx,
            answers_tx,
        ).unwrap();
        // Here we spawn the node on a new thread and keep a handle so we can join on them later.
        let handle = thread::spawn(|| {
            node.run().unwrap();
        });

        Self {
            thread_handle: handle,
            proposals_tx,
            answers_rx,
        }
    }

    fn propose(&mut self, proposal: Proposal<M>) -> bool {
        let id = proposal.id();
        self.proposals_tx.send(proposal).unwrap();
        let answer = self.answers_rx.recv().unwrap();

        if answer.id != id {
            error!("Proposal id not identical to answer id!");
            return false;
        }

        return answer.value;
    }

    fn finalize(self) {
        drop(self.proposals_tx);
        drop(self.answers_rx);
        self.thread_handle.join().unwrap();
    }
}
