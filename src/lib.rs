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

mod proposal;
mod node_error;
mod node_core;
mod serde_polyfill;
mod machine;
mod transport;
mod storage;

pub mod machines;
pub mod transports;
pub mod storages;

pub use node_error::*;
pub use proposal::*;
pub use machine::*;
pub use transport::*;
pub use storage::*;

use std::sync::mpsc;
use std::thread::{self, JoinHandle};

use raft::Config;

use node_core::NodeCore;

pub struct Node<M: Machine> {
    thread_handle: JoinHandle<()>,
    machine: M,
}

// TODO: is 'static really needed / correct?
impl<M: Machine + 'static> Node<M> {
    pub fn new<T: Transport<M::Core> + 'static, S: Storage + 'static>(
        id: u64,
        mut machine: M,
        storage: S,
        transports: Vec<T>,
    ) -> Self {
        let (proposals_tx, proposals_rx) = mpsc::channel();
        let (answers_tx, answers_rx) = mpsc::channel();
        let config = Config {
            election_tick: 10,
            heartbeat_tick: 3,
            ..Default::default()
        };
        machine.init(ProposalChannel::new(proposals_tx, answers_rx));

        let node = NodeCore::new(
            format!("node_{}", id),
            config,
            machine.core(),
            storage,
            transports,
            proposals_rx,
            answers_tx,
        ).unwrap();

        // Here we spawn the node on a new thread and keep a handle so we can join on them later.
        let handle = thread::spawn(move || {
            node.run().unwrap();
        });

        Self {
            thread_handle: handle,
            machine,
        }
    }

    pub fn mut_machine(&mut self) -> &mut M {
        &mut self.machine
    }

    pub fn finalize(self) {
        drop(self.machine);
        self.thread_handle.join().unwrap();
    }
}
