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
#![feature(vec_remove_item, trait_alias, fn_traits, async_await, await_macro)]

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

use std::thread::{self, JoinHandle};
use std::sync::{Arc, Mutex};

use raft::Config;
use crossbeam::channel;

use node_core::NodeCore;

pub struct Node<M: Machine> {
    name: String,
    machine: M,
    handle: Option<JoinHandle<()>>,
    is_running: Arc<Mutex<bool>>,
}

impl<M: Machine> Node<M> {
    pub fn new<IntoString: Into<String>, T: Transport<M::Core> + 'static, S: Storage + 'static>(
        name: IntoString,
        mut machine: M,
        storage: S,
        transports: Vec<T>,
    ) -> Self {
        let (response_tx, response_rx) = channel::unbounded();
        let config = Config {
            election_tick: 10,
            heartbeat_tick: 3,
            ..Default::default()
        };
        machine.init(RequestManager::new(response_tx));
        let name_s = name.into();

        let mut node = NodeCore::new(
            name_s.clone(),
            config,
            machine.core(),
            storage,
            transports,
            response_rx,
        ).unwrap();

        let is_running = Arc::new(Mutex::new(true));
        let is_running_copy = is_running.clone();

        let handle = thread::spawn(move || {
            loop {
                if !*is_running.lock().unwrap() {
                    break;
                }

                match node.advance() {
                    Ok(()) => {},
                    Err(err) => {
                        log::error!("advance of node failed: {}", err);
                        break;
                    },
                };
            }
        });

        Self {
            name: name_s,
            machine,
            handle: Some(handle),
            is_running: is_running_copy,
        }
    }

    pub fn machine(&self) -> &M {
        &self.machine
    }

    pub fn name(&self) -> &String {
        &self.name
    }
}

impl<M: Machine> Drop for Node<M> {
    fn drop(&mut self) {
        log::info!("{} is shutting down...", self.name);
        *self.is_running.lock().unwrap() = false;
        self.handle.take().unwrap().join().unwrap();
    }
}
