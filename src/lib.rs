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
#![feature(
    vec_remove_item,
    drain_filter,
    trait_alias,
    fn_traits,
    async_await,
    await_macro,
)]

mod request;
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
pub use request::*;

pub use raft::Config;

use std::thread::{self, JoinHandle};
use std::sync::{Arc, Mutex};

use failure::Fail;
use crossbeam::channel;

use node_core::NodeCore;

pub struct Node<M: Machine> {
    id: u64,
    machine: M,
    handle: Option<JoinHandle<()>>,
    is_running: Arc<Mutex<bool>>,
}

impl<M: Machine> Node<M> {
    pub fn new<C: ConnectionManager<M::Core> + 'static, S: Storage + 'static>(
        config: Config,
        gateway: <C::Transport as Transport<M::Core>>::Address,
        mut machine: M,
        storage: S,
        connection_manager: C,
    ) -> Self {
        log::debug!("creating new node for id {}...", config.id);
        let (request_tx, request_rx) = channel::unbounded();
        machine.init(RequestManager::new(request_tx.clone()));

        let id = config.id;
        let mut node = NodeCore::new(
            config,
            gateway,
            machine.core(),
            storage,
            connection_manager,
            request_rx,
        ).unwrap();

        let is_running = Arc::new(Mutex::new(true));
        let is_running_copy = is_running.clone();

        log::trace!("spawing worker thread for node {}...", id);
        let handle = thread::spawn(move || {
            loop {
                log::trace!("beginning new cycle on node {}", node.id());

                if !*is_running.lock().unwrap() {
                    log::debug!("stopping worker thread of node {}", node.id());
                    break;
                }

                match node.advance() {
                    Ok(()) => {},
                    Err(err) => {
                        log::error!(
                            "advance of node {} failed: {:?}\n{}",
                            node.id(),
                            err,
                            err.backtrace().unwrap(),
                        );
                        break;
                    },
                };
            }
        });

        Self {
            id,
            machine,
            handle: Some(handle),
            is_running: is_running_copy,
        }
    }

    pub fn machine(&self) -> &M {
        &self.machine
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}

impl<M: Machine> Drop for Node<M> {
    fn drop(&mut self) {
        log::info!("node {} is shutting down...", self.id);
        *self.is_running.lock().unwrap() = false;
        self.handle.take().unwrap().join().unwrap();
    }
}
