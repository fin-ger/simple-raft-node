#![feature(
    vec_remove_item,
    drain_filter,
    trait_alias,
    fn_traits,
    async_await,
)]

/* Hello, dear reader of this source code.
 *
 * I will guide you though this code and try to explain what
 * every piece and bit does. When something is unclear, don't
 * hesitate to create an issue on github! :D
 *
 * When making changes to this source code, I ask you to keep
 * these source code comments up-to-date. Thanks!
 *
 * Let's start!
 *
 * Why no API documentation?
 *
 * This is a prototype and the API and the functionality are
 * far from stable. This work is meant to explore possibilities!
 * As long as not otherwise stated, this work will remain undesirable
 * for production purposes!
 *
 * What is this file all about?
 *
 * This file includes the `Node`. A `Node` manages a `NodeCore`,
 * a connection manager, a storage, and a machine.
 *
 * The `Node` itself provides a thread-safe and asynchronous interface
 * to the contained machine. Also, the thread which is dispatched
 * during the creation of a `Node` is managed by the `Node`. When
 * starting the new thread, a `NodeCore` gets created which is
 * continuously "advanced". When advancing the `NodeCore`, the core
 * will start working on all accumulated tasks since the last advance.
 * This includes communication with other machines, updating the
 * state machine, and establishing and checking connections to other
 * machines.
 *
 * The `Node` uses a "gateway" to join an existing cluster. A gateway is
 * a resource identifier defined by the transport implementation. For
 * TCP transports this is either an ip address with a port or a domain
 * name with a port. The gateway algorithm used for joining an existing
 * cluster is far from optimal. It currently requires the gateway address
 * to be comparable to the instances own ethernet address to decide
 * whether a gateway is valid or not. When a gateway is not valid, a
 * new cluster will be created with the own instance configured as the
 * leader of the cluster. This creates several unnecessary restrictions
 * to the deployment of an application built with this library. E.g. it
 * makes it impossible to use the service address of a Kubernetes service
 * as a gateway address but rather requires the gateway to be the domain
 * of a specific Pod. This results in a fatality of the cluster when the
 * Pod serving the gateway gets unavailable as no new nodes can join the
 * old cluster. However, other nodes can be perfectly used as alternative
 * gateway nodes, this would require a dynamic configuration of the gateway.
 * This is not always possible and should definitely be fixed!
 *
 * Currently, a `Node` is not shutting itself down when it encounters an
 * error. This is due to the above deployment limitation, where the full
 * cluster will die, when the gateway node encounters an error. Whatever
 * happens, a node will try to still be functional as a gateway. When this
 * fails (for whatever reason), the cluster will be locked to dead. This
 * is because the orchestrator has no chance of noticing, that a node is
 * not healthy (exit code, liveness probe). Therefore, the node will
 * keep running until the user asks it to shutdown.
 */
mod request;
mod proposal;
mod node_error;
mod node_core;
mod serde_polyfill;
mod machine;
mod transport;
mod storage;
mod utils;

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
use log;

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

    pub fn stop_handler(&self) -> Box<dyn Fn() + Send + Sync> {
        let r = self.is_running.clone();
        let id = self.id;

        Box::new(move || {
            log::info!("node {} is shutting down...", id);
            *r.lock().unwrap() = false;
        })
    }
}

impl<M: Machine> Drop for Node<M> {
    fn drop(&mut self) {
        self.stop_handler()();
        self.handle.take().unwrap().join().unwrap();
    }
}
