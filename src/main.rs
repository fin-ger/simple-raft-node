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

mod proposals;
mod node_core;
mod transport;

use std::collections::{HashMap};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};

use log::{info, error};

use proposals::{Proposal, Answer};
use node_core::NodeCore;
use transport::TransportItem;

fn main() {
    env_logger::init();

    info!("Setting up test");

    // Create 5 mailboxes to send/receive messages. Every node holds a `Receiver` to receive
    // messages from others, and uses the respective `Sender` to send messages to others.
    let (mut tx_vec, mut rx_vec) = (Vec::new(), Vec::new());
    for _ in 0..5 {
        let (tx, rx) = mpsc::channel();
        tx_vec.push(tx);
        rx_vec.push(rx);
    }

    info!("Spawning nodes");
    let mut nodes = Vec::new();
    for (i, rx) in rx_vec.into_iter().enumerate() {
        // A map[peer_id -> sender]. In the example we create 5 nodes, with ids in [1, 5].
        let mailboxes = (1..6u64).zip(tx_vec.iter().cloned()).collect();
        let raft = AsyncSimpleNode::new(i as u64 + 1, rx, mailboxes);
        nodes.push(raft);
    }

    // Put 5 key-value pairs.
    (0..5u64)
        .filter(|i| {
            let proposal = Proposal::state_change(*i, *i as u16, "hello, world".to_string());
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

struct AsyncSimpleNode {
    thread_handle: JoinHandle<()>,
    proposals_tx: Sender<Proposal>,
    answers_rx: Receiver<Answer>,
}

impl AsyncSimpleNode {
    fn new(
        id: u64,
        my_mailbox: Receiver<TransportItem>,
        mailboxes: HashMap<u64, Sender<TransportItem>>,
    ) -> Self {
        let (proposals_tx, proposals_rx) = mpsc::channel();
        let (answers_tx, answers_rx) = mpsc::channel();
        let node = NodeCore::new(id, my_mailbox, mailboxes, proposals_rx, answers_tx);
        // Here we spawn the node on a new thread and keep a handle so we can join on them later.
        let handle = thread::spawn(|| {
            node.run();
        });

        Self {
            thread_handle: handle,
            proposals_tx,
            answers_rx,
        }
    }

    fn propose(&mut self, proposal: Proposal) -> bool {
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
