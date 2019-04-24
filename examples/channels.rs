use log::info;

use simple_raft_node::transports::MpscChannelTransport;
use simple_raft_node::machines::{HashMapMachine, HashMapStateChange};
use simple_raft_node::storages::MemStorage;
use simple_raft_node::{Node, Proposal};

fn main() {
    env_logger::init();

    info!("Spawning nodes");
    let mut nodes: Vec<_> = MpscChannelTransport::create_transports(vec![1, 2, 3, 4, 5])
        .drain()
        .map(|(node_id, transports)| {
            // yeah, not quite simple yet - such a type argument mess
            Node::<HashMapMachine<u16, String>>::new::<_, MemStorage>(
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
