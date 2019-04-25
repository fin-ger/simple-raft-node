use log::info;

use simple_raft_node::transports::MpscChannelTransport;
use simple_raft_node::machines::HashMapMachine;
use simple_raft_node::storages::MemStorage;
use simple_raft_node::Node;

fn main() {
    env_logger::init();

    info!("Spawning nodes");
    let mut nodes: Vec<Node<HashMapMachine<u16, String>>> = MpscChannelTransport::create_transports(vec![1, 2, 3, 4, 5])
        .drain()
        .map(|(node_id, transports)| {
            // yeah, not quite simple yet - such a type argument mess
            Node::new::<_, MemStorage>(
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
            info!("Adding new proposal {}...", i);
            nodes[0].mut_machine().put(*i as u16, format!("hello, world {}", *i)).unwrap();
            info!("Proposal {} was successful", i);
            true
        })
        .count();

    for node in nodes {
        node.finalize();
    }
}
