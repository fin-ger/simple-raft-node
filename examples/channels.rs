#![feature(async_await, await_macro)]

use simple_raft_node::transports::MpscChannelTransport;
use simple_raft_node::machines::HashMapMachine;
use simple_raft_node::storages::MemStorage;
use simple_raft_node::{Node};

#[runtime::main]
async fn main() {
    env_logger::init();

    log::info!("Spawning nodes");

    let nodes: Vec<Node<HashMapMachine<i64, String>>> = MpscChannelTransport::create_transports(
        vec![1, 2, 3, 4, 5]
    )
        .drain()
        .map(|(node_id, transports)| {
            // yeah, not quite simple yet - such a type argument mess
            Node::new::<_, _, MemStorage>(
                format!("node_{}", node_id),
                Default::default(),
                Default::default(),
                transports,
            )
        })
        .collect();

    // Put 5 key-value pairs.
    let mut handles = Vec::new();
    for i in 0..5 {
        let machine = nodes[0].machine().clone();
        handles.push(runtime::spawn(async move {
            let content = format!("hello, world {}", i);
            log::info!("Inserting [{}, {}]...", i, content);
            let result = await!(machine.put(i, content.clone()));
            log::info!("[{}, {}] has been inserted", i, content);
            result
        }));
    }

    for handle in handles.drain(..) {
        await!(handle).unwrap();
    }

    log::info!("State changes were successful");

    for node in nodes.iter() {
        let name = node.name().clone();
        let handles: Vec<_> = (0..5)
            .map(|i| {
                let machine = node.machine().clone();
                runtime::spawn(async move {
                    (i, await!(machine.get(i)).unwrap())
                })
            }).collect();
        log::info!("{} {{", name);
        for handle in handles {
            let (key, value) = await!(handle);
            log::info!("  {}: {}", key, value);
        }
        log::info!("}}");
    }
}
