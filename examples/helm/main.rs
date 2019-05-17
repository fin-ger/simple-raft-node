#![feature(async_await, await_macro)]

use simple_raft_node::transports::TcpConnectionManager;
use simple_raft_node::machines::HashMapMachine;
use simple_raft_node::storages::MemStorage;
use simple_raft_node::{Node, Config};

use std::env;
use std::net::SocketAddr;

#[runtime::main]
async fn main() {
    env_logger::init();

    log::info!("Spawning nodes");

    let node_id = env::var("NODE_ID")
        .expect("Please specify a node id > 0!")
        .parse::<u64>()
        .expect("Please specify a node id > 0!");

    if node_id <= 0 {
        panic!("Please specify a node id > 0!");
    }

    let gateway = if node_id != 1 {
        Some("127.0.0.1:8081".parse::<SocketAddr>().unwrap())
    } else {
        None
    };
    let address = format!("127.0.0.1:{}", 8080 + node_id)
        .parse::<SocketAddr>()
        .unwrap();
    let config = Config {
        id: node_id,
        tag: format!("node_{}", node_id),
        election_tick: 10,
        heartbeat_tick: 3,
        ..Default::default()
    };
    let machine = HashMapMachine::new();
    let storage = MemStorage::new();
    let mgr = TcpConnectionManager::new(address).unwrap();
    let node = Node::new(
        config,
        gateway,
        machine,
        storage,
        mgr,
    );

    std::thread::sleep(std::time::Duration::from_secs(7));

    let count = 10;

    // we need at least 3 nodes, so for general hacking this is sufficient ;D
    if node_id == 3 {
        let mut handles = Vec::new();
        for i in 0..count {
            let machine = node.machine().clone();
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
    }

    // wait until state-changes are done
    std::thread::sleep(std::time::Duration::from_secs(1));

    let id = node.id();
    let handles: Vec<_> = (0..count)
        .map(|i| {
            let machine = node.machine().clone();
            runtime::spawn(async move {
                (i, await!(machine.get(i)).ok())
            })
        }).collect();
    log::info!("node {} {{", id);
    for handle in handles {
        let (key, value) = await!(handle);
        log::info!("  {}: {:?}", key, value);
    }
    log::info!("}}");
}
