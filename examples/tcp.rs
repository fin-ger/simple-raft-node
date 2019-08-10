#![feature(async_await)]

use simple_raft_node::transports::TcpConnectionManager;
use simple_raft_node::machines::HashMapMachine;
use simple_raft_node::storages::MemStorage;
use simple_raft_node::{Node, Config};

use std::net::SocketAddr;

#[runtime::main]
async fn main() {
    env_logger::init();

    log::info!("Spawning nodes");

    let nodes: Vec<Node<HashMapMachine<i64, String>>> = (1..6)
        .map(|node_id| {
            let gateway = "127.0.0.1:8081".parse::<SocketAddr>().unwrap();
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
            Node::new(
                config,
                gateway,
                machine,
                storage,
                mgr,
            )
        })
        .collect();

    std::thread::sleep(std::time::Duration::from_secs(7));

    let count = 10;

    let mut handles = Vec::new();
    for i in 0..count {
        let machine = nodes[0].machine().clone();
        handles.push(runtime::spawn(async move {
            let content = format!("hello, world {}", i);
            log::info!("Inserting [{}, {}]...", i, content);
            let result = machine.put(i, content.clone()).await;
            log::info!("[{}, {}] has been inserted", i, content);
            result
        }));
    }

    for handle in handles.drain(..) {
        handle.await.unwrap();
    }

    log::info!("State changes were successful");
    std::thread::sleep(std::time::Duration::from_secs(1));

    for node in nodes.iter() {
        let id = node.id();
        let handles: Vec<_> = (0..count)
            .map(|i| {
                let machine = node.machine().clone();
                runtime::spawn(async move {
                    (i, machine.get(i).await.ok())
                })
            }).collect();
        log::info!("node {} {{", id);
        for handle in handles {
            let (key, value) = handle.await;
            log::info!("  {}: {:?}", key, value);
        }
        log::info!("}}");
    }
}
