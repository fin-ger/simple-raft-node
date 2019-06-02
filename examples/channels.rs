#![feature(async_await, await_macro)]

use simple_raft_node::transports::MpscChannelConnectionManager;
use simple_raft_node::machines::HashMapMachine;
use simple_raft_node::storages::MemStorage;
use simple_raft_node::{Node, Config, ConnectionManager};

#[runtime::main]
async fn main() {
    env_logger::init();

    log::info!("Spawning nodes");

    let mut mgrs = MpscChannelConnectionManager::new_managers(vec![1, 2, 3, 4, 5]);
    let nodes: Vec<Node<HashMapMachine<i64, String>>> = mgrs.drain(..)
        .map(|mgr| {
            let config = Config {
                id: mgr.listener_addr(),
                tag: format!("node_{}", mgr.listener_addr()),
                election_tick: 10,
                heartbeat_tick: 3,
                ..Default::default()
            };
            let gateway = 1;
            let machine = HashMapMachine::new();
            let storage = MemStorage::new();
            Node::new(
                config,
                gateway,
                machine,
                storage,
                mgr,
            )
        })
        .collect();
    std::thread::sleep(std::time::Duration::from_secs(10));

    let count = 5;

    let mut handles = Vec::new();
    for i in 0..count {
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
}
