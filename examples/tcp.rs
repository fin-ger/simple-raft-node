#![feature(async_await, await_macro)]

use simple_raft_node::transports::TcpTransport;
use simple_raft_node::machines::HashMapMachine;
use simple_raft_node::storages::MemStorage;
use simple_raft_node::Node;

use std::net::{TcpListener, TcpStream, SocketAddr};

#[runtime::main]
async fn main() {
    env_logger::init();

    let node_id = std::env::var("SRN_NODE_ID")
        .expect("SRN_NODE_ID environment variable is not set");
    let address = std::env::var("SRN_ADDRESS")
        .expect("SRN_ADDRESS environment variable not set")
        .parse::<SocketAddr>()
        .expect("SRN_ADDRESS cannot be parsed as valid address");

    log::info!("Spawning node {}", node_id);
    let node = Node::new(
        format!("node_{}", node_id),
        Default::default(),
        Default::default(),
        Vec::new(),
    );
    let gateway = std::env::var("SRN_GATEWAY")
        .ok()
        .map(|s| s.parse::<SocketAddr>());

    if let Some(gateway) = gateway {
        let stream = TcpStream::connect(gateway)
            .expect("Cannot connect to SRN-gateway");
        node.add_node(stream);
    }

    let listener = TcpListener::bind(address)
        .expect("Could not create TcpListener");

    for conn in listener.incoming() {
        let stream = match conn {
            Ok(stream) => stream,
            Err(e) => {
                log::error!("connection failed: {}", e);
                continue;
            },
        };

        // TODO report other gateways
        // TODO when node got added connect to other nodes reported by gateway
        node.add_node(stream);
    }

    // Put 10000 key-value pairs.
    let mut handles = Vec::new();
    for i in 0..10000 {
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

    let name = node.name().clone();
    let handles: Vec<_> = (0..10000)
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
