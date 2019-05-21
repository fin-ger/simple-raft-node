#![feature(async_await, await_macro, gen_future, decl_macro, proc_macro_hygiene)]

use simple_raft_node::transports::TcpConnectionManager;
use simple_raft_node::machines::HashMapMachine;
use simple_raft_node::storages::MemStorage;
use simple_raft_node::{Node, Config};

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::env;
use std::io::Read;
use std::net::ToSocketAddrs;

use rocket::{State, Data, routes, get, put, delete};
use regex::Regex;
use futures::executor;

#[get("/")]
fn index_handler(node_id: State<u64>, machine: State<HashMapMachine<String, String>>) -> String {
    format!(
        "Response from node {}

Using the API:
HTTP GET    /<key>\t get the content of <key>
HTTP PUT    /<key>\t put content of request into <key>
HTTP DELETE /<key>\t delete content of <key>

HashMap contents:
{{
{}
}}
",
        *node_id,
        executor::block_on(machine.entries())
            .unwrap()
            .iter()
            .map(|(k, v)| format!("  \"{}\": \"{}\"", k, v))
            .collect::<Vec<_>>()
            .join(",\n"),
    )
}

#[get("/<key>")]
fn get_handler(machine: State<HashMapMachine<String, String>>, key: String) -> String {
    format!("{:?}\n", executor::block_on(machine.get(key)).ok())
}

#[put("/<key>", data = "<value>")]
fn put_handler(machine: State<HashMapMachine<String, String>>, key: String, value: Data) -> String {
    let mut s = String::new();
    value.open().read_to_string(&mut s).unwrap();
    match executor::block_on(machine.put(key, s)) {
        Ok(_) => "Success\n",
        Err(_) => "Failure\n",
    }.into()
}

#[delete("/<key>")]
fn delete_handler(machine: State<HashMapMachine<String, String>>, key: String) -> String {
    match executor::block_on(machine.delete(key)) {
        Ok(_) => "Success\n",
        Err(_) => "Failure\n",
    }.into()
}

#[runtime::main]
async fn main() {
    env_logger::init();

    log::info!("Spawning nodes");

    let node_id_msg = "Please specify a NODE_ID >= 0 via an environment variable!";
    let re = Regex::new(r"\d+").unwrap();
    let node_id_str = env::var("NODE_ID").expect(node_id_msg);
    let mut node_id = re.find(node_id_str.as_str())
        .expect(node_id_msg)
        .as_str()
        .parse::<u64>()
        .expect(node_id_msg);

    node_id += 1;

    let node_address = env::var("NODE_ADDRESS").ok()
        .map(|address| {
            loop {
                log::info!("Trying to resolve binding IP address {}...", address);
                match address.to_socket_addrs() {
                    Ok(mut addr) => {
                        return addr
                            .next()
                            .expect("The binding address does not resolve to a valid IP or port!");
                    },
                    Err(e) => {
                        log::warn!("Could not resolve binding address {}: {}", address, e);
                    },
                }
            }
        }).expect("Please specify a NODE_ADDRESS (domain:port) via an environment variable!");
    let gateway = env::var("NODE_GATEWAY").ok()
        .map(|gateway| {
            loop {
                log::info!("Trying to resolve gateway address {}...", gateway);
                match gateway.to_socket_addrs() {
                    Ok(mut addr) => {
                        return addr
                            .next()
                            .expect("The gateway address does not resolve to a valid IP or port!");
                    },
                    Err(e) => {
                        log::warn!("Could not resolve gateway {}: {}", gateway, e);
                    },
                }
            }
        }).expect("The gateway address environment variable NODE_GATEWAY is not specified!");

    let config = Config {
        id: node_id,
        tag: format!("node_{}", node_id),
        election_tick: 10,
        heartbeat_tick: 3,
        ..Default::default()
    };
    let machine = HashMapMachine::<String, String>::new();
    let storage = MemStorage::new();
    let mgr = TcpConnectionManager::new(node_address).unwrap();
    let node = Node::new(
        config,
        gateway,
        machine,
        storage,
        mgr,
    );

    let is_running = Arc::new(Mutex::new(true));
    let handle = {
        let is_running = is_running.clone();
        let machine = node.machine().clone();
        std::thread::spawn(move || {
            let mut t = Instant::now();
            loop {
                if t.elapsed() >= Duration::from_secs(10) {
                    if let Some(entries) = executor::block_on(machine.entries()).ok() {
                        log::info!("node {} {{", node_id);
                        for (key, value) in entries {
                            log::info!("  {}: {:?}", key, value);
                        }
                        log::info!("}}");
                    }
                    t = Instant::now();
                }

                if !*is_running.lock().unwrap() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        })
    };

    rocket::ignite()
        .mount("/", routes![
            index_handler, get_handler, put_handler, delete_handler
        ])
        .manage(node.machine().clone())
        .manage(node_id)
        .launch();

    handle.join().expect("hash-map printer thread couldn't join");
}
