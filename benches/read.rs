#![feature(async_await, await_macro, test)]

extern crate test;

use simple_raft_node::transports::MpscChannelTransport;
use simple_raft_node::machines::HashMapMachine;
use simple_raft_node::storages::MemStorage;
use simple_raft_node::{Node};

use serde::{Serialize, Deserialize};
use test::Bencher;

// One kilobyte in size
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct Value {
    content1: [i64; 32],
    content2: [i64; 32],
    content3: [i64; 32],
    content4: [i64; 32],
}

#[bench]
fn read(b: &mut Bencher) {
    let _ = env_logger::builder().is_test(true).try_init();
    let nodes: Vec<Node<HashMapMachine<i64, Value>>> = MpscChannelTransport::create_transports(
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

    let machine = nodes[0].machine().clone();
    let _ = runtime::raw::enter(runtime::native::Native, async move {
        // Put 5 key-value pairs.
        let mut handles = Vec::new();
        for i in 0..1000 {
            let machine = machine.clone();
            handles.push(runtime::spawn(async move {
                await!(machine.put(i, Default::default()))
            }));
        }

        for handle in handles.drain(..) {
            await!(handle).unwrap();
        }
    });

    b.iter(move || {
        let machine = nodes[0].machine().clone();
        let _ = runtime::raw::enter(runtime::native::Native, async move {
            let handles: Vec<_> = (0..1000)
                .map(|i| {
                    let machine = machine.clone();
                    runtime::spawn(async move {
                        (i, await!(machine.get(i)).unwrap())
                    })
                }).collect();
            for handle in handles {
                let (_key, _value) = await!(handle);
            }
        });
    });
}
