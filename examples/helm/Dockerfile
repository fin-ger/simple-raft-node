from clux/muslrust:nightly

add . /volume
workdir /volume

run cargo build --example helm --release --target x86_64-unknown-linux-musl

from scratch
copy --from=0 /volume/target/x86_64-unknown-linux-musl/release/examples/helm .

entrypoint ["/helm"]
