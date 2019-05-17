from debian

add . /app
workdir /app

shell ["/bin/bash", "-c"]

run apt-get update && apt-get install curl gcc musl-tools -y
run curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- \
  --default-toolchain nightly --default-host x86_64-unknown-linux-gnu -y
run source /root/.cargo/env && \
  rustup target add x86_64-unknown-linux-musl && \
  cargo build --example helm --release --target x86_64-unknown-linux-musl

from scratch
copy --from=0 /app/target/x86_64-unknown-linux-musl/release/examples/helm .

entrypoint ["/helm"]
