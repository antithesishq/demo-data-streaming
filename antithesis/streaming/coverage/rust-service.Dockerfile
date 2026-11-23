# Local loop only: the producer or consumer built with LLVM source coverage
# instead of Antithesis instrumentation. Same binary path as the real build.
FROM rust:1-trixie
ARG SVC
RUN apt-get update && apt-get install -y cmake binutils && rustup component add llvm-tools-preview
COPY /${SVC} /${SVC}
WORKDIR /${SVC}
RUN RUSTFLAGS="--cfg tokio_unstable -Cinstrument-coverage -Cllvm-args=-runtime-counter-relocation" \
    cargo build --release \
      --config 'build.target = "x86_64-unknown-linux-gnu"' \
      --config 'profile.release.debug = true'
