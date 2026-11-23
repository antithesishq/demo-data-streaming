# Antithesis coverage instrumentation, per
# https://antithesis.com/docs/reference/sdk/rust/instrumentation/
FROM docker.io/rust:1-trixie
RUN apt-get update && apt-get install -y cmake binutils
RUN cargo install --locked tokio-console
COPY /producer /producer
WORKDIR /producer
RUN cargo build --release \
      --config 'build.target = "x86_64-unknown-linux-gnu"' \
      --config 'profile.release.debug = true' \
      --config 'target.x86_64-unknown-linux-gnu.rustflags = [ \
        "--cfg", "tokio_unstable", \
        "-Ccodegen-units=1", \
        "-Cpasses=sancov-module", \
        "-Cllvm-args=-sanitizer-coverage-level=3", \
        "-Cllvm-args=-sanitizer-coverage-trace-pc-guard", \
        "-Cllvm-args=-sanitizer-coverage-pc-table", \
        "-Clink-args=-Wl,--build-id", \
      ]' \
 && mkdir -p /symbols \
 && cp target/x86_64-unknown-linux-gnu/release/producer /symbols/producer

# Fail the build if the binary isn't instrumented or has no debug info.
RUN nm /symbols/producer | grep -q ' T antithesis_load_libvoidstar' \
 && readelf -S /symbols/producer | grep -q __sancov_guards \
 && readelf -S /symbols/producer | grep -q __sancov_pcs \
 && [ "$(readelf -S /symbols/producer | grep -c debug_info)" -gt 0 ]

