# Antithesis coverage instrumentation, per
# https://antithesis.com/docs/reference/sdk/rust/instrumentation/
FROM docker.io/rust:1-trixie
RUN apt-get update && apt-get install -y cmake binutils

COPY /consumer /consumer
WORKDIR /consumer
RUN cargo build --release \
      --config 'build.target = "x86_64-unknown-linux-gnu"' \
      --config 'profile.release.debug = true' \
      --config 'target.x86_64-unknown-linux-gnu.rustflags = [ \
        "-Ccodegen-units=1", \
        "-Cpasses=sancov-module", \
        "-Cllvm-args=-sanitizer-coverage-level=3", \
        "-Cllvm-args=-sanitizer-coverage-trace-pc-guard", \
        "-Cllvm-args=-sanitizer-coverage-pc-table", \
        "-Clink-args=-Wl,--build-id", \
      ]' \
 && mkdir -p /symbols \
 && cp target/x86_64-unknown-linux-gnu/release/consumer /symbols/consumer

# Fail the build if the binary isn't instrumented or has no debug info.
RUN nm /symbols/consumer | grep -q ' T antithesis_load_libvoidstar' \
 && readelf -S /symbols/consumer | grep -q __sancov_guards \
 && readelf -S /symbols/consumer | grep -q __sancov_pcs \
 && [ "$(readelf -S /symbols/consumer | grep -c debug_info)" -gt 0 ]

