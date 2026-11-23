# Antithesis driver for the data-streaming demo. Built with Antithesis
# coverage instrumentation, per
# https://antithesis.com/docs/reference/sdk/rust/instrumentation/
FROM rust:1-trixie AS build
WORKDIR /src
COPY driver/Cargo.toml driver/Cargo.lock ./
COPY driver/src ./src
RUN cargo build --release --locked \
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
 && mkdir -p /out /symbols \
 && cp target/x86_64-unknown-linux-gnu/release/driver /out/ \
 && cp /out/driver /symbols/
# Fail the build if the binary isn't instrumented or has no debug info.
RUN nm /symbols/driver | grep -q ' T antithesis_load_libvoidstar' \
 && readelf -S /symbols/driver | grep -q __sancov_guards \
 && readelf -S /symbols/driver | grep -q __sancov_pcs \
 && [ "$(readelf -S /symbols/driver | grep -c debug_info)" -gt 0 ]

FROM debian:trixie-slim
# Debug tools for `snouty runs exec`: curl for the producer, consumer and
# DynamoDB APIs, psql for the state-tracker, jq for their output.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl jq postgresql-client netcat-openbsd \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/driver /usr/local/bin/
COPY --from=build /symbols /symbols
COPY driver/first_start_workload /opt/antithesis/test/v1/main/
RUN chmod 755 /opt/antithesis/test/v1/main/first_start_workload
CMD ["/usr/local/bin/driver"]
