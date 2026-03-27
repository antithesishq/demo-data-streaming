FROM docker.io/rust:latest as builder
RUN apt update && apt install -y cmake
RUN cargo install --locked tokio-console

# Download and install the Antithesis instrumentation library
RUN curl -o /usr/lib/libvoidstar.so https://antithesis.com/assets/instrumentation/libvoidstar.so \
    && ldconfig

COPY /producer /producer
WORKDIR /producer

# Build with both tokio_unstable and Antithesis instrumentation flags
RUN LD_LIBRARY_PATH=/usr/lib RUSTFLAGS=" \
    --cfg tokio_unstable \
    -Ccodegen-units=1 \
    -Cpasses=sancov-module \
    -Cllvm-args=-sanitizer-coverage-level=3 \
    -Cllvm-args=-sanitizer-coverage-trace-pc-guard \
    -Clink-args=-Wl,--build-id \
    -L/usr/lib \
    -lvoidstar" \
    cargo build

# Validate instrumentation (optional, remove for production)
RUN ldd target/debug/producer | grep "libvoidstar" \
    && nm target/debug/producer | grep "sanitizer_cov_trace_pc_guard"

# Copy debug symbols for Antithesis symbolization
RUN mkdir -p /symbols && cp target/debug/producer /symbols/