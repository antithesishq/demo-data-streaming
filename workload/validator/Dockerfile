FROM docker.io/rust:latest

# Download and install the Antithesis instrumentation library
RUN curl -o /usr/lib/libvoidstar.so https://antithesis.com/assets/instrumentation/libvoidstar.so \
    && ldconfig

COPY /validator /validator
WORKDIR /validator
RUN apt update && apt install -y cmake python3-pip
RUN pip install boto3 antithesis psycopg2 --break-system-packages

# Build with Antithesis instrumentation flags
RUN LD_LIBRARY_PATH=/usr/lib RUSTFLAGS=" \
    -Ccodegen-units=1 \
    -Cpasses=sancov-module \
    -Cllvm-args=-sanitizer-coverage-level=3 \
    -Cllvm-args=-sanitizer-coverage-trace-pc-guard \
    -Clink-args=-Wl,--build-id \
    -L/usr/lib \
    -lvoidstar" \
    cargo build

# Validate instrumentation (optional, remove for production)
RUN ldd target/debug/validator | grep "libvoidstar" \
    && nm target/debug/validator | grep "sanitizer_cov_trace_pc_guard"

# Copy debug symbols for Antithesis symbolization
RUN mkdir -p /symbols && cp target/debug/validator /symbols/

COPY --chmod=777 ./test-composer /opt/antithesis/test/v1/demo/

COPY ./entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]