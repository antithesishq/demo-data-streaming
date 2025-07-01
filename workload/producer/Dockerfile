FROM docker.io/rust:latest as builder
RUN apt update && apt install -y cmake
RUN cargo install --locked tokio-console
COPY /producer /producer
WORKDIR /producer
RUN RUSTFLAGS="--cfg tokio_unstable" cargo build

# FROM docker.io/rust:latest
# WORKDIR /app
# COPY --from=builder /producer/target/debug/producer /app/producer
# CMD ["./producer"]
