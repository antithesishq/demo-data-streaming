FROM docker.io/rust:latest
COPY /consumer /consumer
WORKDIR /consumer
RUN apt update && apt install -y cmake
RUN cargo build
