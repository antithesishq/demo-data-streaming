FROM docker.io/rust:latest
COPY /processor /processor
WORKDIR /processor
RUN apt update && apt install -y cmake
RUN cargo build
