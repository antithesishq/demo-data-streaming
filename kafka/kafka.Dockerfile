FROM docker.io/apache/kafka:latest AS builder
FROM mcr.microsoft.com/openjdk/jdk:21-ubuntu
RUN apt update
RUN apt install -y build-essential gdb strace libc6 libc6-dev libc6-dbg glibc-doc manpages manpages-dev
COPY --chown=root:root --from=builder /opt/kafka /opt/kafka
COPY --chown=root:root --from=builder /etc/kafka/docker /etc/kafka/docker 
CMD ["/etc/kafka/docker/run"]