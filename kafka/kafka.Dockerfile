FROM docker.io/apache/kafka:4.3.1 AS builder
FROM mcr.microsoft.com/openjdk/jdk:21-ubuntu
RUN apt update
RUN apt install -y build-essential gdb strace libc6 libc6-dev libc6-dbg glibc-doc manpages manpages-dev curl
COPY --chown=root:root --from=builder /opt/kafka /opt/kafka
COPY --chown=root:root --from=builder /etc/kafka/docker /etc/kafka/docker

# Antithesis coverage instrumentation, per
# https://antithesis.com/docs/reference/sdk/java/how_to_build_with_sdk/
# Move the broker's own jars into the catalog directory so Antithesis instruments
# them, and leave third-party libraries in /opt/kafka/libs uninstrumented.
# Antithesis injects its dependency jars next to the instrumented ones, so that
# directory goes on the classpath. kafka-run-class.sh puts $CLASSPATH first.
ARG ANTITHESIS_FFI_VERSION=1.8.0
RUN mkdir -p /opt/antithesis/catalog/kafka \
 && cd /opt/kafka/libs \
 && mv kafka_2.13-*.jar kafka-clients-*.jar kafka-server-*.jar kafka-raft-*.jar \
       kafka-metadata-*.jar kafka-storage-*.jar kafka-coordinator-common-*.jar \
       kafka-group-coordinator-*.jar kafka-transaction-coordinator-*.jar \
       kafka-share-coordinator-*.jar \
       /opt/antithesis/catalog/kafka/ \
 && curl -fsSLo /opt/kafka/libs/antithesis-ffi-${ANTITHESIS_FFI_VERSION}.jar \
      https://repo1.maven.org/maven2/com/antithesis/ffi/${ANTITHESIS_FFI_VERSION}/ffi-${ANTITHESIS_FFI_VERSION}.jar \
 && ls /opt/antithesis/catalog/kafka/kafka_2.13-*.jar /opt/antithesis/catalog/kafka/kafka-server-[0-9]*.jar
ENV CLASSPATH="/opt/antithesis/catalog/kafka/*"

CMD ["/etc/kafka/docker/run"]
