FROM docker.io/rust:latest
COPY ../validator /validator
WORKDIR /validator
RUN apt update && apt install -y cmake
RUN cargo build
COPY ./eventually_producer_consumer_matches_test.sh /opt/antithesis/test/v1/demo/eventually_producer_consumer_matches_test.sh
COPY ./parallel_driver_consumer_passthrough.sh /opt/antithesis/test/v1/demo/parallel_driver_consumer_passthrough.sh
COPY ./parallel_driver_consumption_order_test.sh /opt/antithesis/test/v1/demo/parallel_driver_consumption_order_test.sh
COPY ./parallel_driver_exactly_once_guarantee_test.sh /opt/antithesis/test/v1/demo/parallel_driver_exactly_once_guarantee_test.sh
COPY ./parallel_driver_produce_atleast_once_batch.sh /opt/antithesis/test/v1/demo/parallel_driver_produce_atleast_once_batch.sh
COPY ./parallel_driver_produce_atleast_once_single.sh /opt/antithesis/test/v1/demo/parallel_driver_produce_atleast_once_single.sh
COPY ./parallel_driver_produce_exactly_once_batch.sh /opt/antithesis/test/v1/demo/parallel_driver_produce_exactly_once_batch.sh
COPY ./parallel_driver_produce_exactly_once_single.sh /opt/antithesis/test/v1/demo/parallel_driver_produce_exactly_once_single.sh
