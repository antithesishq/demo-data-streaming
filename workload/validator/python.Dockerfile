FROM docker.io/rust:latest

RUN apt update && apt install -y cmake pip
RUN pip install antithesis requests --break-system-packages
COPY ./eventually_producer_consumer_matches_test.sh /opt/antithesis/test/v1/demo/eventually_producer_consumer_matches_test.sh
COPY ./parallel_driver_consumer_passthrough.py /opt/antithesis/test/v1/demo/parallel_driver_consumer_passthrough.py
COPY ./parallel_driver_consumption_order_test.sh /opt/antithesis/test/v1/demo/parallel_driver_consumption_order_test.sh
COPY ./parallel_driver_exactly_once_guarantee_test.sh /opt/antithesis/test/v1/demo/parallel_driver_exactly_once_guarantee_test.sh
COPY ./parallel_driver_produce_atleast_once_batch.py /opt/antithesis/test/v1/demo/parallel_driver_produce_atleast_once_batch.py
COPY ./parallel_driver_produce_atleast_once_single.py /opt/antithesis/test/v1/demo/parallel_driver_produce_atleast_once_single.py
COPY ./parallel_driver_produce_exactly_once_batch.py /opt/antithesis/test/v1/demo/parallel_driver_produce_exactly_once_batch.py
COPY ./parallel_driver_produce_exactly_once_single.py /opt/antithesis/test/v1/demo/parallel_driver_produce_exactly_once_single.py
RUN chmod -R 777 /opt/antithesis/test/v1/demo

COPY ../validator /validator
WORKDIR /validator
RUN cargo build