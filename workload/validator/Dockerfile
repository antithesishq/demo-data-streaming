FROM docker.io/rust:latest
COPY /validator /validator
WORKDIR /validator
RUN apt update && apt install -y cmake python3-pip
RUN pip install boto3 antithesis --break-system-packages
RUN cargo build
COPY ./validator/eventually_producer_consumer_matches_test.sh /opt/antithesis/test/v1/demo/eventually_producer_consumer_matches_test.sh
COPY ./validator/parallel_driver_exactly_once_passthrough_consumer.sh /opt/antithesis/test/v1/demo/parallel_driver_exactly_once_passthrough_consumer.sh
COPY ./validator/parallel_driver_atleast_once_passthrough_consumer.sh /opt/antithesis/test/v1/demo/parallel_driver_atleast_once_passthrough_consumer.sh
COPY ./validator/parallel_driver_consumption_order_test.sh /opt/antithesis/test/v1/demo/parallel_driver_consumption_order_test.sh
COPY ./validator/parallel_driver_exactly_once_guarantee_test.sh /opt/antithesis/test/v1/demo/parallel_driver_exactly_once_guarantee_test.sh
COPY ./validator/parallel_driver_produce_atleast_once_batch.sh /opt/antithesis/test/v1/demo/parallel_driver_produce_atleast_once_batch.sh
COPY ./validator/parallel_driver_produce_atleast_once_single.sh /opt/antithesis/test/v1/demo/parallel_driver_produce_atleast_once_single.sh
COPY ./validator/parallel_driver_produce_exactly_once_batch.sh /opt/antithesis/test/v1/demo/parallel_driver_produce_exactly_once_batch.sh
COPY ./validator/parallel_driver_produce_exactly_once_single.sh /opt/antithesis/test/v1/demo/parallel_driver_produce_exactly_once_single.sh
COPY ./validator/anytime_validate_balance.py /opt/antithesis/test/v1/demo/anytime_validate_balance.py
COPY ./validator/eventually_ibans_match.py /opt/antithesis/test/v1/demo/eventually_ibans_match.py

COPY ./entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
