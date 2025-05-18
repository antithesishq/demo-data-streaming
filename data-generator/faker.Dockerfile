# podman build -t data_generator:latest -f faker.Dockerfile .
# flask --app base run --host=0.0.0.0
FROM docker.io/python:3.13.0-bookworm

RUN apt-get -y update && \
apt-get -y install curl nano bash

RUN wget https://bootstrap.pypa.io/get-pip.py
RUN python3 get-pip.py

RUN pip install faker flask requests

COPY ./base.py /root/base.py 
COPY ./parallel_driver_transaction.py /opt/antithesis/test/v1/txn/parallel_driver_transaction.py

WORKDIR /root

ENTRYPOINT ["flask", "--app", "base", "run", "--host=0.0.0.0"]
# ENTRYPOINT ["sleep", "infinity"]