FROM docker.io/python:3.13.0-bookworm

RUN pip install antithesis kafka-python requests flask
RUN apt-get -y update && apt-get -y install curl nano bash

COPY ./producer.py /root/producer.py 
WORKDIR /root

# ENTRYPOINT ["sleep", "infinity"]
ENTRYPOINT ["flask", "--app", "producer", "run", "--host=0.0.0.0"]
