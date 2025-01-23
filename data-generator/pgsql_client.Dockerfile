# podman build -f pgsql_client.Dockerfile -t pgsql_client:latest .
FROM docker.io/python:3.13.0-bookworm

RUN apt-get -y update && \
apt-get -y install curl nano bash

RUN wget https://bootstrap.pypa.io/get-pip.py
RUN python3 get-pip.py

RUN pip install requests psycopg2

WORKDIR /root

COPY ./pgsql_ep.sh /root/entrypoint.sh
COPY ./pgsql_client.py /root/pgsql_client.py

ENTRYPOINT ["bash", "/root/entrypoint.sh"]