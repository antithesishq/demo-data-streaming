FROM ubuntu:latest
RUN apt update && apt install -y curl less unzip jq
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip && ./aws/install
RUN aws configure set aws_access_key_id dummy
RUN aws configure set aws_secret_access_key dummy
RUN aws configure set region us-east-1
RUN aws configure set output json
ENTRYPOINT ["sleep infinity"]
