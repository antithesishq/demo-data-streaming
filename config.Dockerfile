FROM scratch
COPY ./docker-compose.yaml /docker-compose.yaml
COPY ./git.diff /git.diff
COPY ./targeted_coverage.json /targeted_coverage.json