ARG GITHUB_TOKEN
ARG BRANCH
FROM alpine:latest AS builder
RUN apk add --no-cache curl jq
RUN curl -s -H "Authorization: Bearer ${GITHUB_TOKEN}" "https://api.github.com/repos/antithesishq/demo-data-streaming/compare/main...${BRANCH}" > /git.diff

FROM scratch
COPY ./docker-compose.yaml /docker-compose.yaml
COPY --from=builder /git.diff /git.diff