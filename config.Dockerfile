FROM alpine:latest AS builder
ARG GITHUB_TOKEN
ARG BRANCH
RUN apk add --no-cache curl jq
RUN --mount=type=secret,id=GITHUB_TOKEN \
  curl -s -H "Authorization: Bearer $(cat /run/secrets/GITHUB_TOKEN)" \
  "https://api.github.com/repos/antithesishq/demo-data-streaming/compare/main...${BRANCH}" \
  > /git.diff

FROM scratch
COPY ./docker-compose.yaml /docker-compose.yaml
COPY --from=builder /git.diff /git.diff