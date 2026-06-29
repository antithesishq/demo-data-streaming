#!/bin/bash

set -euo pipefail

curl --fail \
    -X POST \
    --config - \
    -d "{\"params\": {
        \"antithesis.description\": \"$DESCRIPTION\",
        \"custom.duration\": \"$DURATION\",
        \"antithesis.config_image\": \"demo-data-streaming-config:latest\",
        \"antithesis.report.recipients\": \"$EMAIL_RECIPIENTS\"
    }}" \
    https://rose-cheetah.antithesis.com/api/v1/launch/async_platform <<EOF
user = "$USERNAME:$PASSWORD"
EOF
