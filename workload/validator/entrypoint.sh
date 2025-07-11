#!/bin/bash

ANTITHESIS_OUTPUT_DIR=${ANTITHESIS_OUTPUT_DIR:-"/tmp"}

echo '{"antithesis_setup": { "status": "complete", "details": null }}' > $ANTITHESIS_OUTPUT_DIR/sdk.jsonl

sleep infinity