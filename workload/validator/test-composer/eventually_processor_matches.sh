#!/bin/bash
# Produce records to Kafka topics using atleast_once (avoids slow exactly_once transactional init)
curl -m 30 -X POST producer:3000/atleast_once_single/bankEO/100 -H "Content-Type: application/json"
curl -m 30 -X POST producer:3000/atleast_once_single/bankAO/100 -H "Content-Type: application/json"
# Process records from Kafka — use a large limit to drain any backlog from parallel drivers.
# The processor will stop early once no more messages are available (10s receive timeout).
curl -m 300 -X POST processor:3000/atleast_once_processor/bankEO/10000 -H "Content-Type: application/json"
curl -m 300 -X POST processor:3000/atleast_once_processor/bankAO/10000 -H "Content-Type: application/json"
RUST_LOG=info /validator/target/debug/validator --test processor-matches
