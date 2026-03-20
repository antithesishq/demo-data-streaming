#!/bin/bash
# Produce records to Kafka topics using atleast_once (avoids slow exactly_once transactional init)
curl -m 30 -X POST producer:3000/atleast_once_single/bankEO/100 -H "Content-Type: application/json"
curl -m 30 -X POST producer:3000/atleast_once_single/bankAO/100 -H "Content-Type: application/json"
# Process records from Kafka — num_records must match what was produced above
curl -m 60 -X POST processor:3000/atleast_once_processor/bankEO/100 -H "Content-Type: application/json"
curl -m 60 -X POST processor:3000/atleast_once_processor/bankAO/100 -H "Content-Type: application/json"
RUST_LOG=info /validator/target/debug/validator --test processor-matches
