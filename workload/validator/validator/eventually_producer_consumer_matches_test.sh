#!/bin/bash
# curl -m 30 -X POST consumer:3000/pass_through_consumer/bank/10000 -H "Content-Type: application/json"
curl -m 30 -X POST consumer:3000/exactly_once_pass_through_consumer/bankEO/10000 -H "Content-Type: application/json"
curl -m 30 -X POST consumer:3000/atleast_once_pass_through_consumer/bankAO/10000 -H "Content-Type: application/json"
RUST_LOG=debug /validator/target/debug/validator --test producer-consumer-matches
