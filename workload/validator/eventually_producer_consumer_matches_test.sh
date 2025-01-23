#!/bin/bash
curl -m 5 -X POST consumer:3000/pass_through_consumer/bank/10000 -H "Content-Type: application/json"
RUST_LOG=debug /validator/target/debug/validator --test exactly-once-guarantee
