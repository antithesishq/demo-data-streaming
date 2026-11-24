#!/bin/bash
# Make sure no fault lands during the drain check (one did in run e5957b51...,
# although an eventually_ command should already stop faults).
[ -n "${ANTITHESIS_STOP_FAULTS}" ] && "${ANTITHESIS_STOP_FAULTS}" 3600
# Faults have just stopped, so a consumer that was killed or stopped may still
# be restarting. Wait for it to answer (any HTTP status) before asking it to
# consume, or the requests below fail and nothing is consumed during the check.
for _ in $(seq 1 60); do
  curl -s -o /dev/null -m 2 consumer:3000/ && break
  sleep 2
done
# curl -m 30 -X POST consumer:3000/pass_through_consumer/bank/10000 -H "Content-Type: application/json"
curl -m 30 -X POST consumer:3000/exactly_once_pass_through_consumer/bankEO/10000 -H "Content-Type: application/json"
curl -m 30 -X POST consumer:3000/atleast_once_pass_through_consumer/bankAO/10000 -H "Content-Type: application/json"
RUST_LOG=debug /validator/target/debug/validator --test producer-consumer-matches
