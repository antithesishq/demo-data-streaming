#!/bin/bash
# Driver: ask the consumer to handle a random number of records.
#
# The consumer and Kafka are faulted, so a request can time out or have its
# connection cut. That's expected under faults, not a test failure: the checks
# (anytime_/eventually_/parallel_driver_*_test) judge what the system did.
# Transport errors therefore exit 0; any other curl error still fails.
x=$(( $(od -An -N2 -i /dev/urandom) % 100 + 1 ))
echo Consume: $x
curl -sS -m 30 -X POST consumer:3000/atleast_once_pass_through_consumer/bankAO/$x -H "Content-Type: application/json"
rc=$?
case $rc in
  0) exit 0 ;;
  6|7|28|52|56)  # couldn't resolve, couldn't connect, timed out, empty reply, recv failure
    echo "consumer unavailable or slow under faults (curl exit $rc); not a test failure"
    exit 0 ;;
  *) exit $rc ;;
esac
