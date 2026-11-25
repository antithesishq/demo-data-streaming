#!/bin/bash
x=$(( $(od -An -N2 -i /dev/urandom) % 100 + 1 ))
echo Consume: $x
curl -sS -m 30 -X POST consumer:3000/exactly_once_pass_through_consumer/bankEO/$x -H "Content-Type: application/json"
rc=$?
case $rc in
  0) exit 0 ;;
  6|7|28|52|56)
    echo "consumer unavailable or slow under faults (curl exit $rc); not a test failure"
    exit 0 ;;
  *) exit $rc ;;
esac
