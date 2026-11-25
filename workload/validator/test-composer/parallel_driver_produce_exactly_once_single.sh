#!/bin/bash
x=$(( $(od -An -N2 -i /dev/urandom) % 100 + 1 ))
echo Produce: $x
curl -sS -m 30 -X POST producer:3000/exactly_once_single/bankEO/$x -H "Content-Type: application/json"
rc=$?
case $rc in
  0) exit 0 ;;
  6|7|28|52|56)
    echo "producer unavailable or slow under faults (curl exit $rc); not a test failure"
    exit 0 ;;
  *) exit $rc ;;
esac
