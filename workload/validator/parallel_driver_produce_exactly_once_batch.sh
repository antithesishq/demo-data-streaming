#!/bin/bash
x=$(( $(od -An -N2 -i /dev/urandom) % 100 + 1 ))
echo Produce: $x
curl -m 5 -X POST producer:3000/exactly_once_batch/bank/$x -H "Content-Type: application/json"
