#!/bin/bash
x=$(( $(od -An -N2 -i /dev/urandom) % 100 + 1 ))
echo Produce: $x
curl -m 30 -X POST producer:3000/atleast_once_batch/bankAO/$x -H "Content-Type: application/json"
