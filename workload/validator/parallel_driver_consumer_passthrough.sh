#!/bin/bash
x=$(( $(od -An -N2 -i /dev/urandom) % 100 + 1 ))
echo Consume: $x
curl -m 5 -X POST consumer:3000/pass_through_consumer/bank/$x -H "Content-Type: application/json"