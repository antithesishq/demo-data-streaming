#!/bin/bash
x=$(( $(od -An -N2 -i /dev/urandom) % 100 + 1 ))
echo Consume: $x
curl -m 30 -X POST consumer:3000/exactly_once_pass_through_consumer/bankEO/$x -H "Content-Type: application/json"
