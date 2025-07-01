#!/bin/bash
x=$(( $(od -An -N2 -i /dev/urandom) % 100 + 1 ))
echo Consume: $x
curl -m 30 -X POST consumer:3000/atleast_once_pass_through_consumer/bankAO/$x -H "Content-Type: application/json"
