#!/bin/bash
x=$(( $(od -An -N2 -i /dev/urandom) % 100 + 1 ))
echo Process: $x
curl -m 30 -X POST processor:3000/atleast_once_processor/bankAO/$x -H "Content-Type: application/json"
