#!/bin/bash
# Report setup complete only once the system is ready for test commands:
# the state tracker has its schema, the producer and consumer answer HTTP,
# and DynamoDB answers. Antithesis runs test commands as soon as setup is
# complete, so reporting it earlier makes them fail on a half-started system.

ANTITHESIS_OUTPUT_DIR=${ANTITHESIS_OUTPUT_DIR:-"/tmp"}

until python3 -c '
import psycopg2
conn = psycopg2.connect(host="state-tracker", user="u", password="p", database="d")
cur = conn.cursor()
cur.execute("SELECT 1 FROM producer LIMIT 1")
' 2>/dev/null; do
  echo "waiting for the state-tracker schema"
  sleep 1
done

for service in producer consumer; do
  # Any HTTP response, even a 404, means the server is up.
  until curl -s -o /dev/null -m 2 "http://$service:3000/"; do
    echo "waiting for $service"
    sleep 1
  done
done

until curl -s -o /dev/null -m 2 "http://ddb:8000/"; do
  echo "waiting for ddb"
  sleep 1
done

echo "system ready, reporting setup complete"
echo '{"antithesis_setup": { "status": "complete", "details": null }}' > $ANTITHESIS_OUTPUT_DIR/sdk.jsonl

sleep infinity
