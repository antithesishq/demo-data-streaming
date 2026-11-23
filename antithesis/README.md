# Hegel workload

A second Antithesis workload for the data-streaming demo, written with the
[Antithesis plugin](https://github.com/antithesishq/antithesis-plugin). It
tests the same system (Kafka, the producer and consumer services,
DynamoDB-local and the Postgres state-tracker) for the same properties as the
test-composer workload in the repository root, but with one
[hegel](https://hegel.dev) driver in place of the data generator, validator
and test-composer scripts. The system's images are the same ones the root
workload builds (same Dockerfiles and tags).

```
streaming/
  config/      docker-compose.yaml: the --config dir (nothing else goes here)
  driver/      the driver (Rust, hegel) and its Dockerfile
```

### What the driver does

One Rust program built on [hegel](https://hegel.dev), the main process of the
fault-free `driver` container. Every random choice comes from hegel, so
Antithesis controls it. Its rules, run concurrently on several workers:

- **insert accounts / insert transfers:** write fund-account and transfer
  records into the state-tracker, as the old data generator client did.
  Transfers pick any known account, favoring the newest, whose funding may not
  have been consumed yet. Inserts pause while 400 records wait to be produced.
- **produce:** `POST producer:3000/<mode>/<topic>/<n>`, one of the four
  delivery modes (`exactly_once_{single,batch}` to `bankEO`,
  `atleast_once_{single,batch}` to `bankAO`), 1-100 records.
- **consume:** `POST consumer:3000/<mode>/<topic>/<n>`, exactly-once from
  `bankEO` or at-least-once from `bankAO`. A consume call blocks until it has
  read `n` records (holding the consumer's only lock), so it asks only for
  what the state-tracker shows as produced and not yet consumed.
- **checks:** the guarantees below, against the state-tracker and DynamoDB.
- **eventually** (rare): stops faults, waits for every service, stops
  producing, gives the consumer 30 s to drain both topics, runs every check in
  the quiet system, then exits the driver to end the timeline.

Producer and consumer calls run on background threads (at most two
outstanding per service, since each service serves one request at a time), so
a hung service never stalls the driver.

### Properties

The guarantee assertions keep the original harness's messages, so their
history lines up with earlier runs, except the ordering property, which
follows `antithesis-instrumentation` (e930628): the old one flagged id gaps
across both topics, which two independently consumed topics never promise.

| Property | Type | Checked |
|---|---|---|
| Instances of exactly_once_producer produces more than once | always | anytime, and in the eventually |
| Exactly-once topic is consumed in the order it was produced | always | anytime, and in the eventually |
| Final total equals expected total | always | anytime (on two identical consecutive DynamoDB scans, since a scan isn't isolated from transactions), and in the eventually |
| produced ibans match ibans in dynamo | always | anytime, and in the eventually |
| Produced data matches consumed data after 30s of not producing and only consuming | always | eventually |
| the system recovers once faults stop | always | eventually |

Exploration assertions in the driver: `the workload starts`, `a produce
request completes: <mode>` for each mode, `a consume request completes:
<mode>` for each mode, and `a produce/consume request fails or gets no
answer`. The producer and consumer keep their own SDK assertions.

### Faults

`--fault-tolerance high_availability --fault-exclusions '^(driver|state-tracker)$'`.

The original `async_platform` runs injected node kills, stops, pauses and
throttles, partitions, clogs and clock skips on every container except the
state-tracker, which only got CPU throttling (it's the test's bookkeeping).
One difference: the old launcher never put network faults on `kafka-1`, but
did kill, stop and pause it. snouty can't exclude a container from network
faults only, so `kafka-1` now gets network faults too.

### Run it

```bash
cd antithesis/streaming/config
docker compose build
snouty validate .
snouty launch --launcher basic_test --config . --max-compute-hours 24 \
  --fault-tolerance high_availability --fault-exclusions '^(driver|state-tracker)$'
```

Locally, `docker compose up` and then
`docker compose exec driver /opt/antithesis/test/v1/main/first_start_workload`
starts the workload. It can't run alongside the root workload's stack: both
use the same container names and subnet.

### Known findings

See `FINDINGS.md` in this directory.
