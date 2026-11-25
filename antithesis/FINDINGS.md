# Findings: plugin-built streaming workload

SUT bugs the new workload surfaces. The failing guarantee properties are the
demo's intended bugs; this file records the evidence for each.

**Latest run:** 8ff1d6f953a5adee728d54ae398cfbea-67-26 (24 compute hours,
[report](https://public.antithesis.com/report/bAgXAqQuOrTfdtBzkrXPdtYx/a3U6991sq_6EoO0SHEXengQGhlACRE5MaQIEOu_yUQ8.html)).
Failing: `Final total equals expected total` and `produced ibans match ibans
in dynamo` (finding 2), `Produced data matches consumed data after 30s ...`
(finding 6), and four consumer `Sometimes` assertions written as
`assert_sometimes!(false, ...)`, which can never pass. Those four are now
`assert_unreachable!`, as in the other session's working copy of
`antithesis-instrumentation`, so they fail only if the consumer really gets a
payload it can't decode. Passing: exactly-once
ordering and no-duplicates, recovery once faults stop, no unexpected
container exits, instrumentation of every component.

## 1. Exactly-once single and batch producers fence each other; the producer hangs forever

- **Seen:** locally, without faults (`antithesis/streaming/runs/local-*`).
- **Cause:** `workload/producer/producer/src/main.rs` gives both exactly-once
  producers the same `transactional.id` (`"eactly_once_id"`). Creating the
  second one calls `init_transactions`, which fences the first. The next
  request to the fenced mode fails `send` with `PurgeQueue` and `safe_send`
  retries it forever (`sleep(1)`, no fatal check on the send path), while the
  handler holds the service-wide `producers` lock. From then on the producer
  serves no request until its container restarts.
- **Log:** `librdkafka: Fatal error: Local: This instance has been fenced by a newer instance`, then `kafka: failed to produce transaction ... PurgeQueue (Local: Purged in queue), retrying...` repeating.

## 2. A transfer consumed before its account's funding destroys money

- **Seen:** locally, without faults, and in run
  517ae013d7b55b3417379a1327527570-67-26: `Final total equals expected total`
  (first at vtime 24.10, 8 accounts, expected 8000, found 1000) and `produced
  ibans match ibans in dynamo` (first at vtime 24.21, 18 IBANs).
- **Run evidence:** fund records 2-80 went to `bankAO`, transfers 81+ to
  `bankEO`. The `bankEO` consumer applied transfers (`dynamodb: transfer
  successful : 85 from ...14 to ...03`) to accounts with no funding yet. When
  `bankAO` was later consumed, each such fund failed (`dynamodb: fatal:
  non-retryable error encountered: service error`) and was dropped, so the
  loss is permanent. Not a check artifact: the balance check needs two
  identical consistent scans, and the IBAN check reads DynamoDB before Postgres.
- **Cause:** the consumer applies a transfer with `ADD balance :amount` on both
  accounts, which creates an account that doesn't exist yet. The account's
  fund record, consumed later, is a `Put` conditioned on
  `attribute_not_exists(iban)`, so it fails with a non-retryable error and is
  dropped. The account exists without its 1000. Fund and transfer records go
  to different topics and modes, so nothing orders them.

## 3. Consumption skips records across the two topics (property replaced)

- **Seen:** locally, and in run 517ae013... at vtime 23.70 (consumed {1, 81},
  ids 2-80 on `bankAO` not yet consumed). All were consumed by vtime ~44:
  nothing is lost.
- **Judgment for the user:** this property (kept unchanged from the old
  harness) asserts that consumed ids are contiguous across both topics, which
  two independently consumed topics never promise. By that reading it is a
  check stricter than the guarantee. A stricter-to-the-design alternative is
  contiguity per topic. The consumer's single service-wide lock makes the lag
  worse: a consume request blocked on one topic (waiting for more records than
  it holds) starves the other topic's consumer.
- **Cause:** the producer reads unproduced records in id order whatever the
  topic, so consecutive ids land on `bankEO` and `bankAO`, whose consumers run
  independently. One topic's consumer lagging leaves gaps in the consumed ids.
- **Resolution:** `antithesis-instrumentation` (e930628) reached the same
  conclusion and replaced the property with `Exactly-once topic is consumed
  in the order it was produced`, using produce/consume sequence numbers. The
  driver now checks that property.

## 4. A consume request for more records than the topic holds blocks the consumer forever

- **Seen:** run 517ae013d7b55b3417379a1327527570-67-26, when the first driver
  version over-asked on purpose (2% of consume calls). At vtime 19.29 it asked
  `bankAO` for 16 records while 2 existed. From 27.61 that request held the
  consumer's service-wide lock in `while count < num_records` with no timeout,
  so every later consume request, including the eventually's drain, queued
  behind it: 6 committed `bankEO` records (ids 3-8) were never read, and
  `Produced data matches consumed data after 30s ...` failed.
- **Cause:** `workload/consumer/consumer/src/main.rs` `handle` loops until it
  has read `num_records` messages, holding `state.consumers` for the whole
  call, and the spawned task keeps running after the HTTP client gives up.
  The old harness asked for 10,000 records in its eventually script, which
  probably explains why that property failed in every old run.
- **Workload:** the driver no longer over-asks, so the drain property now
  tests delivery rather than this hang.

## 5. A restarted Kafka broker gives up and exits when the single controller is unreachable for 60 s

- **Seen:** run 517ae013d7b55b3417379a1327527570-67-26, `No unexpected
  container exits: container: kafka-2, exit code: 1` (vtime 108.69).
- **Timeline:** kafka-2 killed at 42.5 and restarted; for the next 62 s,
  clogs, partitions, two pauses of kafka-2 and a kill of kafka-1 (the only
  controller) kept it from registering. 107.49: `Shutting down because we were
  unable to register with the controller quorum`, then `FATAL Exiting Kafka
  due to fatal exception during startup`.
- **Cause:** Kafka's `initial.broker.registration.timeout.ms` (60 s) with one
  controller voter (`KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093`) and no
  supervisor to restart the broker. Deployment fixes, if wanted: three
  controller voters, a longer registration timeout, or an entrypoint that
  restarts Kafka on exit (not a Compose `restart:` policy).
- **Resolution, partial:** `antithesis-instrumentation` (7f51398) moved to
  three broker+controller nodes; the workload's compose follows it. It still
  happens: run 8a4052b39598dbca43422d6b3ab75dcd-67-26 had kafka-3 (vtime
  231.5) and kafka-1 (257.8) exit 1 the same way. Each restarted while another
  voter was down and partitions or clogs held latency above KRaft's 2 s fetch
  timeout; leaders were elected and resigned within 3-5 s (`Did not receive
  fetch request from the majority of the voters within 3000ms`), so the
  restarted node never registered within 60 s. Raft behaved correctly; Kafka
  exits by design. Options: raise `initial.broker.registration.timeout.ms`,
  or an entrypoint that restarts Kafka on exit.
- **Resolution:** `antithesis-instrumentation` f8a1803 sets
  `KAFKA_INITIAL_BROKER_REGISTRATION_TIMEOUT_MS: 3600000`; with it, run
  8ff1d6f9... had no unexpected container exits.

## 6. A restarted consumer hangs forever creating a DynamoDB table that exists

- **Seen:** run 5493a54273c86fc0a40d7033d4696fca-67-26, `Produced data matches
  consumed data after 30s of not producing and only consuming` (vtime 257.83,
  167 records produced and never consumed). Kafka held all of them
  (`bankAO:0:97`, `bankEO:0:140`) and no consumer group had committed an
  offset: nothing was lost; nothing was consumed.
- **Timeline:** the consumer restarted after faults (`Starting Consumer` at
  219.42). Its first request, at 223.34, called `safe_create_table`, which
  logged `dynamodb: error: failed to create table ServiceError(...
  ResourceInUseException ... "Cannot create preexisting table"), retrying ...`
  every second until the check failed, holding the service-wide lock.
- **Cause:** `workload/consumer/consumer/src/main.rs` `safe_create_table`
  treats "already exists" as success only if `e.to_string()` contains
  `ResourceInUseException`, but an AWS `SdkError`'s Display text never names
  the variant, so it retries forever (with a blocking `std::thread::sleep`
  inside async code). Fix: `e.into_service_error().is_resource_in_use_exception()`.
  Reached once DynamoDB keeps its tables across restarts (e930628).

## 7. Exactly-once delivery breaks end to end: a producer stop duplicates a record

- **Seen:** run 99cf2345eddc3a3e1ffc38b2c5b8d748-67-26, `Instances of
  exactly_once_producer produces more than once` (ids 990 and 235 consumed
  twice).
- **Timeline (id 990):** 139.437 `transaction committed successfully with
  topic bankEO, id 990`, the Postgres UPDATE starts, and at 139.466 a `stop`
  fault hits the producer before it completes. After the restart, 181.629
  `unproduced data query completed successfully: [990, 991, 992]`, and 182.239
  commits id 990 to `bankEO` again. The `read_committed` consumer reads both
  copies (182.42, 182.45) and applies the transfer twice.
- **Cause:** the producer commits the Kafka transaction, then marks the row
  produced in Postgres as a separate step. Kafka transactions give
  exactly-once only inside Kafka. Fixes: mark the row (or a pending state)
  before committing and reconcile on restart, an outbox, or consumer-side
  dedupe by id.

## 8. Kafka 4.3.1 exits on a stop during startup

- **Seen:** run cbc43d11933c4006aa0984d9d38dd402-67-26: kafka-2 and kafka-3
  `exit code: 1`, e.g. `ProcessTerminatingFaultHandler - Encountered fatal
  fault ... NullPointerException: Cannot invoke
  "org.apache.kafka.raft.RaftManager.client()" because "this.raftManager" is
  null`, or the same in `MetadataLoader#installPublishers`, when a `stop`
  lands while the broker is still starting. A Kafka bug, deliberately not
  worked around.
