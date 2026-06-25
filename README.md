# Data Streaming Demo

A sales demo that shows how [Antithesis](https://antithesis.com) finds correctness and availability bugs in a Kafka-based pipeline by running the whole system under deterministic fault injection while continuously checking a set of properties.

It models a simple bank: fake accounts and transactions are generated, produced to Kafka under four different delivery configurations, consumed back out, and applied to account balances in DynamoDB. Antithesis perturbs the environment (network faults, restarts, message reordering, scheduling) and reports any property that can be violated.

The demo is most relevant to organizations that:

* Build on a microservices / asynchronous architecture
* Use a message broker or streaming technology such as Apache Kafka
* Use AWS services such as SQS, S3, or DynamoDB

## Contents

* [Why this demo](#why-this-demo)
* [Architecture](#architecture)
* [Repository layout](#repository-layout)
* [Quickstart](#quickstart)
  * [Prerequisites](#prerequisites)
  * [Run locally](#run-locally)
  * [Launch an Antithesis test](#launch-an-antithesis-test)
* [Properties under test](#properties-under-test)
  * [How validation works](#how-validation-works)
  * [Sanity-checking test templates locally](#sanity-checking-test-templates-locally)
* [Components](#components)
* [Contributing](#contributing)
* [Roadmap](#roadmap)

## Why this demo

Distributed pipelines make guarantees that are easy to state and hard to keep: "exactly once", "at least once", "messages are processed in order", "money is never lost". These guarantees usually hold on a good day and break only under the rare interleaving of a crash, a retry, and a rebalance — exactly the conditions that are hard to reproduce in a normal test suite.

Antithesis runs this system in a deterministic hypervisor, injects faults, and explores the state space looking for an execution that violates a property. When it finds one, the exact run is fully reproducible and can be replayed in the multiverse debugger. This demo wires up real-world delivery semantics so those violations — duplicate processing, dropped records, out-of-order consumption, invented or lost money — can be surfaced and explained.

## Architecture

```
                 ┌──────────────────┐
                 │  data_generator  │  Flask + Faker HTTP service
                 │   (port 5000)    │  generates fake bank data
                 └────────┬─────────┘
                          │ HTTP (fetch batches)
                          ▼
               ┌──────────────────────┐
               │ data_generator_client│ continuously fetches + inserts
               └──────────┬───────────┘
                          │ INSERT
                          ▼
               ┌─────────────────────┐
               │   state-tracker     │  Postgres "producer" table:
               │     (Postgres)      │  source of truth for produced/consumed
               └──┬───────────────▲──┘
          read    │               │  record produced / consumed timestamps
       unproduced │               │
                  ▼               │
            ┌──────────┐    ┌──────────┐       ┌──────────┐
            │ producer │───▶│  Kafka   │──────▶│ consumer │
            │  (axum)  │    │ 3 brokers│       │  (axum)  │
            └──────────┘    │  (KRaft) │       └────┬─────┘
                            └──────────┘            │ apply transactions
                                                    ▼
                                              ┌──────────┐
                                              │   ddb    │ DynamoDB-local
                                              │ balances │ (accounts table)
                                              └──────────┘

            ┌───────────┐   reads state-tracker + DynamoDB and asserts
            │ validator │   correctness / availability properties
            └───────────┘
```

The **state-tracker** is the source of truth: every generated record is one Postgres row whose lifecycle columns record when and how it was produced and consumed. The **validator** and the **test-composer** scripts are driven by Antithesis to apply load and check properties while faults are injected.

## Repository layout

```
.
├── docker-compose.yaml        # the full system under test (used locally and by Antithesis)
├── config.Dockerfile          # packages docker-compose.yaml into a "config image"
├── Makefile                   # build / push / run targets
├── data-generator/            # Flask + Faker service and the Postgres loader client
├── kafka/                     # 3-broker Kafka (KRaft) image
├── dynamoDb/                  # DynamoDB-local helpers (AWS CLI tooling)
├── workload/
│   ├── producer/              # Rust/axum producer (4 delivery modes)
│   ├── consumer/              # Rust/axum passthrough consumer (2 modes)
│   └── validator/             # Rust validator CLI + test-composer scripts
└── .github/workflows/         # CI + Antithesis trigger / debugging workflows
```

## Quickstart

### Prerequisites

* **Docker** and the **`docker compose`** plugin — everything builds and runs in containers, so no local Rust/Python toolchain is required.
* **`make`** — for the convenience targets in the [`Makefile`](Makefile).
* To **push** images you need credentials for the demo's container registry (the `push-*` targets call `customer credentials_shell`).

### Run locally

```bash
make all     # build all images, then start the stack
# or, separately:
make build-all
make run     # docker-compose up -d
make down    # docker-compose down
```

Individual images can be built or pushed with the per-component targets, e.g. `make build-producer` / `make push-producer`. Images are tagged both locally (`producer:latest`) and for the registry.

### Launch an Antithesis test

1. Build and push the images (`make build-all && make push-all`) and the config image (`make build-config && make push-config`).
2. Run the [`Run Antithesis Test`](.github/workflows/run_antithesis_test.yml) GitHub Action (`workflow_dispatch`), providing:
   * `duration` — how long the test runs, in hours
   * `emails` — comma-separated recipients for the report
3. When the report arrives, optionally open a finding in the **multiverse debugger** to replay and inspect the exact failing run.

The action submits the locally-built images plus a `config_image` — a `FROM scratch` image carrying only `docker-compose.yaml` — to the `async_platform` notebook in the `demo` tenant via the [`antithesis-trigger-action`](https://github.com/antithesishq/antithesis-trigger-action).

## Properties under test

Properties are expressed with [Antithesis SDK](https://antithesis.com/docs/using_antithesis/sdk/) assertions (`assert_always`, `assert_sometimes`, `assert_reachable`, `assert_unreachable`) embedded in the producer, consumer, and validator, plus standalone test-composer scripts.

| # | Property | Kind | Where checked |
|---|----------|------|---------------|
| 1 | Generated data is produced to Kafka | liveness | producer (`assert_sometimes`) |
| 2 | Produced data is consumed from Kafka | liveness | consumer (`assert_sometimes`) |
| 3 | **Exactly-once**: no record is consumed more than once | correctness | `validator --test exactly-once-guarantee` |
| 4 | **At-least-once**: every produced record is eventually consumed | correctness | `validator --test producer-consumer-matches` |
| 5 | **Ordering**: consumption order matches generation order | correctness | `validator --test consumption-order` |
| 6 | **Conservation of money**: total balance never changes from transfers | correctness | [`anytime_validate_balance.py`](workload/validator/test-composer/anytime_validate_balance.py) |
| 7 | Every account in DynamoDB was actually produced and consumed | correctness | [`eventually_ibans_match.py`](workload/validator/test-composer/eventually_ibans_match.py) |
| 8 | With production stopped, all produced data is consumed within 30s | availability | `validator --test producer-consumer-matches` |

Violating any of these maps to a real business impact: duplicate records, data loss, out-of-order processing, or money created/destroyed.

### How validation works

The **state-tracker** Postgres `producer` table is the source of truth. Each row tracks a single generated record and the lifecycle columns `produced_timestamp`, `producer_type`, `consumed_timestamp`, `consumer_type`, and `consumed_count`. By inspecting these columns the validator decides whether the delivery and ordering guarantees held. DynamoDB holds the resulting account balances, which the money-conservation property checks.

The validator ([`workload/validator/validator/src/main.rs`](workload/validator/validator/src/main.rs)) is a small Rust CLI with one subcommand per property:

* `--test exactly-once-guarantee` — asserts (`always`) that **no** record produced by an `exactly_once` producer has `consumed_count > 1`.
* `--test consumption-order` — asserts (`always`) that consumed record IDs form a contiguous, monotonically increasing sequence (consumption order == generation order).
* `--test producer-consumer-matches` — for 30s, while production is stopped and only consumption runs, asserts (`always`) that every produced record is eventually consumed.

### Sanity-checking test templates locally

The [`workload/validator/test-composer`](workload/validator/test-composer) scripts are what Antithesis runs to drive load and validate the system. Because every service is reachable on the `kafka-net` docker network, you can run these by hand against a locally running stack (`make run`) before launching a full test:

```bash
docker compose exec validator /validator/target/debug/validator --test exactly-once-guarantee
docker compose exec validator bash test-composer/parallel_driver_produce_atleast_once_single.sh
```

## Components

### data generator

[`data-generator/base.py`](data-generator/base.py) is a Flask HTTP service reachable at `http://data-generator:5000`. It uses [Faker](https://faker.readthedocs.io/) with [custom providers](data-generator/custom_providers.py) that source their randomness from the Antithesis SDK, so the generated data is part of the explorable test space.

Endpoints:

```
POST http://data-generator:5000/single
POST http://data-generator:5000/batch/<num_records>
POST http://data-generator:5000/batch_sequential/<num_records>
```

The data type is selected with a `data_type` form field (`_bank_account`, `_banktest_fund_account`, `_banktest_transaction`, `_contact`). `batch_sequential` adds a `serial_id` field, and batches are capped at 1000 records. The `_contact` provider deliberately injects malformed data (bad emails, blank fields) a small percentage of the time. See [`data-generator/README.md`](data-generator/README.md) for full details.

### data generator client (pgsql)

[`data-generator/pgsql_client.py`](data-generator/pgsql_client.py) continuously requests batches from the data generator and saves them into the state tracker. Its [entrypoint](data-generator/pgsql_ep.sh) waits for the data generator and Postgres, creates the `producer` schema, then loops fetching and inserting. For the bank-transaction workload it first seeds a pool of funded accounts, then alternates between generating transactions against them and adding more accounts. Configured via environment variables (`DATA_TYPE`, `BATCH_SAVE_SIZE`, `BANK_TEST_NO_ACCOUNTS`, Postgres connection settings).

### producer

[`workload/producer/producer/src/main.rs`](workload/producer/producer/src/main.rs) is an `axum` HTTP service with one nested route per delivery mode:

```
POST producer:3000/exactly_once_single/<topic>/<num_records>
POST producer:3000/exactly_once_batch/<topic>/<num_records>
POST producer:3000/atleast_once_single/<topic>/<num_records>
POST producer:3000/atleast_once_batch/<topic>/<num_records>
```

On each call it reads up to `<num_records>` rows from the state-tracker that have no `produced_timestamp`, produces them to the given Kafka `<topic>`, and writes back `produced_timestamp` and `producer_type`. The `exactly_once` modes use Kafka transactions (`enable.idempotence=true`, `transactional.id`, `acks=all`) and commit each message transactionally; the `batch` modes add `linger.ms` / `batch.size` / `compression` tuning.

### passthrough consumer

[`workload/consumer/consumer/src/main.rs`](workload/consumer/consumer/src/main.rs) is an `axum` HTTP service with one nested route per consumer mode:

```
POST consumer:3000/exactly_once_pass_through_consumer/<topic>/<num_records>
POST consumer:3000/atleast_once_pass_through_consumer/<topic>/<num_records>
```

It consumes `<num_records>` messages from `<topic>`, records `consumed_timestamp` / `consumer_type` and increments `consumed_count` in the state-tracker, then **applies the message as a bank transaction to DynamoDB** (funding an account or transferring between two accounts). The `exactly_once` consumer uses `isolation.level=read_committed` with manual offset commits; the `atleast_once` consumer uses auto-commit.

### state tracker

A Postgres database that tracks the status of data being produced and consumed. When a producer/consumer completes an operation it is recorded in the `producer` table:

| column                | meaning                                              |
| --------------------- | ---------------------------------------------------- |
| `id`                  | identity primary key (also the generation order)     |
| `data`                | the JSON payload of the generated record             |
| `topic`               | the data type / topic the record belongs to          |
| `produced_timestamp`  | set when a producer successfully produces the record |
| `producer_type`       | which producer mode produced it                      |
| `consumed_timestamp`  | set when a consumer successfully consumes the record |
| `consumer_type`       | which consumer mode consumed it                      |
| `consumed_count`      | number of times the record has been consumed         |

### validator

[`workload/validator/validator/src/main.rs`](workload/validator/validator/src/main.rs) is a Rust CLI that runs a single property check per invocation against the state-tracker (and, for the Python scripts, DynamoDB). See [Properties under test](#properties-under-test). In `docker-compose` the validator container's [entrypoint](workload/validator/entrypoint.sh) marks the Antithesis setup complete and idles; the actual checks are invoked by the test-composer scripts during a test.

### Kafka

A 3-broker Apache Kafka cluster in [KRaft mode](kafka/kafka.Dockerfile) (no ZooKeeper). `kafka-1` is the combined broker/controller; `kafka-2` and `kafka-3` are brokers. The image is built on a JDK base with debugging tools (`gdb`, `strace`) so brokers can be inspected during a test.

### DynamoDB

A local [Amazon DynamoDB](https://hub.docker.com/r/amazon/dynamodb-local) instance (`ddb`) holds the `accounts` table keyed by `iban`. The consumer applies each consumed bank transaction as a DynamoDB transactional write (`transact_write_items`) — funding an account or transferring funds between two accounts atomically. The `ddd` helper container ([`dynamoDb/Dockerfile`](dynamoDb/Dockerfile)) bundles the AWS CLI for ad-hoc inspection during debugging (see [`dynamoDb/cool_commands`](dynamoDb/cool_commands)).

### docker-compose configuration

[`docker-compose.yaml`](docker-compose.yaml) wires every service onto a single bridge network (`kafka-net`, subnet `11.0.0.0/24`) with static IPs and health-check/`depends_on` ordering so the stack comes up in the right sequence. This same file is packaged into the `config_image` that Antithesis uses to stand up the system under test.

## Contributing

This is an evolving demo. The general workflow:

1. Change a component under [`workload/`](workload), [`data-generator/`](data-generator), [`kafka/`](kafka), or [`dynamoDb/`](dynamoDb).
2. Rebuild the affected image(s) with the relevant `make build-*` target and run the stack with `make run`.
3. Add or update Antithesis assertions and/or test-composer scripts to cover the new behavior, and sanity-check them locally.
4. Push images with `make push-*` and launch a test via the GitHub Action.

Larger pieces of work are tracked as issues in the repo.

## Roadmap

* Probabilistic bad-data injection in the data generator (amount overflow, malformed records)
* Easier / standardized build scripts
* S3 / Minio object-storage workload
* More exhaustive Kafka error classification in the producer/consumer (several errors are currently retried rather than classified)
