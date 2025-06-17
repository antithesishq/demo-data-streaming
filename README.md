# Data Streaming Demo

This is a sales demo aimed at organizations that build applications with a distributed/asychronous architecture. It is relevant for organizations that:

* Have microservices architecture
* Use message-broker or streaming technology such as Apache Kafka
* Use AWS services such as SQS, S3, DynamoDB

## Highlight of the demo

Currently the demo spins up multiple producers and consumers exercising different semantics that a business application using Kafka might rely on
The different configurations spun up:

1. Producer and Consumer implementation using "atleast once" delivery semantics using streaming mode
2. Producer and Consumer implementation using "atleast once" delivery semantics using batch mode
3. Producer and Consumer implementation using "exactly once" delivery semantics using streaming mode
4. Producer and Consumer implementation using "atleast once" delivery semantics using batch mode

Currently, the demo focuses on testing the following **correctness** related guarantees:

1. Data being generated is produced by the Kafka Producers
2. Data is consumed by the Kafka Consumers
3. "exactly once" semantics are implemented correctly
   - We have an implementation of a producer and consumer according to Kafka's "exactly once" semantics
   - Our test validates that that this guarantee is held through an entire test
4. "atleast once" semantics are implemented correcty
   - Our test validates that all data that is produced by the producer is consumed "atleast once"

Additionally, we test the following **availability** related guarantee:

1. When the producer is stopped for 30s and we only consume for 30s, produced data should be consumed

The impact of these guarantee violations will result in downstream business impact such as duplicate records created, potential data-loss, etc. 

## Running the demo (coming soon!)

1. Use the Github action to trigger a test and include your email as a receipent to the report.
2. (Optional) Generate a bug report from one of the issues found from the report.
3. Run the interactive debugging notebook to show basic concepts around the multiverse debugger


## Contribute to the Demo

## Run things locally

The simplest way to run the demo stack is to run

`make run` or manually do `docker-compose up -d`

You can also run `make all` to build all of the container images and run the stack

## Sanity-check test templates locally

(TBD)

## Test Properties

### how we are validating things

### producer

### passthrough consumer

## How to contribute

## Components

### data generator

The data generator is a simple http service that can be reached at http://[data_generator]:5000

It has the following endpoints

```
http://[data_generator]:5000:/batch/<num_records>
http://[data_generator]:5000:/batch_sequential/<num_records>
http://[data_generator]:5000:/single
```

The main difference is that batch_sequential will generate a batch of data with a serial ID. The single batch is limited at 1000 records for now.

### data generator client (pgsql)

The data generator client continuously requests batches of data from the data generator and save them in the state tracker to be used as the master source of truth.

### producer

### passthrough consumer

### state tracker

The state tracker is a Postgres database used to track the status of the data being produced and consumed. When the producer/consumer successfully complete an operation, it is recorded in the state tracker `producer` table. 

### validator

### Docker-compose configuration

### (coming soon) DynamoDB

### (coming soon) S3/Minio

## Todos

Mostly we will create issues in the repo
