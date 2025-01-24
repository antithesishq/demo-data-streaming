# Data Streaming Demo

This is a sales demo aimed at organizations that build applications with a distributed/asychronous architecture. It is relevant for organizations that:

* Have microservices architecture
* Use message-broker or streaming technology such as Apache Kafka
* Use AWS services such as SQS, S3, DynamoDB

## Highlight of the demo

For now, the demo focuses on testing for **correctness** related guarantees such as:

1. Producer produced at least once
2. Consumer consumed exactly once
3. Consumer consumed at least once

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