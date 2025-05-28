## What is this?
This is a simple fake data generator service paired with a client that stores generated data into a data store

## How to use
Build **data_generator** and **pgsql_client** container image such as:

* docker build -t data_generator:latest -f faker.Dockerfile .
* docker build -f pgsql_client.Dockerfile -t pgsql_client:latest .

Inside of the directory use docker-compose up -d or similar command 

## Some configuration
The data generator client will continuously fetch (environment variable $BATCH_SAVE_SIZE) number of fake data from the data generator and save them into the database. There is a small 1 second sleep between each fetch and import.

## Manual usage
The data generator can be reached at http://data-generator:5000, there are several endpoints at the moment:

* http://data-generator:5000/single 
* http://data-generator:5000/batch 
* http://data-generator:5000/batch_sequential

The difference between batch and batch_sequential is the latter will have a field called "serial_id" 

## Todo
1. Add more types of data from faker to generate
2. From the client perspective, make the data schema more generic (e.g. serialize the data) to support more fake data types
