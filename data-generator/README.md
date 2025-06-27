## What is this?
This is a simple fake data generator service paired with a client that stores generated data into a data store

## How to use
Build **data_generator** and **pgsql_client** container image such as:

* docker build -t data_generator:latest -f faker.Dockerfile .
* docker build -f pgsql_client.Dockerfile -t pgsql_client:latest .

Inside of the directory use docker-compose up -d or similar command 

## Components

* data-generator - A stand-alone http server that can generate different types of data
* data-store - A postgres database that can be used to store generated data
* data-generator-client - A client that continuously request data from the data-generator and save them into the data-store. The intend is for the data store to be used as a "state tracker".

## Configuration

There are several configurations available as environment variables for the data-generator-client

* DATA_TYPE: The type of data to request from the data-generator. Currently we support the following 

    * _bank_account - Bank account with the fields of iban, aba, swift11, bank_country
    * _banktest_fund_account - Same as bank account with an additional field of "amount"
    * _banktest_transaction - A transaction between 2 accounts (iban) that exist in the system.
    * _contact - A contact record with given name, family name, email address and phone number

* BANK_TEST_NO_ACCOUNTS: the number of bank accounts to generate in a batch. This is only used if the DATA_TYPE is set to _banktest_transaction

When the data type is set to _banktest_transaction, the data-generator-client will attempt to create BANK_TEST_NO_ACCOUNTS first before generating transactions against these accounts. It will continue to go through a loop of generating accounts followed by generating transactions against all accounts in the database.

* BATCH_SAVE_SIZE: the number of records to request from the data-generator in a batch. This currently caps at 1000. The client will continuously fetch (environment variable $BATCH_SAVE_SIZE) number of fake data from the data generator and save them into the database. There is a small 1 second sleep between each fetch and import.

## Manual (raw) usage
The data generator can be reached at http://data-generator:5000 via POST requests, there are several endpoints at the moment:

* http://data-generator:5000/single 
* http://data-generator:5000/batch
* http://data-generator:5000/batch_sequential

Making example request with curl for 500 randomly generated contact record

'''
curl -X POST -d "data_type=_contact" http://data-generator:5000/batch/500
'''

The difference between batch and batch_sequential is the latter will have a field called "serial_id" 

## Todo
* Introduce probabilistic bad data (e.g. transaction amount overflow, malformed data) systematically
* Easier build scripts
* Antithesis SDK assertions
