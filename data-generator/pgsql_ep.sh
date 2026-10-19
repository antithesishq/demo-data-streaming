#!/bin/bash

set -e

echo "Waiting for the data generator to be ready"
dg_ready=0
while [ $dg_ready -eq 0 ]; do
    echo "Checking for data generator at $DATA_GENERATOR_ENDPOINT for a 200 response"
    dg_ready=`curl -X POST -I $DATA_GENERATOR_ENDPOINT/single | grep 200 | wc -l`
    sleep 2
done

echo "Waiting for Postgres to be ready"
psql_ready=0
while [ $psql_ready -eq 0 ]; do
    echo "Attempting to check Postgres with $POSTGRES_USER@$POSTGRES_HOST/$POSTGRES_DB"
    psql_ready=$(python3 /root/pgsql_client.py check_data_store | grep successful | wc -l)
    sleep 2
done

echo "Data generator and data store ready for random data generation"
#1 creating database schema
#2 continuously getting random data and dumping them into the database

echo "creating producer table"
python3 /root/pgsql_client.py create_schema

BANK_TEST_NO_ACCOUNTS=${BANK_TEST_NO_ACCOUNTS:-"20"}

# initialize a bunch of bank accounts with values
if [[ $DATA_TYPE == "_banktest_transaction" ]]; then
    DATA_TYPE=_banktest_fund_account python3 /root/pgsql_client.py fetch_n_save --batch_size=$BANK_TEST_NO_ACCOUNTS
fi

echo "starting to continuously generate + import data"
while true
do
    python3 /root/pgsql_client.py fetch_n_save --batch_size=$BATCH_SAVE_SIZE
    sleep 1

    # looping between generating transactions and creating new bank accounts
    if [[ $DATA_TYPE == "_banktest_transaction" ]]; then
        DATA_TYPE=_banktest_fund_account python3 /root/pgsql_client.py fetch_n_save --batch_size=$BANK_TEST_NO_ACCOUNTS
    fi
done
