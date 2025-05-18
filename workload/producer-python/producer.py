import json
from flask import Flask, request, jsonify
from flask import request
from kafka import KafkaProducer

app = Flask(__name__)


producer = KafkaProducer(
    bootstrap_servers=["kafka-3:9092", "kafka-2:9092", "kafka-1:9092"],
    api_version=(3,4,0),
    # delivery_timeout_ms=5000,
    acks='all',
    enable_idempotence=True,
    max_in_flight_requests_per_connection=1,
    retries=10
)


def on_send_success(record_metadata):
    return f'Topic: {record_metadata.topic} | Partition: {record_metadata.partition} | Offset: {record_metadata.offset}'


def on_send_error(e):
    log.error('Producer error', exc_info=e)


@app.route('/txn', methods = ['POST'])
def produce_txn():
    producer.begin_transaction()
    txn_data = request.json.encode('utf-8')
    print(f'Transaction data: {txn_data}')
    future = producer.send('txn', value=txn_data)
    
    try:
        record_metadata = future.get(timeout=10)
        return on_send_success(record_metadata)
    except Exception as e:
        on_send_error(e)
        return 'Error producing message', 500
    
