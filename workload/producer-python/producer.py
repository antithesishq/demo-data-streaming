from flask import Flask, request, jsonify
from flask import request
from json import dumps
from kafka import KafkaProducer

app = Flask(__name__)

producer = KafkaProducer(
    bootstrap_servers=["kafka-3:9092", "kafka-2:9092", "kafka-1:9092"],
    delivery_timeout_ms=5000,
    acks=all,
    enable_idempotence=True,
    max_in_flight_requests_per_connection=1,
    retries=10
)

@app.route('/txn', methods = ['POST'])
def produce_txn():
    txn_data = request.json
    print(f'Transaction data: {txn_data}')
    return producer.send('txn', value=jsonify(txn_data))
