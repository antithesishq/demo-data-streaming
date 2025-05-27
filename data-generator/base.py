#!/usr/bin/env nix-shell
#!nix-shell -i "python3 -i" -p "python311.withPackages(ps: [ ps.faker ps.flask ps.pydantic])"

import json, os
from flask import Flask, request
from faker import Faker
from custom_providers import ContactProvider
from custom_providers import BankProvider

class generate_data():

    def __init__(self):
        self.fake = Faker()
        
    def generate_batch(self, num_records:int, provider_type:str='_bank_account', sequential:bool=False) -> str|None:
        """
        Generating a batch 
        """
        data = []
        func = getattr(self, provider_type)
        for i in range(num_records):
            random_record = func()
            if sequential:
                random_record['serial_id'] = i

            data.append(random_record)

        return json.dumps(data)

    def generate_single(self, provider_type:str='_bank_account'):
        """
        Generate a single record
        """
        func = getattr(self, provider_type)
        return json.dumps(func())

    # Various data providers
    def _bank_account(self):
        self.fake.add_provider(BankProvider)
        return self.fake.generate_account()

    def _contact(self):
        self.fake.add_provider(ContactProvider)
        return self.fake.generate_contact()


def request_data_type():

    valid_data_types = ['_bank_account', '_contact']

    # default data type
    data_type = '_bank_account'
    if bool(request.form) and 'data_type' in request.form:
        if request.form['data_type'] in valid_data_types:
            data_type = request.form['data_type'] 

    return data_type

generator = generate_data()

app = Flask(__name__)

@app.route("/batch/<int:num_records>", methods=['POST'])
def get_batch(num_records:int):
    record_type = request_data_type()
    if num_records > 1000:
        num_records = 1000

    return generator.generate_batch(num_records, record_type, False)

@app.route("/batch_sequential/<int:num_records>", methods=['POST'])
def get_batch_sequential(num_records:int):
    record_type = request_data_type()
    if num_records > 1000:
        num_records = 1000

    return generator.generate_batch(num_records, record_type, True)

@app.route("/single", methods=['POST'])
def get_single():
    record_type = request_data_type()
    return generator.generate_single(record_type)
