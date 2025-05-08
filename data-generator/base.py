#!/usr/bin/env nix-shell
#!nix-shell -i "python3 -i" -p "python311.withPackages(ps: [ ps.faker ps.flask ps.pydantic])"

import json, os
from flask import Flask
from faker import Faker
from faker.providers import bank
from customer_providers import ContactProvider

class generate_data():

    def __init__(self):
        self.fake = Faker()
        self.fake.add_provider(bank)
        
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
        """
        Simple provider with some data structure
        """
        return {
           'iban': self.fake.iban(),
           'aba': self.fake.aba(),
           'swift11': self.fake.swift11(primary=True),
           'bank_country': self.fake.bank_country(),
        }

    def _contact(self):
        self.fake.add_provider(ContactProvider)
        return self.fake.generate_contact()

generator = generate_data()

app = Flask(__name__)

record_types = ['_bank_account', 'contact']
record_type = os.environ.get('DATA_TYPE') 
if record_type not in record_types:
    record_type = '_bank_account'

@app.route("/batch/<int:num_records>")
def get_batch(num_records:int):
    if num_records > 1000:
        num_records = 1000

    return generator.generate_batch(num_records, record_type, False)

@app.route("/batch_sequential/<int:num_records>")
def get_batch_sequential(num_records:int):
    if num_records > 1000:
        num_records = 1000

    return generator.generate_batch(num_records, record_type, True)

@app.route("/single")
def get_single():
    return generator.generate_single(record_type)
