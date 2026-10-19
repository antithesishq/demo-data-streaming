'''
A client that constantly ask faker for data and import them into database table

@todo
1. Create method to connect to postgres database(done)
2. Create method to create the postgres schema  (done)
3. Create method to bulk insert data into postgres schema (done)
4. Create method to make request to faker service via http requests and dump the data into the schema (done)
5. Create a wrapper for continuous data import with some batch size/rate
6. Tie everything together with argparser and CLI
7. Build entrypoint to wait for postgres and data generator to be ready 
'''

import psycopg2, requests, os, argparse, json, time

class faker_pgsql():    
    def __init__(self, pg_host:str, pg_user:str, pg_pass:str, pg_db:str, data_generator_endpoint:str) -> None:
        self.pg_host = pg_host
        self.pg_user = pg_user
        self.pg_pass = pg_pass
        self.pg_db = pg_db

        self.faker_endpoint = data_generator_endpoint

        self.pg_conn = self.db_connect()
        # @todo: retry loop with some back off limit in the main execution as a way to wait for postgres to come online
        # if not bool(self.pg_conn):
        #     raise Exception(
        #         "Unable to connect to the postgres database"
        #     )

    def db_connect(self):
        return psycopg2.connect(host=self.pg_host,database=self.pg_db,user=self.pg_user, password=self.pg_pass)

    def fetch_and_save(self, num_to_get=100):
        try:
            records = self.get_faker_data(num_to_get)
            if not records:
                return
            self.insert_records(records)
        except requests.exceptions.RequestException as e:
            print(f'Error fetching data from generator: {e}')
        except (psycopg2.Error, json.JSONDecodeError, ValueError, TypeError) as e:
            print(f'Error saving fetched data: {e}')

    def create_schema(self) -> None:
        '''
        Create a schema of different Faker types
        '''
        try:
            cursor = self.pg_conn.cursor()
            cursor.execute('''CREATE TABLE producer (
                id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                data TEXT,
                topic VARCHAR(255) DEFAULT NULL,
                produced_timestamp TIMESTAMP DEFAULT NULL,
                producer_type VARCHAR(255) DEFAULT NULL,
                consumed_timestamp TIMESTAMP DEFAULT NULL,
                consumer_type VARCHAR(255) DEFAULT NULL,
                consumed_count INT DEFAULT 0
            )''')
            self.pg_conn.commit()
                # print(cursor.statusmessage)
        except psycopg2.Error as e:
            print('Error creating db table')
            print("Error connecting to the database:", e)

    def insert_records(self, records:list) -> None:
        cursor = self.pg_conn.cursor()
        for record in records:
            self._insert_record(cursor, record)
        self.pg_conn.commit()

    def _insert_record(self, cursor, record:tuple) -> None:
        # @todo: handling different types of fake data
        # https://www.psycopg.org/docs/cursor.html#cursor.executemany is not more performant
        cursor.execute(
            "INSERT INTO producer (data, topic) VALUES  (%s, %s)",
            record
        )

    def get_faker_data(self, num_to_get:int = 100) -> list:

        # headers = {
        #     "Content-Type": "application/json"
        # }

        data_type = os.getenv('DATA_TYPE')
        data = {
            'data_type': data_type
        }

        # @todo: super bad for now but we need additional request data
        # we will have to search for accounts first in the state tracker
        if data_type == '_banktest_transaction':
            bank_account_ids = self.get_bank_accounts()
            if not bool(bank_account_ids):
                raise Exception("No bank accounts found under the topic _banktest_fund_account in the state tracker")
            data['account_ids'] = json.dumps(bank_account_ids)

        request_url = f'{self.faker_endpoint}/batch/{num_to_get}'

        response = None
        for _ in range(5):
            try:
                response = requests.post(request_url, data=data, timeout=5)
                if response.status_code == 200:
                    break
                print(f"Request to {request_url} resulted in status {response.status_code} and {response.text}")
            except requests.exceptions.RequestException as e:
                print(f"Request to {request_url} failed: {e}")
            time.sleep(1)

        if response is None or response.status_code != 200:
            raise requests.exceptions.RequestException(f"Unable to fetch data from {request_url}")

        # psycopg expects tuples for inserting
        data = response.json()
        records = []
        for record in data:
            record = (json.dumps(record), data_type)
            # record = (item['iban'], item['aba'], item['swift11'], item['bank_country'])
            records.append(record)

        return records

    def get_bank_accounts(self) -> list:
        """
        Get all bank accounts in the state tracker for transaction spamming
        This is needed for bank test workload only
        """
        try:
            cursor = self.pg_conn.cursor()
            cursor.execute("SELECT data FROM producer WHERE topic = '_banktest_fund_account'")
            accounts = cursor.fetchall()

            ibans = []
            for account in accounts:
                _account = json.loads(account[0])
                ibans.append(_account['iban'])
            return ibans
        except psycopg2.Error as e:
            print(f'Error: no bank accounts found with error {e}')
            return []

def check_connection(pg_host, pg_db, pg_user, pg_pass, data_generator_endpoint):
    conn = psycopg2.connect(host=pg_host,database=pg_db,user=pg_user, password=pg_pass)
    if bool(conn):
        print(f'connection to {pg_host} successful')
    else:
        print(f'unable to connect to {pg_host}')

if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "task",
        type=str,
        help="tasks to run",
        choices=[
            "check_data_store",
            "create_schema",
            "fetch_n_save",
            "test"
        ],
        default="fetch_n_save"
    )

    parser.add_argument(
        "--batch_size",
        type=int,
        help="The number of record to generate from the data generator per batch",
        default=500,
    )

    # Make sure we have all the config in the docker-compose
    client_envars = {
        'pg_host': os.getenv('POSTGRES_HOST'),
        'pg_user': os.getenv('POSTGRES_USER'),
        'pg_pass': os.getenv('POSTGRES_PASSWORD'),
        'pg_db': os.getenv('POSTGRES_DB'),
        'data_generator_endpoint': os.getenv('DATA_GENERATOR_ENDPOINT'),
    }

    missing_envar = False
    for envar in client_envars.keys():
        if not bool(client_envars[envar]):
            missing_envar = envar
            break

    if bool(missing_envar):
        raise Exception(f"Missing environment variable {missing_envar} to run the data generator client")

    try:
        args = parser.parse_args()
        if args.task == 'check_data_store':
            check_connection(**client_envars) 
        elif args.task == 'create_schema':
            print('Creating the producer schema on the data store')
            client = faker_pgsql(**client_envars)
            client.create_schema()
        elif args.task == 'fetch_n_save':
            num_to_get = args.batch_size
            print(f"fetching {num_to_get} records from the data generator and saving them")
            client = faker_pgsql(**client_envars)
            client.fetch_and_save(num_to_get)
        elif args.task == 'test':
            client = faker_pgsql(**client_envars)
            print(client.get_bank_accounts())

    except argparse.ArgumentError as e:
        print(f'An error has occured {str(e)}')
