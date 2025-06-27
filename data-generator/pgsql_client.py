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

import psycopg2, requests, os, argparse

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
        records = self.get_faker_data(num_to_get, 'bank')
        self.insert_records(records)

    def create_schema(self, faker_data:str = 'bank') -> None:
        '''
        Create a schema of different Faker types
        '''
        try:
            cursor = self.pg_conn.cursor()
            # @todo: add more faker data types and maybe serialize the data 
            if faker_data == 'bank':
                cursor.execute('''CREATE TABLE producer (
                    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    iban VARCHAR(50) NOT NULL,
                    aba VARCHAR(50) NOT NULL,
                    swift11 VARCHAR(50) NOT NULL,
                    bank_country VARCHAR(50) NOT NULL,
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

    def insert_records(self, records:list, faker_data = 'bank') -> None:
        cursor = self.pg_conn.cursor()
        for record in records:
            self._insert_record(cursor, record)
        self.pg_conn.commit()

    def _insert_record(self, cursor, record:tuple, faker_data:str = 'bank') -> None:

        # @todo: handling different types of fake data
        # https://www.psycopg.org/docs/cursor.html#cursor.executemany is not more performant
        if faker_data == 'bank':
            cursor.execute(
                "INSERT INTO producer (iban, aba, swift11, bank_country) VALUES  (%s, %s, %s, %s)",
                record
            )

    def get_faker_data(self, num_to_get:int = 100, faker_data:str = 'bank') -> list:

        headers = {
            "Content-Type": "application/json"
        }

        # @todo: adding params for the data type
        request_url = f'{self.faker_endpoint}/batch/{num_to_get}'

        response = requests.get(request_url, headers=headers)

        if response.status_code != 200:
            raise Exception(f"Request to {request_url} resulted in status {response.status_code} and {response.text}")

        # psycopg expects tuples for inserting
        data = response.json()
        records = []
        for item in data:
            record = (item['iban'], item['aba'], item['swift11'], item['bank_country'])
            records.append(record)

        return records

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

    faker_data = 'bank'

    try:
        args = parser.parse_args()
        if args.task == 'check_data_store':
            check_connection(**client_envars) 
        elif args.task == 'create_schema':
            print('Creating the producer schema on the data store')
            client = faker_pgsql(**client_envars)
            client.create_schema(faker_data)
        elif args.task == 'fetch_n_save':
            num_to_get = args.batch_size
            print(f"fetching {num_to_get} records from the data generator and saving them")
            client = faker_pgsql(**client_envars)
            client.fetch_and_save(num_to_get)

    except argparse.ArgumentError as e:
        print(f'An error has occured {str(e)}')
