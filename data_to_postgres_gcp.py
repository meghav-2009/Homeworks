import psycopg2
import pandas as pd
from io import StringIO
from google.cloud import storage
from sqlalchemy import create_engine


class GCPTransformedData:

    def __init__(self):
        self.bucket_name = 'datacenterlab3'

    def get_credentials(self):
        credentials_info = {
             "type": "service_account",
             "project_id": "dazzling-decker-406202",
             "private_key_id": "f70ecb9de1b4388e93b8f51717be98747ad1216b",
             "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDKhDfq5xGzV2v0\njIxDTVhs5me/LmfhR9uY4e7JnCJFubnooWale4d1nQWte1p+YV1cwqDOZZFr+reU\nQXV5it28ITwOeD0Z9U3avUI+qjjGE4a9t1c4eIGpnGrHAzzHB3vyLJvTJgpOavbz\nq7/qlLGphytyNVL9MK9rLt8JFviXL/MS/DIMLAQJzfAs6tm0M65UZ9G0yAtII2w+\nZoWRcApw2oTqmS0TElQol28WwqJVszrpQKSnXJYoWNIDEQ1S58UVRqIgQEyPjmG2\nZ0OlXZPrg3L9XjrBO4zzrAcPjjSrGT+e7PcT22LHp50qq/tuJVJFX7dA+oPG15oy\nfQEbCTzLAgMBAAECggEAERabCU06JyHnUBlT9JHgNkAy3OTziwTfOMVHuudb1PpY\n/5Iu8SO9SGVZh0vzVNquYBdkLkZPwQbE2tOzL/BRyYAcazQmI7yZNy3lGxiN9djL\n89I2n+M7Pa6fK+/P/xNYmBS/iU+aaweHkwQZC5hEbYOkXNy3BfpHswmfdErl6wvQ\nSNByK8YvKy/A1VQFwQ6eYLB0cmOLYRiWXJJZooWO7Rz87qxCWuwjhN7gK3GJ5Ar9\nTTBXrelLSTfO9xAJRMqjry0QDQZtffK/ayyhZXJcLgi8BXjWi4vxvCHua+PlHExb\nBKwRALTEwl/XCAxkc0k9NBeBHLvlAev08b1U51zBwQKBgQDsgYyGZeRG9nxM9wC0\nSgdNlqflMacBnh14f/6mNMRxM9JMFlu1zo4gu0MiNMwdr9YrEHSdZuc2LMTI6Rt+\nIxx6UZo4tywk4/Bkq4eKo4wfSSX5YTuGDH+pHlCvDx0NjaF/W4jp0p5COUQqvA1X\neLiHBoyqG1bUQjkSFowz0s7d8QKBgQDbNXbwbj/TO40TpwPVvasndmuGiT147DlF\nErfzIpBPsbG+oaIPJXouYXXErI3tShH1c535EwvdOk0DgYQ4BmmXhs9aOOly7OEM\nySK6fCKrtGCHHi3QveCWVeGpBcweT30nFB8u0+fJqGCatk7O7vVV4KG9a2cNx2vw\nj2HpfRw6ewKBgQDSBm2bgGqLFiLmWkc207LOlSZ+I2zSw+4Z71hdkuiMOt/bD9Pb\njRWsPX/tpYNKLbd8QL2+df6HnpQWcuQmTNniSgsxqIC8NetqCuVwGbK6qcPeRRmN\n/hV0QuaVv08mlQrAxbG9K3u9BYvig2M5tYvw5MEP4v1lu3Vg+cMB1og8kQKBgQCc\nmOspA3mxCx/TZELHFMIKZPyXlb9GfWrEP4UNuilh5U1XI9zD+T/5lkItiB+z3nBJ\n6ph18PdOyXFvCb1M0LfqObIzf+i14yc6nQ2kLz4Nb8Rtgk+0iZDBlQSqXKvfE2YS\n3rsQFu3FD/ZDT+2owuicuuaQjQOHwmkH50ZJKMlbtQKBgQCPZ0Gfw9B+AixLs8H6\nIFO4SKtF4vnOkvvTl4Cy9QCIznphPIxcQuNCk+sH4SoxR9iPls8jh0plJcpB4qtz\n2iAKqyw/+LRbXMiFoeXsNz65cri7e7v35CUlO99Ph55h6npEne0WnqkO3eu0/ofp\nuCztjMg9WJ28pgPWWsgRm1Jxdw==\n-----END PRIVATE KEY-----\n",
             "client_email": "megha123@dazzling-decker-406202.iam.gserviceaccount.com",
             "client_id": "118408094597469376584",
             "auth_uri": "https://accounts.google.com/o/oauth2/auth",
             "token_uri": "https://oauth2.googleapis.com/token",
             "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
             "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/megha123%40dazzling-decker-406202.iam.gserviceaccount.com",
             "universe_domain": "googleapis.com"
            }
        return credentials_info

    def transformed_data_from_gcp(self, file_name):
        gcs_file_path = f'transformed_data/{file_name}'

        credentials_info = self.get_credentials()
        client = storage.Client.from_service_account_info(credentials_info)
        bucket = client.get_bucket(self.bucket_name)

        blob = bucket.blob(gcs_file_path)
        csv_data = blob.download_as_text()
        df = pd.read_csv(StringIO(csv_data))

        return df



class data_load_to_postgres:

    def __init__(self):
        self.db_config = {
            'dbname': 'postgres',
            'user': 'postgres',
            'password': 'postgres',
            'host': '34.121.243.233',
            'port': '5432',
        }

    def get_queries(self, table_name):
        if table_name =="data_animal_dim":
            query = """CREATE TABLE IF NOT EXISTS data_animal_dim (
                            animalkey INT PRIMARY KEY,
                            animal_id VARCHAR,
                            animal_name VARCHAR,
                            dob DATE,
                            animal_type VARCHAR,
                            breed VARCHAR,
                            color VARCHAR,
                            repro_status VARCHAR,
                            gender VARCHAR
                        );
                        """
        elif table_name =="data_outcome_dim":
            query = """CREATE TABLE IF NOT EXISTS data_outcome_dim (
                            outcomekey INT PRIMARY KEY,
                            outcome_type VARCHAR
                        );
                        """
        elif table_name =="data_time_dim":
            query = """CREATE TABLE IF NOT EXISTS data_time_dim (
                            timekey INT PRIMARY KEY, 
                            ts TIMESTAMP, 
                            month INT, 
                            year INT
                        );
                        """
        else:
            query = """CREATE TABLE IF NOT EXISTS data_animal_fact (
                            main_pk SERIAL PRIMARY KEY, 
                            age VARCHAR, 
                            animalkey INT REFERENCES data_animal_dim(animalkey), 
                            outcomekey INT REFERENCES data_outcome_dim(outcomekey), 
                            timekey INT REFERENCES data_time_dim(timekey)
                        );
                        """
        return query

    def connect_to_postgres(self):
        connection = psycopg2.connect(**self.db_config)
        return connection

    def create_table(self, connection, table_query):
        print("Executing Create Table Queries...")
        cursor = connection.cursor()
        cursor.execute(table_query)
        connection.commit()
        cursor.close()
        print("Finished creating tables...")

    def load_data_into_postgres(self, connection, gcs_data, table_name):
        cursor = connection.cursor()
        print(f"Dropping Table {table_name}")
        drop_table = f"DROP TABLE {table_name};"
        cursor.execute(drop_table)
        connection.commit()
        cursor.close()
        
        print(f"Loading data into PostgreSQL for table {table_name}")
        
        engine = create_engine(
            f"postgresql+psycopg2://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}"
        )

        gcs_data.to_sql(table_name, engine, if_exists='replace', index=False)

        print(f"Table insertion completed - {table_name}")
        

def load_data_to_postgres_main(file_name, table_name):
    gcp_loader = GCPTransformedData()
    table_data = gcp_loader.transformed_data_from_gcp(file_name)

    postgres_dataloader = data_load_to_postgres()
    table_query = postgres_dataloader.get_queries(table_name)
    postgres_connection = postgres_dataloader.connect_to_postgres()

    postgres_dataloader.create_table(postgres_connection, table_query)
    postgres_dataloader.load_data_into_postgres(postgres_connection, table_data, table_name)
