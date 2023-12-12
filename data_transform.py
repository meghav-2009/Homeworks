import pytz
import numpy as np
import pandas as pd
from io import StringIO
from datetime import datetime
from google.cloud import storage
from collections import OrderedDict

mountain_time_zone = pytz.timezone('US/Mountain')


def get_credentials():
    bucket_name = "datacenterlab3"
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
    return credentials_info, bucket_name


def data_from_gcp(credentials_info, gcs_bucket_name):

    gcs_file_path = 'data/{}/outcomes_{}.csv'

    client = storage.Client.from_service_account_info(credentials_info)
    
    bucket = client.get_bucket(gcs_bucket_name)
   
    current_date = datetime.now(mountain_time_zone).strftime('%Y-%m-%d')

    formatted_file_path = gcs_file_path.format(current_date, current_date)
    
    blob = bucket.blob(formatted_file_path)
    csv_data = blob.download_as_text()
    df = pd.read_csv(StringIO(csv_data))

    return df


def transformed_data_to_gcp(dataframe, credentials_info, bucket_name, file_path):
    print(f"Writing data to GCP.....")

    client = storage.Client.from_service_account_info(credentials_info)
    csv_data = dataframe.to_csv(index=False)
    
    bucket = client.get_bucket(bucket_name)
    
    blob = bucket.blob(file_path)
    blob.upload_from_string(csv_data, content_type='text/csv')
    print(f"Finished writing data to GCP.")
    

def preprocess_data(data):
    data['monthyear'] = pd.to_datetime(data['monthyear'])
    data['month'] = data['monthyear'].dt.month
    data['year'] = data['monthyear'].dt.year
    data['sex'] = data['sex_upon_outcome'].replace('Unknown', np.nan)
    data.drop(columns = ['monthyear', 'sex_upon_outcome'], inplace=True)
    mapping = {
    'name': 'animal_name',
    'datetime': 'ts',
    'date_of_birth': 'dob',
    'age_upon_outcome': 'age'
    }
    data.rename(columns=mapping, inplace=True)

    data[['repro_status', 'gender']] = data.sex.str.split(' ', expand=True)
    data.drop(columns = ['sex'], inplace=True)

    data.fillna('unknown', inplace = True)

    return data


def dimensionalmodeling():
    credentials_info, bucket_name = get_credentials()

    raw_data = data_from_gcp(credentials_info, bucket_name)
    
    transformed_data = preprocess_data(raw_data)
    
    data_time_dim = transformed_data[['ts','month','year']].drop_duplicates()
    data_time_dim['timekey'] = range(1, len(data_time_dim)+1 )

    data_animal_dim = transformed_data[['animal_id','animal_name','dob','animal_type','breed','color','repro_status','gender']].drop_duplicates()
    data_animal_dim['animalkey'] = range(1, len(data_animal_dim)+1 )

    data_outcome_dim = transformed_data[['outcome_type']].drop_duplicates()
    data_outcome_dim['outcomekey'] = range(1, len(data_outcome_dim)+1 )

    data_combined = transformed_data.merge(data_animal_dim, how = 'inner', left_on = 'animal_id', right_on = 'animal_id')
    data_combined = data_combined.merge(data_time_dim, how = 'inner', left_on = 'ts', right_on = 'ts')
    data_combined = data_combined.merge(data_outcome_dim, how = 'inner', left_on = 'outcome_type', right_on = 'outcome_type')

    data_animal_fact = data_combined[['age','animalkey','outcomekey','timekey']]

    dim_animal_path = "transformed_data/data_animal_dim.csv"
    dim_dates_path = "transformed_data/data_time_dim.csv"
    dim_outcome_types_path = "transformed_data/data_outcome_dim.csv"
    fct_outcomes_path = "transformed_data/data_animal_fact.csv"

    transformed_data_to_gcp(data_animal_dim, credentials_info, bucket_name, dim_animal_path)
    transformed_data_to_gcp(data_time_dim, credentials_info, bucket_name, dim_dates_path)
    transformed_data_to_gcp(data_outcome_dim, credentials_info, bucket_name, dim_outcome_types_path)
    transformed_data_to_gcp(data_animal_fact, credentials_info, bucket_name, fct_outcomes_path)