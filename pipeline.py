#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np
import argparse 
from sqlalchemy import create_engine

def extract_data(source):
    return pd.read_csv(source)

def transform_data(data):
    new_data = data.copy()
    new_data[['month', 'year']] = new_data.MonthYear.str.split(' ', expand=True)
    new_data['sex'] = new_data['Sex upon Outcome'].replace('Unknown', np.nan)
    new_data.drop(columns = ['MonthYear', 'Sex upon Outcome'], inplace=True)
    mapping = {
    'Animal ID': 'animal_id',
    'Name': 'animal_name',
    'DateTime': 'ts',
    'Date of Birth': 'dob',
    'Outcome Type': 'outcome_type',
    'Outcome Subtype': 'outcome_subtype',
    'Animal Type': 'animal_type',
    'Age upon Outcome': 'age',
    'Breed': 'breed',
    'Color': 'color'
    }
    new_data.rename(columns=mapping, inplace=True)
    new_data[['repro_status', 'gender']] = new_data.sex.str.split(' ', expand=True)
    new_data.drop(columns = ['sex'], inplace=True)

    new_data.fillna('unknown', inplace = True)

    return new_data

def load_animal_dim(data):
    data_animal_dim = data[['animal_id','animal_name','dob','age','animal_type','breed','color','repro_status','gender']]
    data_animal_dim['animalkey'] = range(1, len(data_animal_dim)+1 )
    return data_animal_dim


def load_time_dim(data):
    data_time_dim = data[['ts','month','year']]
    data_time_dim['timekey'] = range(1, len(data_time_dim)+1 )
    return data_time_dim
    

def load_outcome_dim(data):
    data_outcome_dim = data[['outcome_type','outcome_subtype']].drop_duplicates()
    data_outcome_dim['outcomekey'] = range(1, len(data_outcome_dim)+1 )
    return data_outcome_dim
    


def load_data(data):
    db_url = "postgresql+psycopg2://megha:megha2009@db:5432/shelter"
    conn = create_engine(db_url)
    
    data_animal_dim = load_animal_dim(data)
    data_animal_dim.to_sql("animaldimension", conn, if_exists="append", index = False)
    
    data_time_dim = load_time_dim(data)
    data_time_dim.to_sql("timedimension", conn, if_exists="append", index = False)
    
    data_outcome_dim = load_outcome_dim(data)
    data_outcome_dim.to_sql("outcomedimension", conn, if_exists="append", index = False)

    data_combined = data.merge(data_animal_dim, how = 'inner', left_on = 'animal_id', right_on = 'animal_id')
    data_combined = data_combined.merge(data_time_dim, how = 'inner', left_on = 'ts', right_on = 'ts')
    data_combined = data_combined.merge(data_outcome_dim, how = 'inner', left_on = ['outcome_type','outcome_subtype'], right_on = ['outcome_type','outcome_subtype'])

    data_animal_fact = data_combined[['animalkey','outcomekey','timekey']]
    data_animal_fact.to_sql("animalfact", conn, if_exists="append", index = False)  




if __name__ == "__main__": 
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='source csv')
#    parser.add_argument('target', help='target csv')
    args = parser.parse_args()

    print("Starting...")
    df = extract_data(args.source)
    new_df = transform_data(df)
    load_data(new_df)
    print("Complete")