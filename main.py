"""This simple CRUD application performs the following operations sequentially:
    1. Creates 100 new accounts with randomly generated IDs and randomly-computed balance amounts.
    2. Chooses two accounts at random and takes half of the money from the first and deposits it
     into the second.
    3. Chooses five accounts at random and deletes them.
"""

from math import floor
import os
import pandas as pd
import random
import uuid

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy_cockroachdb import run_transaction

from models import Account

# The code below inserts new accounts.


def get_files(path):
    files = []
    with os.scandir(path) as entries:
        for entry in entries:
            if entry.is_file():
                files.append(entry.name)
    return files

def read_file(path):
    with open(path, 'r') as file:
        content = file.read()
    return content

def replace_values(string, replacements):
    for old_value, new_value in replacements.items():
        string = string.replace(old_value, new_value)
    return string



if __name__ == '__main__':
    # For cockroach demo:
    # DATABASE_URL=postgresql://demo:<demo_password>@127.0.0.1:26257?sslmode=require
    # For CockroachCloud:
    user_name = os.environ.get('user_name')
    pass_data = os.environ.get('password_value')
    database_name = os.environ.get('url_value')
    DATABASE_URL=f"cockroachdb://{user_name}:{pass_data}/{database_name}?sslmode=verify-full"
    db_uri = DATABASE_URL
    try:
        engine = create_engine(db_uri, connect_args={"application_name":"docs_simplecrud_sqlalchemy"})
    except Exception as e:
        print("Failed to connect to database.")
        print(f"{e}")

    df_data = pd.DataFrame()
    files = get_files('tables')
    replacements = {
        'capsule': 'cp',
        'carum': 'ca',
        'cumen': 'cm',
        'capgov': 'cg'
    }
    for single_file in files:
        s_table_data = read_file(f'tables/{single_file}')
        new_string = replace_values(s_table_data, replacements)
        s_table_create = new_string.split(',',4)[4].replace('"','')

        with engine.begin() as db:
            db.execute(s_table_create)

    with engine.connect() as db:
        df_data = pd.read_sql(text('show tables from public'), db)
        for single_index, single_row in df_data.iterrows():
            single_table = single_row['table_name']
            print(single_table)
