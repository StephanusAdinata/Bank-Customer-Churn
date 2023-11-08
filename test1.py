#INTRODUCTION
#Nama : Stephanus Adinata Susanto
#Batch : SBY 001
#Objective : Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. Adapun dataset yang dipakai adalah dataset tentang faktor-faktor yang mempengaruhi seseorang terkena sleep disorder


import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os.path

def get_data():
    '''
    Fungsi untuk Import data dari postgres ke python
    '''
    # conn_string="dbname='FTDS' host='localhost' user='postgres' password='sitepo157'"
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn=db.connect(conn_string)

    SQL = '''SELECT "RowNumber", "CustomerId", "Surname", "CreditScore", "Geography", "Gender", "Age",
		"Tenure", "Balance", "NumOfProducts", "HasCrCard", "IsActiveMember", "EstimatedSalary", "Exited" 
        FROM table_m3'''

    df = pd.read_sql(SQL, conn)
    print(df)
    df.to_csv('/opt/airflow/data/P2M3_Stephanus_data_raw.csv', index=False)


def cleaning_data():
    '''
    Fungsi untuk cleaning data
    '''
    df=pd.read_csv('/opt/airflow/data/P2M3_Stephanus_data_raw.csv', index_col=False)
    df1 = df.copy()
    df = df.rename(columns={'RowNumber': 'row_number'})
    df = df.rename(columns={'CustomerId': 'customer_id'})
    df = df.rename(columns={'CreditScore': 'credit_score'})
    df = df.rename(columns={'NumOfProducts': 'num_of_products'})
    df = df.rename(columns={'HasCrCard': 'has_cr_card'})
    df = df.rename(columns={'IsActiveMember': 'is_active_member'})
    df = df.rename(columns={'EstimatedSalary': 'estimated_salary'})
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    df.drop_duplicates()
    df.to_csv('/opt/airflow/data/P2M3_Stephanus_data_clean.csv', index=False)

def import_data(df_clean):
    '''
    Fungsi untuk import data ke Kibana
    '''
    df = pd.read_csv('/opt/airflow/data/P2M3_Stephanus_data_clean.csv', index_col=False)
    es = Elasticsearch("http://localhost:9200")
    es.ping()

    for i,r in df_clean.iterrows():
        doc=r.to_json()
        res=es.index(index="data_m3", body=doc)

def csvToJson():
    df=pd.read_csv('P2M3_Stephanus_data_clean.csv')
    for i,r in df.iterrows():
        print(r['name'])
    df.to_json('fromAirflow.json', orient='records')

default_args = {
    'owner': 'stephanus',
    'start_date': dt.datetime(2023, 11, 3, 30, 0) - dt.timedelta(hours=7),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}


with DAG('m3_stephanus',
         default_args=default_args,
         schedule_interval='30 6 * * *',
         )as dag:

    fetchFromPostgreSQL = PythonOperator(task_id='fetchFromPostgreSQL', python_callable=get_data) 

    # cleaningDataTask = PythonOperator(task_id='cleaningDataTask', python_callable=cleaning_data)

    # postToElasticTask = PythonOperator(task_id='postToElasticTask', python_callable=import_data)

fetchFromPostgreSQL #>> cleaningDataTask >> postToElasticTask