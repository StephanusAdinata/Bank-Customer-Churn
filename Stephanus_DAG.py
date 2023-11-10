'''
=================================================
INTRODUCTION
Nama : Stephanus Adinata Susanto
Batch : SBY 001
Objective : Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke Elasticsearch dan menggunakan DAG Airflow. Adapun dataset yang dipakai adalah dataset tentang faktor-faktor yang mempengaruhi customer churn pada nasabah bank di Eropa
=================================================
'''

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
    Fungsi for Import data from postgres to python 
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
    Function for cleaning data
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

def import_data():
    '''
    Function for import data to Kibana
    '''
    df = pd.read_csv('/opt/airflow/data/P2M3_Stephanus_data_clean.csv', index_col=False)
    es = Elasticsearch("http://localhost:9200")
    es.ping()

    for i,r in df.iterrows():
        doc=r.to_csv()
        res=es.index(index="data_m3", body=doc)

def csvToJson():
    '''
    Function for DAG airflow
    '''
    df=pd.read_csv('P2M3_Stephanus_data_clean.csv')
    for i,r in df.iterrows():
        print(r['name'])
    df.to_csv('fromAirflow.json', orient='records')

default_args = {
    'owner': 'stephanus',
    'start_date': dt.datetime(2023, 11, 3, 15, 30, 0) - dt.timedelta(hours=7),
    # 'retries': 1,
    # 'retry_delay': dt.timedelta(minutes=5),
}


with DAG('m3_stephanus1',
         default_args=default_args,
         schedule_interval='30 6 * * *', #scheduled at 6.30
         )as dag:

    getdataFromPostgreSQL = PythonOperator(task_id='getdataFromPostgreSQL', python_callable=get_data) 

    cleaningdata = PythonOperator(task_id='cleaningdata', python_callable=cleaning_data)

    importoelastic = PythonOperator(task_id='importoelastic', python_callable=import_data)

getdataFromPostgreSQL >> cleaningdata >> importoelastic 