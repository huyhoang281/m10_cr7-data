import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
import creds


# task for cleaning CR7 data
def clean_cr7():
    read_path = '/mnt/c/Users/PC/Downloads/datamodel/'
    file = 'cr7.csv'
    # loading raw data from the file
    CR7 = pd.read_csv(read_path + file)
    # Cleaning and arranging the data
    CR7['Playing_Position'].replace(['LW ', 'CF '], ['LW', 'CF'], inplace=True)
    CR7['Playing_Position'].fillna(method='ffill', inplace=True)
    CR7['Type'].fillna(method='bfill', inplace=True)
    CR7['Goal_assist'].fillna('Cristiano Ronaldo', inplace=True)

    CR7['Date'] = pd.to_datetime(CR7['Date'])
    CR7['Minute'].replace(['90+5', '45+1', '90+3', '90+2', '90+1', '90+4', '90+6', '90+7', '45+7', '45+2'],
                          ['95', '46', '93', '92', '91', '94', '96', '97', '52', '47'], inplace=True)

    # storing the cleaned data into a new file
    output_path = '/mnt/c/Users/PC/Downloads/datamodel/cleaned_data/'
    CR7.to_csv(output_path + file, index=False)


# task for cleaning messi data
def clean_messi():
    read_path = '/mnt/c/Users/PC/Downloads/datamodel/'
    file = 'messi.csv'
    # loading raw data from the file
    messi = pd.read_csv(read_path + file)
    messi['Type'].fillna(method='bfill', inplace=True)
    messi['Goal_assist'].fillna('Lionel Messi', inplace=True)

    messi['Date'] = pd.to_datetime(messi['Date'])
    messi['Minute'].replace(['90+1', '90+2', '90+3', '45+2', '45+1', '90+4', '45+4'],
                            ['91', '92', '93', '47', '46', '94', '49'],
                            inplace=True)

    # storing the new cleaned data into a new file 
    output_path = '/mnt/c/Users/PC/Downloads/datamodel/cleaned_data/'
    messi.to_csv(output_path + file, index=False)


# Creating the database
def create_database_tables_insert_values():
    import psycopg2
    # connect to default database
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=" + creds.postgres_pass)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    cur.execute('DROP DATABASE IF EXISTS ronaldomessi')
    cur.execute('CREATE DATABASE ronaldomessi')


    conn.close()


    conn = psycopg2.connect("host=localhost dbname=ronaldomessi user=postgres password=" + creds.postgres_pass)
    cur = conn.cursor()

    ronaldo_data_table_create = ("""CREATE TABLE IF NOT EXISTS ronaldo(
                               Season VARCHAR ,
                               Competition VARCHAR,
                               Matchday VARCHAR,
                               Date DATE,
                               Venue VARCHAR,
                               Club VARCHAR,
                               Opponent VARCHAR, 
                               Result VARCHAR,
                               Playing_Position VARCHAR,
                               Minute VARCHAR,
                               At_score VARCHAR,
                               Type VARCHAR,
                               Goal_assist VARCHAR
                               )""")
    cur.execute(ronaldo_data_table_create)
    conn.commit()

    messi_data_table_create = ("""CREATE TABLE IF NOT EXISTS messi(
                               Season VARCHAR ,
                               Competition VARCHAR,
                               Matchday VARCHAR,
                               Date DATE,
                               Venue VARCHAR,
                               Club VARCHAR,
                               Opponent VARCHAR, 
                               Result VARCHAR,
                               Playing_Position VARCHAR,
                               Minute VARCHAR,
                               At_score VARCHAR,
                               Type VARCHAR,
                               Goal_assist VARCHAR
                               )""")
    cur.execute(messi_data_table_create)
    conn.commit()

    ronaldo_data_table_insert = ("""INSERT INTO ronaldo(
                               Season  ,
                               Competition ,
                               Matchday ,
                               Date ,
                               Venue ,
                               Club ,
                               Opponent , 
                               Result ,
                               Playing_Position ,
                               Minute ,
                               At_score ,
                               Type ,
                               Goal_assist)
                               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                               """)
    file = 'cr7.csv'
    output_path = '/mnt/c/Users/PC/Downloads/datamodel/cleaned_data/'
    ronaldo_df = pd.read_csv(output_path + file)
    for i, row in ronaldo_df.iterrows():
        cur.execute(ronaldo_data_table_insert, list(row))
    conn.commit()

    messi_data_table_insert = ("""INSERT INTO messi(
                               Season  ,
                               Competition ,
                               Matchday ,
                               Date ,
                               Venue ,
                               Club ,
                               Opponent , 
                               Result ,
                               Playing_Position ,
                               Minute ,
                               At_score ,
                               Type ,
                               Goal_assist)
                               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                               """)
    file = 'messi.csv'
    output_path = '/mnt/c/Users/PC/Downloads/datamodel/cleaned_data/'
    messi_df = pd.read_csv(output_path + file)
    for i, row in messi_df.iterrows():
        cur.execute(messi_data_table_insert, list(row))
    conn.commit()


#create DAG 
default_dag_args = {
    'start_date': datetime(2023, 3, 10),  
    'email_on_failure': False,  
    'email_on_retry': False,  
    'retries': 1,  
    'retry_delay': timedelta(minutes=5),  # delay before doing another retry
    'project_id': 1
}

with DAG('cr7_messi_data_processing', schedule_interval='@daily', catchup=False,
         default_args=default_dag_args) as cr7messi_dag:
    task_0 = PythonOperator(task_id='cleaning_cr7_data', python_callable=clean_cr7)
    task_1 = PythonOperator(task_id='clean_messi_data', python_callable=clean_messi)
    task_2 = PythonOperator(task_id='create_database_table_insert_values',
                            python_callable=create_database_tables_insert_values)
    task_3 = BashOperator(task_id='moving_cr7_data', bash_command="cp /mnt/c/Users/PC/Downloads/datamodel/cleaned_data/cr7.csv  \
                          /mnt/c/Users/PC/Downloads/Rondaldo_Messi_Project")
    task_4 = BashOperator(task_id='moving_messi_data', bash_command="cp /mnt/c/Users/PC/Downloads/datamodel/cleaned_data/messi.csv  \
                          /mnt/c/Users/PC/Downloads/Rondaldo_Messi_Project")
    task_5 = BashOperator(task_id='moving_code_file', bash_command="cp /mnt/c/Users/PC/Downloads/airflow/dags/cr7_messi.py  \
                          /mnt/c/Users/PC/Downloads/Rondaldo_Messi_Project")

    task_0 >> task_1 >> task_2
    task_2 >> task_3
    task_2 >> task_4
    task_2 >> task_5
