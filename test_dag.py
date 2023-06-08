from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import os 
import pyarrow.parquet as pq
import pandas as pd
import sqlite3
import logging

log = logging.getLogger(__name__)

def fetch_trip_data():
    #Find the current path in order to extract the parquet files
    current_path = os.path.abspath(__file__)
    df = pd.DataFrame()
    months_list = ["2023-01","2023-02","2023-03"]
    dataframes_list = []
    #Create list of files path for each file
    file_path_list = [f"{'/'.join(current_path.split('/')[:-1])}/yellow_tripdata_{month}.parquet" for month in months_list]
    try:
        #For each file fetch the data from the parquet file and load it into a dataframe
        for file_path in file_path_list:
            table = pq.read_table(file_path)
            month_df = table.to_pandas()
            dataframes_list.append(month_df)
        #Create a union data with all of the months   
        df = pd.concat(dataframes_list)  
        #Keep only the fields that relevant for calaculate the amount of passengers for each month  
        df = df[["tpep_pickup_datetime","passenger_count"]]
        #Remove records with null values in the datetime field
        mask = df["tpep_pickup_datetime"].notna()
        df = df[mask]
        #Fill null passenger_count values with 0
        df["passenger_count"] = df["passenger_count"].fillna(0)
        df["passenger_count"] = df["passenger_count"].astype('int64')
        #Create new DB connection in sqlite3
        con = sqlite3.connect('yellow_tripdata.db')

        # Export the DataFrame to the SQLite database table
        df.to_sql('yellow_tripdata_raw_data', con, if_exists='replace')
        c = con.cursor()
        # Running SQL query in order to create an SQL aggregation table that counts the number of passengers by month
        c.execute("""select strftime('%Y-%m', tpep_pickup_datetime) AS month,sum(passenger_count) 
                     from yellow_tripdata_raw_data
                     group by strftime('%Y-%m', tpep_pickup_datetime)
                     limit 20""")
        print(c.fetchall())
        con.commit()
        con.close()
    except Exception as e:
        log.info(f"Error Type:{type(e)},Error Content:{str(e)}")    


with DAG("yellow_taxi_trip_data", start_date=datetime(2023, 1, 1), schedule_interval="0 0 * * *") as dag:
    start = DummyOperator(task_id='start',dag=dag)
    fetch_data = PythonOperator(task_id="fetch_trip_data", python_callable=fetch_trip_data)
    end = DummyOperator(task_id='end',dag=dag)

    start >> fetch_data >> end
    