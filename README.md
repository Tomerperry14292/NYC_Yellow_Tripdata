Assumptions:
1) The data that will be fetched will be only from 2023.

In order to run the project, You need to run Airflow locally on your computer:
1) Open the terminal and install aieflow by running the next command - pip install apache-airflow
2) Initialize the Airflow Database: Airflow requires a database to store its metadata. Initialize the Airflow database by       running the following command: airflow db init
3) Create an Admin User: After installing the necessary dependencies, you can create an admin user by running the following command: airflow users create \
    --username admin \
    --password 1234 \
    --firstname admin\
    --lastname admin \
    --role Admin \
    --email youremail@gmail.com
4) Start the Airflow Web Server: Start the Airflow web server using the following command: 
airflow webserver --port 8080
5) Start the Airflow scheduler using the following command: airflow scheduler
6) In Chrome put the next URL - http://localhost:8080/
7) Put the username and password that you used in the admin user command (in our case - username: admin, password: 1234)
8) Create a new folder name "dags" under the airflow folder in your computer - insert into this folder my project files:

  a)test_dag.py
  
  b)yellow_tripdata_2023-01.parquet 
  
  c)yellow_tripdata_2023-02.parquet
  
  d)yellow_tripdata_2023-03.parquet
  
9) Run the Airflow DAG name "yellow_taxi_trip_data" and feel free to look at the logs of the python opertor name "fetch_trip_data" and see the results.  
