from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from dotenv import load_dotenv
import os

# Define default arguments
default_args = {
    'start_date': datetime(2024, 6, 1),
    'catchup': False,
}

# Instantiate the DAG object
with DAG('Gold_Layer', default_args=default_args, schedule_interval='@daily') as dag:

    def execute_script():
        # Your existing script goes here
        # Make sure to adjust the paths and configurations for Airflow environment
        # Also, ensure the necessary Spark configurations are set for Airflow
        # You may need to replace local paths with Airflow paths or modify accordingly
        pass

    # Define PythonOperator to execute the script
    execute_script_task = PythonOperator(
        task_id='execute_script',
        python_callable=execute_script,
    )

    # Define the task dependencies
    execute_script_task  # You may add other tasks and dependencies here as needed
