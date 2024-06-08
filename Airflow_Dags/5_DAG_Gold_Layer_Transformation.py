from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.subprocess import SubprocessHook
from airflow.hooks.filesystem import FSHook
from datetime import datetime
import logging
import os
import pandas as pd


# Define the DAG
default_args = {
    'start_date': datetime(2024, 6, 1),
    'catchup': False,
}


with DAG('Gold_Layer', default_args=default_args, schedule_interval='@daily') as dag: