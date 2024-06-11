from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.subprocess import SubprocessHook
from airflow.hooks.filesystem import FSHook
from datetime import datetime
import logging
import os
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_command(command, success_msg, error_msg):
    hook = SubprocessHook()
    try:
        result = hook.run_command(command)
        logger.info(success_msg)
        logger.debug(f"Command output: {result.output}")
    except Exception as e:
        logger.error(f"{error_msg}: {str(e)}")
        raise


def copy_files_from_docker_to_local():
    tmp_Data_hook = FSHook(conn_id='tmp_Data')
    local_directory = tmp_Data_hook.get_path()

    # Ensure the local directory exists
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)
        logger.info(f"Created directory {local_directory}")

    airbyte_command = [
        "docker", "cp",
        "airbyte-server:/tmp/airbyte_local/.",
        local_directory
    ]
    run_command(
        airbyte_command,
        success_msg="Files copied from AirByte Docker to local successfully.",
        error_msg="Error copying files from Docker to local"
    )


def rename_file():
    tmp_Data_hook = FSHook(conn_id='tmp_Data')
    in_local_directory = tmp_Data_hook.get_path()

    source_file_path = [
        os.path.join(in_local_directory, "_airbyte_raw_commodity.jsonl"),
        os.path.join(in_local_directory, "_airbyte_raw_transaction.jsonl"),
        os.path.join(in_local_directory, "_airbyte_raw_electricity.jsonl"),
        os.path.join(in_local_directory, "_airbyte_raw_iso.jsonl")
    ]
    renamed_file_path = [
        os.path.join(in_local_directory, "airbyte_raw_commodity.json"),
        os.path.join(in_local_directory, "airbyte_raw_transaction.json"),
        os.path.join(in_local_directory, "airbyte_raw_electricity.json"),
        os.path.join(in_local_directory, "airbyte_raw_iso.json")
    ]

    if all(os.path.exists(src) for src in source_file_path):
        for source_name, destination_name in zip(source_file_path, renamed_file_path):
            mv_command = ['mv', source_name, destination_name]
            run_command(mv_command, success_msg=f"{source_name} file renamed successfully.",
                        error_msg=f"Error renaming {source_name} file")
    else:
        logger.error(f"One or more source files do not exist: {source_file_path}")


def process_json_to_csv():
    tmp_Data_hook = FSHook(conn_id='tmp_Data')
    in_local_directory = tmp_Data_hook.get_path()
    bronze_data_hook = FSHook(conn_id='Bronze_Layer')
    out_local_directory = bronze_data_hook.get_path()

    renamed_file_path = [
        os.path.join(in_local_directory, "airbyte_raw_commodity.json"),
        os.path.join(in_local_directory, "airbyte_raw_transaction.json"),
        os.path.join(in_local_directory, "airbyte_raw_electricity.json"),
        os.path.join(in_local_directory, "airbyte_raw_iso.json")
    ]
    csv_file_path = [
        os.path.join(out_local_directory, "commodity.csv"),
        os.path.join(out_local_directory, "transaction.csv"),
        os.path.join(out_local_directory, "electricity.csv"),
        os.path.join(out_local_directory, "iso.csv")
    ]

    for json_file, csv_file in zip(renamed_file_path, csv_file_path):
        try:
            # Check if the file is empty
            if os.path.getsize(json_file) == 0:
                logger.error(f"File {json_file} is empty. Skipping.")
                continue

            # Read the JSON file
            df = pd.read_json(json_file, lines=True)

            # Check if the dataframe is empty
            if df.empty:
                logger.error(f"No data found in {json_file}.")
                continue

            # Process the dataframe and save to CSV
            df_data = df['_airbyte_data'].apply(pd.Series)
            df_data.to_csv(csv_file, index=False)
            logger.info(f"Data from {json_file} processed and saved to {csv_file}.")
        except ValueError as ve:
            logger.error(f"ValueError processing {json_file}: {str(ve)}")
        except Exception as e:
            logger.error(f"Error processing {json_file} to {csv_file}: {str(e)}")
            raise


def execute_hadoop_copy_command():
    bronze_layer_hook = FSHook(conn_id='Bronze_Layer')
    bronze_layer_directory = bronze_layer_hook.get_path()

    hadoop_command = [
        'hadoop', 'fs', '-copyFromLocal',
        os.path.join(bronze_layer_directory, '.'),
        '/user/'
    ]
    run_command(hadoop_command, success_msg="Hadoop copy command executed successfully.",
                error_msg="Error executing Hadoop copy command")


# Define the DAG
default_args = {
    'start_date': datetime(2024, 6, 1),
    'catchup': False,
}


with DAG('AirByte_to_Hadoop', default_args=default_args, schedule_interval='@daily') as dag:
    t1 = PythonOperator(
        task_id='copy_files_from_docker_to_local',
        python_callable=copy_files_from_docker_to_local
    )

    t2 = PythonOperator(
        task_id='rename_file',
        python_callable=rename_file
    )

    t3 = PythonOperator(
        task_id='process_json_to_csv',
        python_callable=process_json_to_csv
    )

    t4 = PythonOperator(
        task_id='execute_hadoop_copy_command',
        python_callable=execute_hadoop_copy_command
    )

    # Define the task dependencies
    t1 >> t2 >> t3 >> t4
