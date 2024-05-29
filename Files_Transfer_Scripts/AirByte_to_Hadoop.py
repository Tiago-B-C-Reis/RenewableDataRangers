import subprocess
import logging
import shutil
import pandas as pd
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_command(command, success_msg, error_msg):
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        logger.info(success_msg)
        logger.debug(f"Command output: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"{error_msg}: {e.output}")
        raise


def copy_files_from_docker_to_local():
    airbyte_command = [
        "docker", "cp",
        "airbyte-server:/tmp/airbyte_local/.",
        "/Users/tiagoreis/Downloads/Air_Byte_Outp/In/"
    ]
    run_command(airbyte_command, success_msg="Files copied from AirByte Docker to local successfully.",
                error_msg="Error copying files from Docker to local")


def rename_file(source, destination):
    if all(os.path.exists(src) for src in source):
        for source_name, destination_name in zip(source, destination):
            mv_command = ['mv', source_name, destination_name]
            run_command(mv_command, success_msg=f"{source_name} file renamed successfully.",
                        error_msg=f"Error renaming {source_name} file")
    else:
        logger.error(f"One or more source files do not exist: {source}")


def process_json_to_csv(json_file_path, csv_file_path):
    for json, csv in zip(json_file_path, csv_file_path):
        df = pd.read_json(json, lines=True)
        df_data = df['_airbyte_data'].apply(pd.Series)
        df_data.to_csv(csv, index=False)
        logger.info(f"Data from {json} processed and saved to {csv}.")


def copy_files_to_hadoop_docker():
    docker_cp_to_hadoop_command = [
        "docker", "cp",
        f"/Users/tiagoreis/Downloads/Air_Byte_Outp/Out/.",
        "namenode:/tmp/Data/"
    ]
    run_command(docker_cp_to_hadoop_command, success_msg="Files copied from local to Hadoop/temp Docker successfully.",
                error_msg="Error copying files from local to Hadoop/temp Docker")


def execute_hadoop_copy_command():
    hadoop_command = [
        'docker', 'exec', 'namenode', 'hadoop', 'fs', '-copyFromLocal',
        f'/tmp/Data/.', '/user/Bronze_Layer'
    ]
    run_command(hadoop_command, success_msg="Hadoop copy command executed successfully.",
                error_msg="Error executing Hadoop copy command")


def main():
    # Define file paths
    in_local_directory = "/Users/tiagoreis/Downloads/Air_Byte_Outp/In/"
    out_local_directory = "/Users/tiagoreis/Downloads/Air_Byte_Outp/Out/"

    # Ensure the output directory exists
    os.makedirs(out_local_directory, exist_ok=True)

    source_file_path = [
        os.path.join(in_local_directory, "_airbyte_raw_codes.jsonl"),
        os.path.join(in_local_directory, "_airbyte_raw_electricity.jsonl"),
        os.path.join(in_local_directory, "_airbyte_raw_iso.jsonl")
    ]
    renamed_file_path = [
        os.path.join(in_local_directory, "airbyte_raw_codes.json"),
        os.path.join(in_local_directory, "airbyte_raw_electricity.json"),
        os.path.join(in_local_directory, "airbyte_raw_iso.json")
    ]
    csv_file_path = [
        os.path.join(out_local_directory, "code.csv"),
        os.path.join(out_local_directory, "electricity.csv"),
        os.path.join(out_local_directory, "iso.csv")
    ]

    # Step-by-step execution
    try:
        copy_files_from_docker_to_local()
        rename_file(source_file_path, renamed_file_path)
        process_json_to_csv(renamed_file_path, csv_file_path)
        copy_files_to_hadoop_docker()
        execute_hadoop_copy_command()
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
    else:
        logger.info("Script execution completed successfully.")


if __name__ == "__main__":
    main()