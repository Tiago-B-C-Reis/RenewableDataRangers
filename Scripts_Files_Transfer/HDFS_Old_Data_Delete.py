import subprocess
import logging


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


def check_hdfs_directory_exists(directory):
    check_command = ['hadoop', 'fs', '-test', '-e', directory]
    try:
        subprocess.run(check_command, check=True)
        return True
    except subprocess.CalledProcessError:
        return False


def execute_hadoop_delete_command():
    hadoop_command = [
        'hdfs', 'dfs', '-rm', '-r', '/user/Bronze_Layer'
    ]
    run_command(hadoop_command, success_msg="Hadoop data delete command executed successfully.",
                error_msg="Error executing Hadoop delete command")


def main():
    # Step-by-step execution
    try:
        if check_hdfs_directory_exists('/user/Bronze_Layer'):
            execute_hadoop_delete_command()
        else:
            logger.info("/user/Bronze_Layer does not exists in HDFS. Skipping Hadoop copy command.")
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
    else:
        logger.info("Script execution completed successfully.")


if __name__ == "__main__":
    main()

