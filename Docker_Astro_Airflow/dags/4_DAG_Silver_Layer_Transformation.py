from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dotenv import load_dotenv
import os

# Configure logging
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
RDS_PSQL_SERVER = os.getenv("RDS_PSQL_SERVER")
RDS_USER = os.getenv("RDS_USER")
RDS_PASS = os.getenv("RDS_PASS")

def create_spark_session():
    return SparkSession.builder \
        .config("spark.jars", "/path/to/postgresql-42.7.3.jar") \
        .master("local") \
        .appName("Silver_Layer_Transformations") \
        .getOrCreate()

def read_and_write_data():
    spark = create_spark_session()

    # File paths
    transaction_file_path = "hdfs://localhost:9000/user/Bronze_Layer/transaction.csv"
    commodity_file_path = "hdfs://localhost:9000/user/Bronze_Layer/commodity.csv"
    electricity_file_path = "hdfs://localhost:9000/user/Bronze_Layer/electricity.csv"
    iso_file_path = "hdfs://localhost:9000/user/Bronze_Layer/iso.csv"

    # Read CSV data
    transaction_data = spark.read.csv(transaction_file_path, header=True, inferSchema=True) \
        .select(col("code"), col("transaction"))

    commodity_data = spark.read.csv(commodity_file_path, header=True, inferSchema=True) \
        .select(col("code"), col("commodity"))

    iso_data = spark.read.csv(iso_file_path, header=True, inferSchema=True) \
        .select(col("M49 code"), col("Country or Area"), col("ISO-alpha3 code")) \
        .withColumnRenamed("M49 code", "M49Code") \
        .withColumnRenamed("Country or Area", "CountryOrArea") \
        .withColumnRenamed("ISO-alpha3 code", "ISOAlpha3Code")

    electricity_data = spark.read.csv(electricity_file_path, header=True, inferSchema=True) \
        .select(col("freq"), col("ref_area"), col("commodity"), col("transaction"), col("unit_measure"),
                col("time_period"), col("value"), col("unit_mult"), col("obs_status"), col("conversion_factor"))

    # Write data to PostgreSQL
    for df, table_name in zip([transaction_data, commodity_data, iso_data, electricity_data],
                              ["silver.transaction", "silver.commodity", "silver.CountryCodes", "silver.electricity"]):
        df.write \
            .format("jdbc") \
            .option("url", RDS_PSQL_SERVER) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", table_name) \
            .option("user", RDS_USER) \
            .option("password", RDS_PASS) \
            .mode("append") \
            .save()

    logger.info("Data written to PostgreSQL successfully.")

default_args = {
    'start_date': datetime(2024, 6, 1),
    'catchup': False,
}

with DAG('Silver_Layer', default_args=default_args, schedule_interval='@daily') as dag:

    spark_task = PythonOperator(
        task_id='read_and_write_data',
        python_callable=read_and_write_data
    )

    spark_task
