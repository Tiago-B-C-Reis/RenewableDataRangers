from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from dotenv import load_dotenv
import subprocess
import logging
import pandas as pd
import os


# Create SparkSession
spark = SparkSession.builder \
    .config("spark.jars", "/Users/tiagoreis/PycharmProjects/RenewableDataRangers/Spark_Scripts/Scripts/postgresql-42.7.3.jar") \
    .master("local") \
    .appName("Silver_Layer_Transformations") \
    .getOrCreate()


# Read JSON data
# csv_file_path = "hdfs://localhost:50070/user/Bronze_Layer/airbyte_data.csv"
transaction_file_path = "hdfs://localhost:9000/user/Bronze_Layer/transaction.csv"
commodity_file_path = "hdfs://localhost:9000/user/Bronze_Layer/commodity.csv"
electricity_file_path = "hdfs://localhost:9000/user/Bronze_Layer/electricity.csv"
iso_file_path = "hdfs://localhost:9000/user/Bronze_Layer/iso.csv"


# Define schema for transaction
transaction_data = spark.read.csv(transaction_file_path, header=True, inferSchema=True) \
    .select(col("code"), col("transaction"))

# Define schema for commodity
commodity_data = spark.read.csv(commodity_file_path, header=True, inferSchema=True) \
    .select(col("code"), col("commodity"))

# Define schema for iso_data
iso_data = spark.read.csv(iso_file_path, header=True, inferSchema=True) \
    .select(col("M49 code"), col("Country or Area"), col("ISO-alpha3 code"))

# Rename columns in iso_data to match your PostgreSQL table schema
iso_data = iso_data \
    .withColumnRenamed("M49 code", "M49Code") \
    .withColumnRenamed("Country or Area", "CountryOrArea") \
    .withColumnRenamed("ISO-alpha3 code", "ISOAlpha3Code") \

# Define schema for electricity_data
electricity_data = spark.read.csv(electricity_file_path, header=True, inferSchema=True) \
    .select(col("freq"),col("ref_area"), col("commodity"), col("transaction"),col("unit_measure"),
    col("time_period"), col("value"), col("unit_mult"), col("obs_status"), col("conversion_factor"))


# Load environment variables
load_dotenv()
# Read environment variables
RDS_PSQL_SERVER = os.getenv("RDS_PSQL_SERVER")
RDS_USER = os.getenv("RDS_USER")
RDS_PASS = os.getenv("RDS_PASS")

# Write data to SILVER layer on PostgreSQL
transaction_data.write \
    .format("jdbc") \
    .option(key="url", value=RDS_PSQL_SERVER) \
    .option(key="driver", value="org.postgresql.Driver") \
    .option(key="dbtable", value="silver.transaction") \
    .option(key="user", value=RDS_USER)\
    .option(key="password", value=RDS_PASS) \
    .mode("append") \
    .save() # Mode as append to avoid duplicate data

# Write data to PostgreSQL
commodity_data.write \
    .format("jdbc") \
    .option(key="url", value=RDS_PSQL_SERVER) \
    .option(key="driver", value="org.postgresql.Driver") \
    .option(key="dbtable", value="silver.commodity") \
    .option(key="user", value=RDS_USER)\
    .option(key="password", value=RDS_PASS) \
    .mode("append") \
    .save() # Mode as append to avoid duplicate data

# Write data to PostgreSQL
iso_data.write \
    .format("jdbc") \
    .option(key="url", value=RDS_PSQL_SERVER) \
    .option(key="driver", value="org.postgresql.Driver") \
    .option(key="dbtable", value="silver.CountryCodes") \
    .option(key="user", value=RDS_USER)\
    .option(key="password", value=RDS_PASS) \
    .mode("append") \
    .save() # Mode as append to avoid duplicate data

electricity_data.write \
    .format("jdbc") \
    .option(key="url", value=RDS_PSQL_SERVER) \
    .option(key="driver", value="org.postgresql.Driver") \
    .option(key="dbtable", value="silver.electricity") \
    .option(key="user", value=RDS_USER)\
    .option(key="password", value=RDS_PASS) \
    .mode("append") \
    .save() # Mode as append to avoid duplicate data
