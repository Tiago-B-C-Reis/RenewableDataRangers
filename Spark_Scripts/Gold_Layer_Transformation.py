from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import *
from pyspark.sql.types import *
import subprocess
import logging
import pandas as pd
import os

# Create SparkSession
spark = SparkSession.builder \
    .appName("Gold_Layer_Transformations") \
    .getOrCreate()


# Connection details
PSQL_SERVER = "localhost"
PSQL_PORT = "5432"
PSQL_DB = "Silver_Layer"
PSQL_USER = "admin"
PSQL_PASS = "admin"


# Saving data to a JDBC source
electricity_df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql:{PSQL_DB}") \
    .option("dbtable", "dbo.electricity") \
    .option("user", PSQL_USER) \
    .option("password", PSQL_PASS) \
    .option("customSchema", "id DECIMAL(38, 0), name STRING") \
    .load()

iso_df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql:{PSQL_DB}") \
    .option("dbtable", "dbo.iso") \
    .option("user", PSQL_USER) \
    .option("password", PSQL_PASS) \
    .option("customSchema", "id DECIMAL(38, 0), name STRING") \
    .load()
