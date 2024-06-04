from pyspark.sql import SparkSession
from pyspark.sql.functions import *
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
    .appName("Gold_Layer_Transformations") \
    .getOrCreate()


# Load environment variables
load_dotenv()

# Read environment variables
RDS_PSQL_SERVER = os.getenv("RDS_PSQL_SERVER")
RDS_USER = os.getenv("RDS_USER")
RDS_PASS = os.getenv("RDS_PASS")

# Check if environment variables are loaded
if not RDS_PSQL_SERVER or not RDS_USER or not RDS_PASS:
    raise ValueError("Environment variables for database connection are not set properly.")

# Write data to PostgreSQL
transaction_data_df = spark.read \
    .format("jdbc") \
    .option(key="url", value=RDS_PSQL_SERVER) \
    .option(key="driver", value="org.postgresql.Driver") \
    .option(key="dbtable", value="silver.transaction") \
    .option(key="user", value=RDS_USER)\
    .option(key="password", value=RDS_PASS) \
    .load() # Mode as append to avoid duplicate data

# Write data to PostgreSQL
commodity_data_df = spark.read \
    .format("jdbc") \
    .option(key="url", value=RDS_PSQL_SERVER) \
    .option(key="driver", value="org.postgresql.Driver") \
    .option(key="dbtable", value="silver.commodity") \
    .option(key="user", value=RDS_USER)\
    .option(key="password", value=RDS_PASS) \
    .load() # Mode as append to avoid duplicate data

# Write data to PostgreSQL
countrycodes_data_df = spark.read \
    .format("jdbc") \
    .option(key="url", value=RDS_PSQL_SERVER) \
    .option(key="driver", value="org.postgresql.Driver") \
    .option(key="dbtable", value="silver.CountryCodes") \
    .option(key="user", value=RDS_USER)\
    .option(key="password", value=RDS_PASS) \
    .load() # Mode as append to avoid duplicate data

electricity_data_df = spark.read \
    .format("jdbc") \
    .option(key="url", value=RDS_PSQL_SERVER) \
    .option(key="driver", value="org.postgresql.Driver") \
    .option(key="dbtable", value="silver.electricity") \
    .option(key="user", value=RDS_USER)\
    .option(key="password", value=RDS_PASS) \
    .load() # Mode as append to avoid duplicate data


# Drop duplicates
transaction_data_df = transaction_data_df.dropDuplicates()
commodity_data_df = commodity_data_df.dropDuplicates()
countrycodes_data_df = countrycodes_data_df.dropDuplicates()
electricity_data_df = electricity_data_df.dropDuplicates()

# Drop null values
transaction_data_df = transaction_data_df.dropna(thresh=2)
commodity_data_df = commodity_data_df.dropna(thresh=2)
countrycodes_data_df = countrycodes_data_df.dropna(thresh=3)
electricity_data_df = electricity_data_df.dropna(thresh=10)


# Create temporary views
transaction_data_df.createOrReplaceTempView("transaction_data_df")
commodity_data_df.createOrReplaceTempView("commodity_data_df")
countrycodes_data_df.createOrReplaceTempView("countrycodes_data_df")
electricity_data_df.createOrReplaceTempView("electricity_data_df")

# Perform SQL transformations
electricity_data_view = spark.sql("""
SELECT 
    edf.id, 
    edf.freq, 
    cdf.countryorarea AS Country_Name,
    cdf.isoalpha3code AS Country_Codes,
    edf.commodity AS commodity_code, 
    codf.commodity AS commodity_name,
    edf.transaction AS transaction_code, 
    tdf.transaction AS transaction_name,
    edf.unit_measure, 
    edf.time_period, 
    edf.value,
    edf.obs_status
FROM electricity_data_df edf
LEFT JOIN countrycodes_data_df cdf ON cdf.m49code = edf.ref_area
LEFT JOIN commodity_data_df codf ON codf.code = edf.commodity
LEFT JOIN transaction_data_df tdf ON tdf.code = edf.transaction
""")


# Create new aggregation tables
electricity_year_agg_df = electricity_data_view \
    .groupBy("time_period") \
    .agg(
        sum("value").alias("total_energy_per_year"),
        max("value").alias("max_energy_per_year"),
        min("value").alias("min_energy_per_year"),
        avg("value").alias("avg_energy_per_year")
    ) \
    .orderBy("time_period")

electricity_country_agg_df = electricity_data_view \
    .groupBy("Country_Codes") \
    .agg(
        sum("value").alias("total_energy_per_country"),
        max("value").alias("max_energy_per_country"),
        min("value").alias("min_energy_per_country"),
        avg("value").alias("avg_energy_per_country")
    ) \
    .orderBy("Country_Codes")

electricity_commodity_agg_df = electricity_data_view \
    .groupBy("commodity_name") \
    .agg(
        sum("value").alias("energy_per_commodity"),
        max("value").alias("max_energy_per_commodity"),
        min("value").alias("min_energy_per_commodity"),
        avg("value").alias("avg_energy_per_commodity")
    ) \
    .orderBy("commodity_name")


# Write data to SILVER layer on PostgreSQL
transaction_data_df.write \
    .format("jdbc") \
    .option(key="url", value=RDS_PSQL_SERVER) \
    .option(key="driver", value="org.postgresql.Driver") \
    .option(key="dbtable", value="gold.transaction") \
    .option(key="user", value=RDS_USER)\
    .option(key="password", value=RDS_PASS) \
    .mode("append") \
    .save() # Mode as append to avoid duplicate data

commodity_data_df.write \
    .format("jdbc") \
    .option(key="url", value=RDS_PSQL_SERVER) \
    .option(key="driver", value="org.postgresql.Driver") \
    .option(key="dbtable", value="gold.commodity") \
    .option(key="user", value=RDS_USER)\
    .option(key="password", value=RDS_PASS) \
    .mode("append") \
    .save() # Mode as append to avoid duplicate data

electricity_data_view.write \
    .format("jdbc") \
    .option(key="url", value=RDS_PSQL_SERVER) \
    .option(key="driver", value="org.postgresql.Driver") \
    .option(key="dbtable", value="gold.electricity_complete") \
    .option(key="user", value=RDS_USER)\
    .option(key="password", value=RDS_PASS) \
    .mode("append") \
    .save() # Mode as append to avoid duplicate data

electricity_year_agg_df.write \
    .format("jdbc") \
    .option(key="url", value=RDS_PSQL_SERVER) \
    .option(key="driver", value="org.postgresql.Driver") \
    .option(key="dbtable", value="gold.electricity_per_year") \
    .option(key="user", value=RDS_USER)\
    .option(key="password", value=RDS_PASS) \
    .mode("append") \
    .save() # Mode as append to avoid duplicate data

electricity_country_agg_df.write \
    .format("jdbc") \
    .option(key="url", value=RDS_PSQL_SERVER) \
    .option(key="driver", value="org.postgresql.Driver") \
    .option(key="dbtable", value="gold.electricity_per_country") \
    .option(key="user", value=RDS_USER)\
    .option(key="password", value=RDS_PASS) \
    .mode("append") \
    .save() # Mode as append to avoid duplicate data

electricity_commodity_agg_df.write \
    .format("jdbc") \
    .option(key="url", value=RDS_PSQL_SERVER) \
    .option(key="driver", value="org.postgresql.Driver") \
    .option(key="dbtable", value="gold.electricity_per_commodity") \
    .option(key="user", value=RDS_USER)\
    .option(key="password", value=RDS_PASS) \
    .mode("append") \
    .save() # Mode as append to avoid duplicate data
