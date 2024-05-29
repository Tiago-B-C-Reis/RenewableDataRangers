from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create SparkSession
spark = SparkSession.builder \
    .appName("Silver_Layer_Transformations") \
    .getOrCreate()

# Read JSON data
# csv_file_path = "hdfs://localhost:50070/user/Bronze_Layer/airbyte_data.csv"
code_file_path = "/Users/tiagoreis/Downloads/Air_Byte_Outp/Out/code.csv"
electricity_file_path = "/Users/tiagoreis/Downloads/Air_Byte_Outp/Out/electricity.csv"
iso_file_path = "/Users/tiagoreis/Downloads/Air_Byte_Outp/Out/iso.csv"

code_data = spark.read.csv(code_file_path, header=True, inferSchema=True)
electricity_data = spark.read.csv(iso_file_path, header=True, inferSchema=True) \
    .select(col("M49 code"), col("Country or Area"), col("ISO-alpha3 code"))
iso_data = spark.read.csv(electricity_file_path, header=True, inferSchema=True) \
    .select(col("freq"),col("ref_area"), col("commodity"), col("transaction"),col("unit_measure"),
    col("time_period"), col("value"), col("unit_mult"), col("obs_status"), col("conversion_factor"))

# Show data
#code_data.show(truncate=False)
#code_data.printSchema()
#electricity_data.show(truncate=False)
#electricity_data.printSchema()
#iso_data.show(truncate=False)
#iso_data.printSchema()


# Connection details
PSQL_SERVER = "localhost"
PSQL_PORT = "5432"
PSQL_DB = "Silver_Layer"
PSQL_USER = "admin"
PSQL_PASS = "admin"


# Saving data to a JDBC source
electricity_data.write \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql:{PSQL_DB}") \
    .option("dbtable", "dbo.electricity") \
    .option("user", PSQL_USER) \
    .option("password", PSQL_PASS) \
    .save()

iso_data.write \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql:{PSQL_DB}") \
    .option("dbtable", "dbo.iso") \
    .option("user", PSQL_USER) \
    .option("password", PSQL_PASS) \
    .save()
