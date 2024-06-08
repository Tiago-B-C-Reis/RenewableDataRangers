from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("demo").getOrCreate()

# Load data from HDFS
df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load("hdfs://localhost:9000/user/Bronze_Layer/Data/electricity.csv")

df.show(truncate=False)
df.printSchema()

