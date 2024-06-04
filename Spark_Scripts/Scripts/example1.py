from pyspark.sql import SparkSession

spark = SparkSession.builder.config("spark.jars", "/tmp/shared/postgresql-42.2.5.jar").master("local").appName("PySpark_Postgres_test").getOrCreate()

df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/citizix_db") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "customers") \
    .option("user", "citizix_user") \
    .option("password", "S3cret") \
    .load()

df.write.format("csv").save("/tmp/shared/report2")