from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadBronze") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.parquet("lakehouse/bronze/user_events")

df.show()