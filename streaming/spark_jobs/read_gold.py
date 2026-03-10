from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()

df = spark.read.parquet("lakehouse/gold/top_products")

df.show()
df = spark.read.parquet("lakehouse/gold/revenue_per_hour")

df.show()
df = spark.read.parquet("lakehouse/gold/user_metrics")

df.show()
df = spark.read.parquet("lakehouse/gold/conversion_rate")

df.show()