from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()

df = spark.read.parquet("lakehouse/silver/user_events")

df.show()