# streaming/spark_jobs/stream_processor.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, FloatType


def create_spark_session():

    spark = SparkSession.builder \
        .appName("KafkaSparkStreaming") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    return spark


def main():

    spark = create_spark_session()

    print("Spark Session Started")

    print("Spark Version:", spark.version)

    ## Simple test dataframe
    #data = [
    #    ("user1", "Laptop", "view", 1200.0),
    #    ("user2", "Camera", "purchase", 900.0),
    #    ("user3", "Smartwatch", "cart", 300.0)
    #]
    #
    #columns = ["user_id", "product_id", "event_type", "price"]
    #
    #df = spark.createDataFrame(data, columns)
    #
    #print("Sample DataFrame:")
    #
    #df.show()
    #
    #spark.stop()

    # Define schema matching the event structure

    schema = StructType() \
        .add("user_id", StringType()) \
        .add("product_id", StringType()) \
        .add("event_type", StringType()) \
        .add("price", FloatType()) \
        .add("timestamp", StringType()) \
        .add("device", StringType()) \
        .add("location", StringType())

    # Read stream from Kafka

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "ubuntu.hom:9092") \
        .option("subscribe", "user_events") \
        .option("startingOffsets", "latest") \
        .load()

    print("Kafka stream connected")

    # Kafka message value is in binary → convert to string

    json_df = kafka_df.selectExpr("CAST(value AS STRING)")

    # Parse JSON messages

    parsed_df = json_df.select(
        from_json(col("value"), schema).alias("data")
    ).select("data.*")

    print("Parsed streaming DataFrame")

    # Print stream to console

    query = parsed_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    main()