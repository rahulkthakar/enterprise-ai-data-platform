# streaming/spark_jobs/stream_processor.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json,  to_timestamp, lower, trim, hour, count, sum, when
from pyspark.sql.types import StructType, StringType, FloatType

from data_quality.expectations.user_events_expectations.expectations import (
    validate_events,
)


BRONZE_PATH = "lakehouse/bronze/user_events"
SILVER_PATH = "lakehouse/silver/user_events"
GOLD_TOP_PRODUCTS_PATH = "lakehouse/gold/top_products"
GOLD_REVENUE_PER_HOUR_PATH = "lakehouse/gold/revenue_per_hour"
GOLD_USER_METRICS_PATH = "lakehouse/gold/user_metrics"
GOLD_COVERSION_RATE_PATH = "lakehouse/gold/conversion_rate"


def create_spark_session():

    spark = (
        SparkSession.builder.appName("KafkaSparkStreaming")
        .master("local[*]")
        .config("spark.sql.streaming.checkpointLocation", "lakehouse/checkpoints")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def validate_and_write(batch_df, batch_id):

    if batch_df.count() == 0:
        return

    print("Running data quality validation...")

    if validate_events(batch_df):

        print("Data quality passed")

        batch_df.write.mode("append").parquet(BRONZE_PATH)

        # Clean data for Silver layer
        cleaned_df = clean_events(batch_df)

        print("Writing cleaned data to Silver")

        cleaned_df.write \
            .mode("append") \
            .parquet(SILVER_PATH)

        print("Generating Gold analytics tables")

        top_products, revenue_per_hour, user_metrics, conversion_rate = generate_gold_tables(cleaned_df)

        # Write Gold tables
        top_products.write \
            .mode("overwrite") \
            .parquet(GOLD_TOP_PRODUCTS_PATH)

        revenue_per_hour.write \
            .mode("overwrite") \
            .parquet(GOLD_REVENUE_PER_HOUR_PATH)

        user_metrics.write \
            .mode("overwrite") \
            .parquet(GOLD_USER_METRICS_PATH)

        conversion_rate.write \
            .mode("overwrite") \
            .parquet(GOLD_COVERSION_RATE_PATH)

    else:

        print(f"Data quality failed — batch {batch_id} skipped")

def clean_events(df):

    cleaned_df = df \
        .dropDuplicates(["user_id", "product_id", "timestamp"]) \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("device", lower(trim(col("device")))) \
        .withColumn("location", trim(col("location")))

    return cleaned_df

def generate_gold_tables(silver_df):

    # Top Products by purchase count
    top_products = silver_df \
        .filter(col("event_type") == "purchase") \
        .groupBy("product_id") \
        .agg(count("*").alias("purchase_count")) \
        .orderBy(col("purchase_count").desc())

    # Revenue per hour
    revenue_per_hour = silver_df \
        .filter(col("event_type") == "purchase") \
        .withColumn("hour", hour(col("timestamp"))) \
        .groupBy("hour") \
        .agg(sum("price").alias("total_revenue"))

    # User metrics
    user_metrics = silver_df \
        .groupBy("user_id") \
        .agg(
            count("*").alias("total_events"),
            sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases")
        )

    # Conversion rate
    conversion_rate = silver_df \
        .groupBy("user_id") \
        .agg(
            sum(when(col("event_type") == "view", 1).otherwise(0)).alias("views"),
            sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases")
        ) \
        .withColumn("conversion_rate", col("purchases") / when(col("views") > 0, col("views")).otherwise(1))   

    return top_products, revenue_per_hour, user_metrics, conversion_rate

def main():

    spark = create_spark_session()

    print("Spark Session Started")

    print("Spark Version:", spark.version)

    # Simple test dataframe
    # data = [
    #    ("user1", "Laptop", "view", 1200.0),
    #    ("user2", "Camera", "purchase", 900.0),
    #    ("user3", "Smartwatch", "cart", 300.0)
    # ]
    #
    # columns = ["user_id", "product_id", "event_type", "price"]
    #
    # df = spark.createDataFrame(data, columns)
    #
    # print("Sample DataFrame:")
    #
    # df.show()
    #
    # spark.stop()

    # Define schema matching the event structure

    schema = (
        StructType()
        .add("user_id", StringType())
        .add("product_id", StringType())
        .add("event_type", StringType())
        .add("price", FloatType())
        .add("timestamp", StringType())
        .add("device", StringType())
        .add("location", StringType())
    )

    # Read stream from Kafka

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "ubuntu.hom:9092")
        .option("subscribe", "user_events")
        .option("startingOffsets", "latest")
        .load()
    )

    print("Kafka stream connected")

    # Kafka message value is in binary → convert to string

    json_df = kafka_df.selectExpr("CAST(value AS STRING)")

    # Parse JSON messages

    parsed_df = json_df.select(from_json(col("value"), schema).alias("data")).select(
        "data.*"
    )

    print("Parsed streaming DataFrame")

    # Print stream to console
    #
    # query = parsed_df.writeStream \
    #    .format("console") \
    #    .outputMode("append") \
    #    .start()

    # Write raw events to Bronze layer

    # query = (
    #    parsed_df.writeStream.format("parquet")
    #    .option("path", BRONZE_PATH)
    #    .option("checkpointLocation", "lakehouse/bronze/checkpoints")
    #    .outputMode("append")
    #    .start()
    # )

    query = (
        parsed_df.writeStream.foreachBatch(validate_and_write)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
