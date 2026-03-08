# streaming/spark_jobs/stream_processor.py

from pyspark.sql import SparkSession

def create_spark_session():

    spark = SparkSession.builder \
        .appName("EnterpriseAIStreaming") \
        .master("local[*]") \
        .getOrCreate()

    return spark


def main():

    spark = create_spark_session()

    print("Spark Session Started")
    print("Spark Version:", spark.version)

    # Simple test dataframe
    data = [
        ("user1", "Laptop", "view", 1200.0),
        ("user2", "Camera", "purchase", 900.0),
        ("user3", "Smartwatch", "cart", 300.0)
    ]

    columns = ["user_id", "product_id", "event_type", "price"]

    df = spark.createDataFrame(data, columns)

    print("Sample DataFrame:")

    df.show()

    spark.stop()


if __name__ == "__main__":
    main()