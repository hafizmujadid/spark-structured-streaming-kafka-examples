from pyspark import SparkConf, __version__ as pyspark_version
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

main_pyspark_version = ".".join(pyspark_version.split(".")[:-1])
KAFKA_HOST = "ADFGH"

packages = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{main_pyspark_version}.0"

def get_spark_session(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages",
            packages,
        ).config(
            "spark.sql.warehouse.dir", "file:///spark-warehouse"
        )
    )

    return builder.enableHiveSupport().getOrCreate()


def read_data(topic_name):
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_HOST) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()


def process_json():
    df = read_data("spark_test_topic")

    schema = StructType(
        [StructField("id", IntegerType()), StructField("name", StringType()), StructField("salary", IntegerType())])

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").na.drop(subset=["value"]) \
        .select(from_json(col("value"), schema).alias("value")) \
        .selectExpr("value.*") \
        .writeStream \
        .format("console") \
        .option("checkpointLocation", "file:///spark_kafka_checkpoint") \
        .outputMode("append") \
        .trigger(processingTime='60 seconds') \
        .start() \
        .awaitTermination()


spark = get_spark_session("kafka-spark-example")
process_json()
