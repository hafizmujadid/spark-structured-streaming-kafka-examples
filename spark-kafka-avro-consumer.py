from pyspark import __version__ as pyspark_version
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr

main_pyspark_version = ".".join(pyspark_version.split(".")[:-1])
KAFKA_HOST = "ADFGH"

packages = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{main_pyspark_version}.0,org.apache.spark:spark-avro_2.12:{main_pyspark_version}.0"


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


def process_avro():
    df = read_data("spark_avro")
    df = df.withColumns({
        "magicByte": expr("substring(value, 1, 1)"),
        "valueSchemaId": expr("substring(value, 2, 4)"),
        "value": expr("substring(value, 6, length(value)-5)")
    })

    fromAvroOptions = {"mode": "PERMISSIVE"}
    valuejsonFormatSchema = open("./schema/value_schema.avsc", "r").read()
    keyjsonFormatSchema = open("./schema/key_schema.avsc", "r").read()

    df = df.withColumns({
        "value": from_avro(col("value"), valuejsonFormatSchema, fromAvroOptions),
        "key": from_avro(col("key"), keyjsonFormatSchema, fromAvroOptions),
    }).selectExpr("key", "value", "topic", "timestamp")

    df.writeStream \
        .format("console") \
        .option("checkpointLocation", "file:///spark_kafka_checkpoint") \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start() \
        .awaitTermination()


spark = get_spark_session("kafka-spark-avro-example")
process_avro()

