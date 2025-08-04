from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col, to_timestamp, expr
import json

spark = SparkSession.builder \
    .appName("test") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.catalogImplementation", "in-memory") \
    .getOrCreate()

def load_kafka_config():
    with open('../storage_config/kafka_config.json', 'r') as f:
        return json.load(f)
    
def load_s3_config():
    with open('../storage_config/aws_s3_config.json', 'r') as f:
        return json.load(f)

kafka_config = load_kafka_config()
KAFKA_BROKER = kafka_config['bootstrap_servers']
GPS_TOPIC = kafka_config['topics']['gps_data']
WEATHER_TOPIC = kafka_config['topics']['weather_data']

gps_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", GPS_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()

weather_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", WEATHER_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()

gps_schema = StructType() \
    .add("vehicle_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("speed_kmh", DoubleType())

weather_schema = StructType() \
    .add("city", StringType()) \
    .add("timestamp", StringType()) \
    .add("temperature_c", DoubleType()) \
    .add("weather_condition", StringType()) \
    .add("humidity", IntegerType())

gps_df = gps_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), gps_schema).alias("data")) \
        .select("data.*")
weather_df = weather_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), weather_schema).alias("data")) \
        .select("data.*")

gps_df = gps_df.withColumn("timestamp", to_timestamp("timestamp"))
weather_df = weather_df.withColumn("timestamp", to_timestamp("timestamp"))

weather_df = weather_df.withWatermark("timestamp", "10 minutes")
gps_df = gps_df.withWatermark("timestamp", "10 minutes")

joined_df = weather_df.join(gps_df, on="timestamp", how="inner")
joined_df = joined_df.dropDuplicates()

joined_query = joined_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

joined_query.awaitTermination()