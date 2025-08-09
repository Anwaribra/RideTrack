from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col, to_timestamp
import json
from dotenv import load_dotenv
import os

load_dotenv()

def load_kafka_config():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(os.path.dirname(current_dir), 'storage_config', 'kafka_config.json')
    with open(config_path, 'r') as f:
        return json.load(f)

kafka_config = load_kafka_config()
KAFKA_BROKER = kafka_config['bootstrap_servers']
GPS_TOPIC = kafka_config['topics']['gps_data']
WEATHER_TOPIC = kafka_config['topics']['weather_data']

# S3 configuration 
S3_BUCKET = os.getenv('S3_BUCKET', 'ridetrack-data-lake')
ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

FOLDER_NAME = "streaming_output_v2"
base_path = f"s3a://{S3_BUCKET}/{FOLDER_NAME}/"

spark = SparkSession.builder \
    .appName("RideTrack360_Streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:4.0.0") \
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.sql.catalogImplementation", "in-memory") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.security.authentication", "NONE") \
    .config("spark.hadoop.security.authorization", "false") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.streaming.kafka.maxRatePerPartition", "10000") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.default.parallelism", "10") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.streaming.kafka.consumer.cache.enabled", "true") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer") \
    .config("spark.hadoop.fs.s3a.multipart.size", "10485760") \
    .config("spark.hadoop.fs.s3a.threads.max", "20") \
    .getOrCreate()

gps_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", GPS_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

weather_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", WEATHER_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
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
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp"))) \
    .dropDuplicates(["vehicle_id", "timestamp"]) \
    .withWatermark("event_time", "2 minutes")

weather_df = weather_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), weather_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp"))) \
    .dropDuplicates(["city", "timestamp"]) \
    .withWatermark("event_time", "5 minutes")

# Add timestamp-based partitioning
from pyspark.sql.functions import year, month, dayofmonth, hour

weather_df = weather_df \
    .withColumn("year", year("event_time")) \
    .withColumn("month", month("event_time")) \
    .withColumn("day", dayofmonth("event_time")) \
    .withColumn("hour", hour("event_time"))

gps_df = gps_df \
    .withColumn("year", year("event_time")) \
    .withColumn("month", month("event_time")) \
    .withColumn("day", dayofmonth("event_time")) \
    .withColumn("hour", hour("event_time"))

# Write with optimized settings
weather_query = weather_df.writeStream \
    .format("json") \
    .partitionBy("year", "month", "day", "hour") \
    .option("path", base_path + 'weather_data') \
    .option("checkpointLocation", base_path + 'weather_data/checkpoint') \
    .option("maxFilesPerTrigger", "10") \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .start()

gps_query = gps_df.writeStream \
    .format("json") \
    .partitionBy("year", "month", "day", "hour") \
    .option("path", base_path + 'gps_data') \
    .option("checkpointLocation", base_path + 'gps_data/checkpoint') \
    .option("maxFilesPerTrigger", "10") \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .start()

weather_query.awaitTermination()
gps_query.awaitTermination()
