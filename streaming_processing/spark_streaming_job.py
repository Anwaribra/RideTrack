from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col, to_timestamp, expr
import json

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

aws_s3_config = load_s3_config()
S3_BUCKET = aws_s3_config['S3_BUCKET']
ACCESS_KEY = aws_s3_config['ACCESS_KEY']
SECRET_KEY = aws_s3_config['SECRET_KEY']

spark = SparkSession.builder \
    .appName("test") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.750") \
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.sql.catalogImplementation", "in-memory") \
    .getOrCreate()


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

output_path = f"s3a://{S3_BUCKET}/streaming_output/data/"

weather_query = weather_df.writeStream \
    .format("json") \
    .option("path", output_path + 'weather_data') \
    .option("checkpointLocation", output_path + 'weather_data/checkpoint') \
    .outputMode("append") \
    .start()

gps_query = gps_df.writeStream \
    .format("json") \
    .option("path", output_path + 'gps_data') \
    .option("checkpointLocation", output_path + 'gps_data/checkpoint') \
    .outputMode("append") \
    .start()

weather_query.awaitTermination()
gps_query.awaitTermination()