import json
import requests
import os
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer
import boto3
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq

load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'rt360_kafka:29092')
TOPIC = os.getenv('WEATHER_TOPIC', 'weather_topic')
API_KEY = os.getenv('OPENWEATHER_API_KEY')
CITIES = os.getenv('WEATHER_CITIES', 'New York,Chicago,Los Angeles').split(',')
S3_BUCKET = os.getenv('S3_BUCKET', 'ridetrack-data-lake')

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

s3 = boto3.client('s3')

def get_weather_data():
    records = []
    for city in CITIES:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
        response = requests.get(url)
        data = response.json()
        
        weather_data = {
            'city': city,
            'timestamp': datetime.utcnow().isoformat(),
            'temperature': data['main']['temp'],
            'weather_condition': data['weather'][0]['main'],
            'wind_speed': data['wind']['speed'],
            'humidity': data['main']['humidity']
        }
        records.append(weather_data)
        producer.send(TOPIC, weather_data)
    return records

def save_to_s3(records):
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    key = f"weather_data/batch_{timestamp}.parquet"
    
    table = pa.Table.from_pylist(records)
    buf = BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    
    s3.upload_fileobj(buf, S3_BUCKET, key)
    print(f"Uploaded {len(records)} weather records to S3")

def main():
    print("Starting Weather Data Producer")
    while True:
        records = get_weather_data()
        save_to_s3(records)

if __name__ == "__main__":
    main()