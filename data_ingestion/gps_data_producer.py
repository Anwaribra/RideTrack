import json
import random
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
TOPIC = os.getenv('GPS_TOPIC', 'gps_topic')
S3_BUCKET = os.getenv('S3_BUCKET', 'ridetrack-data-lake')

NYC_BOUNDS = {
    'lat': {'min': 40.5, 'max': 40.9},
    'lon': {'min': -74.0, 'max': -73.7}
}

DRIVERS = [f'DRV{str(i).zfill(3)}' for i in range(1, 51)]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

s3 = boto3.client('s3')

def generate_gps_data():
    records = []
    for driver_id in DRIVERS:
        record = {
            'driver_id': driver_id,
            'timestamp': datetime.utcnow().isoformat(),
            'latitude': round(random.uniform(NYC_BOUNDS['lat']['min'], NYC_BOUNDS['lat']['max']), 6),
            'longitude': round(random.uniform(NYC_BOUNDS['lon']['min'], NYC_BOUNDS['lon']['max']), 6),
            'speed_kmh': round(random.uniform(10, 55), 1),
            'heading': round(random.uniform(0, 360), 1),
            'status': 'active'
        }
        records.append(record)
        producer.send(TOPIC, record)
    return records

def save_to_s3(records):
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    key = f"gps_data/batch_{timestamp}.parquet"
    
    table = pa.Table.from_pylist(records)
    buf = BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    
    s3.upload_fileobj(buf, S3_BUCKET, key)
    print(f"Uploaded {len(records)} GPS records to S3")

def main():
    print("Starting GPS Data Producer")
    while True:
        records = generate_gps_data()
        save_to_s3(records)

if __name__ == "__main__":
    main()