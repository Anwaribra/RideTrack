import json
import random
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from kafka import KafkaProducer
import boto3
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq

load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'rt360_kafka:29092')
TOPIC = os.getenv('TAXI_TOPIC', 'nyc_taxi')
S3_BUCKET = os.getenv('S3_BUCKET', 'ridetrack-data-lake')

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

s3 = boto3.client('s3')

def generate_taxi_trips(batch_size=100):
    records = []
    current_time = datetime.utcnow()
    
    for _ in range(batch_size):
        trip = {
            "trip_id": f"TRIP{random.randint(10000, 99999)}",
            "pickup_datetime": current_time.isoformat(),
            "dropoff_datetime": (current_time + timedelta(minutes=random.randint(10, 45))).isoformat(),
            "pickup_location": {
                "lat": round(random.uniform(40.5, 40.9), 6),
                "lon": round(random.uniform(-74.0, -73.7), 6)
            },
            "dropoff_location": {
                "lat": round(random.uniform(40.5, 40.9), 6),
                "lon": round(random.uniform(-74.0, -73.7), 6)
            },
            "passenger_count": random.randint(1, 6),
            "fare_amount": round(random.uniform(10, 100), 2),
            "payment_type": random.choice(["CASH", "CARD", "DIGITAL"]),
            "driver_id": f"DRV{random.randint(1, 50):03d}"
        }
        records.append(trip)
        producer.send(TOPIC, trip)
    
    return records

def save_to_s3(records):
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    key = f"taxi_data/batch_{timestamp}.parquet"
    
    table = pa.Table.from_pylist(records)
    buf = BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    
    s3.upload_fileobj(buf, S3_BUCKET, key)
    print(f"Uploaded {len(records)} taxi records to S3")

def main():
    print("Starting NYC Taxi Data Producer")
    while True:
        records = generate_taxi_trips(batch_size=100)
        save_to_s3(records)

if __name__ == "__main__":
    main()