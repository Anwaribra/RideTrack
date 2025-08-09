import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

NYC_BOUNDS = {
    'lat': {'min': 40.5, 'max': 40.9},  
    'lon': {'min': -74.0, 'max': -73.7}
}

def load_kafka_config():
    with open('storage_config/kafka_config.json', 'r') as f:
        return json.load(f)

kafka_config = load_kafka_config()
KAFKA_BROKER = kafka_config['bootstrap_servers']
TOPIC = kafka_config['topics']['gps_data']

VEHICLES = [f'V{str(i).zfill(4)}' for i in range(1, 51)]

def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def generate_gps_data():
    vehicle_id = random.choice(VEHICLES)
    return {
        "vehicle_id": vehicle_id,
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "latitude": round(random.uniform(NYC_BOUNDS['lat']['min'], NYC_BOUNDS['lat']['max']), 6),
        "longitude": round(random.uniform(NYC_BOUNDS['lon']['min'], NYC_BOUNDS['lon']['max']), 6),
        "speed_kmh": round(random.uniform(0, 60), 1)
    }

def main():
    producer = create_kafka_producer()
    
    while True:
        gps_data = generate_gps_data()
        producer.send(TOPIC, gps_data)
        time.sleep(5)

main()