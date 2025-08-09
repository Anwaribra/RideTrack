import json
import os
import time
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from kafka import KafkaProducer
from dotenv import load_dotenv

DEFAULT_INTERVAL_SECONDS = 300

load_dotenv()

def load_config():
    with open('storage_config/kafka_config.json', 'r') as f:
        kafka_config = json.load(f)
    
    interval_seconds = int(os.getenv('WEATHER_FETCH_INTERVAL_SECONDS', DEFAULT_INTERVAL_SECONDS))
    
    return {
        'kafka': kafka_config,
        'interval_seconds': interval_seconds,
        'city': os.getenv('WEATHER_CITY', 'New York'),
        'api_key': os.getenv('OPENWEATHER_API_KEY')
    }

config = load_config()
KAFKA_BROKER = config['kafka']['bootstrap_servers']
TOPIC = config['kafka']['topics']['weather_data']
CITY = config['city']
API_KEY = config['api_key']

def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5,
        retry_backoff_ms=1000
    )

def setup_http_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def validate_weather_data(data):
    return (isinstance(data, dict) and
            all(field in data['main'] for field in ['temp', 'humidity']) and
            isinstance(data['weather'], list) and
            len(data['weather']) > 0 and
            'main' in data['weather'][0])

def fetch_weather_data():
    url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    session = setup_http_session()
    
    response = session.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    
    return {
        "city": CITY,
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "temperature_c": round(data['main']['temp'], 1),
        "weather_condition": data['weather'][0]['main'],
        "humidity": data['main']['humidity']
    }

def main():
    config = load_config()
    interval_seconds = config['interval_seconds']
    producer = create_kafka_producer()
    
    next_fetch_time = datetime.now()
    
    while True:
        current_time = datetime.now()
        weather_data = fetch_weather_data()
        producer.send(TOPIC, weather_data).get(timeout=10)
        next_fetch_time = current_time + timedelta(seconds=interval_seconds)
        time.sleep(interval_seconds)

main()