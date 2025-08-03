import json
import os
import time
from datetime import datetime
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv


load_dotenv()

# Load Kafka configuration
def load_kafka_config():
    with open('storage_config/kafka_config.json', 'r') as f:
        return json.load(f)

# kafka configuration
kafka_config = load_kafka_config()
KAFKA_BROKER = kafka_config['bootstrap_servers']
TOPIC = kafka_config['topics']['weather_data']

# openweather api configuration
CITY = "New York"
API_KEY = os.getenv('OPENWEATHER_API_KEY')  
if not API_KEY:
    raise ValueError("OpenWeather API key not found. Set OPENWEATHER_API_KEY environment variable.")

def create_kafka_producer():
    """Create and return a Kafka producer instance"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def fetch_weather_data():
    """Fetch current weather data from OpenWeather API"""
    url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    
    try:
        response = requests.get(url)
        response.raise_for_status()  
        data = response.json()
        
        weather_data = {
            "city": CITY,
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "temperature_c": round(data['main']['temp'], 1),
            "weather_condition": data['weather'][0]['main'],
            "humidity": data['main']['humidity']
        }
        
        return weather_data
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None

def main():
    """Main function to run the weather data producer"""
    producer = create_kafka_producer()
    print(f"Starting Weather Data Producer - fetching data every 5 minutes for {CITY}")
    
    try:
        while True:
            # fetch and send weather data
            weather_data = fetch_weather_data()
            
            if weather_data:
                producer.send(TOPIC, weather_data)
                print(f"Sent weather data: {weather_data}")
            
            
            time.sleep(0.1)  
            
    except KeyboardInterrupt:
        print("\nStopping Weather Data Producer")
        producer.close()
        
    except Exception as e:
        print(f"Error occurred: {e}")
        producer.close()

if __name__ == "__main__":
    main()