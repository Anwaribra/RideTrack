import os
import requests
from datetime import datetime
import boto3
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv('AWS_S3_BUCKET', 'ridetrack-data-lake')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def get_months_for_current_year():
    current_date = datetime.now()
    current_year = str(current_date.year)
    months = []
    
    for month in range(1, current_date.month):
        month_str = f"{current_year}-{month:02d}"
        months.append(month_str)
            
    return months

def download_taxi_data(month):
    filename = f"green_tripdata_{month}.parquet"
    url = f"{BASE_URL}/{filename}"
    
    response = requests.get(url)
    response.raise_for_status()
    return response.content

def upload_to_s3(data, month):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    
    s3_key = f"nyc_taxi/green_tripdata_{month}.parquet"
    
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=data
    )
    return True

def main():
    months = get_months_for_current_year()
    
    for month_str in months:
        data = download_taxi_data(month_str)
        upload_to_s3(data, month_str)

main()