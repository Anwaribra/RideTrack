import os
import json
import requests
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

# s3 configuration
S3_BUCKET = os.getenv('AWS_S3_BUCKET', 'ridetrack-data-lake')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
    raise ValueError("AWS credentials not found. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env file")

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def get_months_for_current_year():
    """get all months for the current year up to current month
    """
    current_date = datetime.now()
    current_year = str(current_date.year)
    months = []
    
    for month in range(1, current_date.month):
        month_str = f"{current_year}-{month:02d}"
        months.append(month_str)
            
    return months



def download_taxi_data(month):
    """download taxi data for the specified month"""
    filename = f"green_tripdata_{month}.parquet"
    url = f"{BASE_URL}/{filename}"
    
    print(f"downloading {filename}...")
    try:
        response = requests.get(url)
        response.raise_for_status()  
        return response.content
    except requests.exceptions.RequestException as e:
        print(f"error downloading {filename}: {e}")
        return None

def upload_to_s3(data, month):
    """upload data to s3 bucket"""
    if not data:
        return False
        
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    
    s3_key = f"nyc_taxi/green_tripdata_{month}.parquet"
    
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=data
        )
        print(f"successfully uploaded to s3://{S3_BUCKET}/{s3_key}")
        return True
    except ClientError as e:
        print(f"error uploading to S3: {e}")
        return False

def main():
    """Mamin function to run the nyc taxi data ingestion"""
    print("starting nyc taxi data ingestion")
    
    # all months for current year
    months = get_months_for_current_year()
    print(f"attempting to download data for {len(months)} months")
    
    successful_months = 0
    failed_months = 0
    
    #  download and store each month
    for month_str in months:
        print(f"\nprocessing month: {month_str}")
        data = download_taxi_data(month_str)
        
        if data:
            success = upload_to_s3(data, month_str)
            if success:
                print(f"successfully stored {month_str}")
                print(f"location: s3://{S3_BUCKET}/nyc_taxi/green_tripdata_{month_str}.parquet")
                successful_months += 1
            else:
                print(f"failed to upload {month_str}")
                failed_months += 1
        else:
            failed_months += 1
    
    print(f"\nsummary:")
    print(f"attempted months: {len(months)}")
    print(f"successful: {successful_months}")
    print(f"failed: {failed_months}")

if __name__ == "__main__":
    main()