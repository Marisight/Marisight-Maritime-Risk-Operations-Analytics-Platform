import urllib.request
import urllib.error
import boto3
import logging
import time
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    BUCKET_NAME = 'marisight-staging-layer-121913093195-us-east-1-an' 
    API_URL = 'https://msi.nga.mil/api/publications/download?type=view&key=16920959/SFH00000/UpdatedPub150.csv'
    
    logger.info("Starting Ports Data Extraction...")
    
    # retry
    csv_data = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(API_URL, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=30) as response:
                if response.getcode() == 200:
                    csv_data = response.read()
                    break
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(2)
            
    if not csv_data:
        logger.error("Failed to download data after 3 attempts.")
        return {'statusCode': 500, 'body': 'Download failed'}

    file_size_kb = len(csv_data) / 1024
    row_count = len(csv_data.strip().split(b'\n')) - 1 
    
    logger.info(f"[STATS] File Size: {file_size_kb:.2f} KB")
    logger.info(f"[STATS] Total Ports Extracted: {row_count}")
    
    
    current_month = datetime.now().strftime("%Y-%m")
    file_name = f"ports/api_{current_month}.csv" 
    
    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=csv_data,
            ContentType='text/csv'
        )
        logger.info(f"[SUCCESS] Uploaded seamlessly to S3: {file_name}")
        return {
            'statusCode': 200,
            'body': f'Successfully ingested {row_count} ports.'
        }
    except Exception as e:
        logger.error(f"S3 Upload failed: {str(e)}")
        return {'statusCode': 500, 'body': str(e)}