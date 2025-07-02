
import boto3
from configparser import ConfigParser
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO)

def get_aws_config(config_file='project/config/config.ini'):
    config = ConfigParser()
    try:
        config.read(config_file)
        aws_config = {
            'region': config['aws']['region'].strip(),
            'table': config['aws']['table'].strip()
        }
        logging.info("AWS configuration loaded successfully.")
        return aws_config
    except Exception as e:
        logging.error(f"Error reading AWS configuration: {e}")
        raise

def load_data_to_dynamodb(data):
    aws_config = get_aws_config()

    # Using environment variables for AWS credentials (best practice)
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_session_token = os.getenv('AWS_SESSION_TOKEN', None)

    if not aws_access_key or not aws_secret_key:
        logging.error("AWS credentials are not set in environment variables.")
        raise ValueError("AWS credentials are required")

    try:
        dynamodb = boto3.resource(
            'dynamodb',
            region_name=aws_config['region'],
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            aws_session_token=aws_session_token
        )

        table = dynamodb.Table(aws_config['table'])

        # Use batch_writer for efficient writes
        with table.batch_writer() as batch:
            for item in data:
                batch.put_item(Item=item)
        logging.info(f"Successfully loaded {len(data)} records to DynamoDB.")
    except Exception as e:
        logging.error(f"Error loading data to DynamoDB: {e}")
        raise
