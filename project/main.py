from extract import extract_data
from transform import transform_data
from load import load_data_to_dynamodb
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

def main():
    try:
        # Extract
        raw_data = extract_data()
        
        # Transform
        transformed_data = transform_data(raw_data)
        
        # Load
        load_data_to_dynamodb(transformed_data)
        
        logging.info("ETL process completed successfully.")
    except Exception as e:
        logging.error(f"ETL process failed: {e}")

if __name__ == "__main__":
    main()
