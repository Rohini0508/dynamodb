import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

def transform_data(raw_data):
    transformed = []
    try:
        for record in raw_data:
            record.pop('_id', None)  # Remove MongoDB _id field
            # Additional transformations can be added here
            transformed.append(record)
        logging.info(f"Transformed {len(raw_data)} records.")
        return transformed
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        raise
