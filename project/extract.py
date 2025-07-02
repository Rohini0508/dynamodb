import configparser
from pymongo import MongoClient  # type: ignore
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

def extract_data():
    config = configparser.ConfigParser()
    try:
        config.read('project/config/config.ini')  # Correct relative path
        mongo_uri = config['mongodb']['uri']
        db_name = config['mongodb']['database']
        collection_name = config['mongodb']['collection']

        # Connect to MongoDB
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]

        # Fetch all documents from MongoDB collection
        documents = list(collection.find())
        logging.info(f"Extracted {len(documents)} records from MongoDB.")
        return documents
    except Exception as e:
        logging.error(f"Error extracting data from MongoDB: {e}")
        raise
