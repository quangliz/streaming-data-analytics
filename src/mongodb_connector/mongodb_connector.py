import os
import logging
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load env variables
load_dotenv()

# MongoDB configuration
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://admin:admin@mongodb:27017/')
MONGODB_DATABASE = os.getenv('MONGODB_DATABASE', 'user_analytics')

class MongoDBConnector:
    # MongoDB connector
    
    def __init__(self, uri=None, database=None):
        # connect with URI and database name
        self.uri = uri or MONGODB_URI
        self.database_name = database or MONGODB_DATABASE
        self.client = None
        self.db = None
    
    def connect(self):
        # connect to MongoDB
        try:
            # create a MongoDB client
            self.client = MongoClient(self.uri)
            
            # check if the connection is successful
            self.client.admin.command('ping')
            
            # get the database
            self.db = self.client[self.database_name]
            
            logger.info(f"Connected to MongoDB database: {self.database_name}")
            return self.db
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
        except OperationFailure as e:
            logger.error(f"Authentication failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error connecting to MongoDB: {e}")
            raise
    
    def get_collection(self, collection_name):
        # get a collection from database
        if not self.db:
            self.connect()
        
        return self.db[collection_name]
    
    def insert_one(self, collection_name, document):
        # insert one
        collection = self.get_collection(collection_name)
        try:
            result = collection.insert_one(document)
            logger.debug(f"Inserted document with ID: {result.inserted_id}")
            return result.inserted_id
        except Exception as e:
            logger.error(f"Error inserting document into {collection_name}: {e}")
            raise
    
    def insert_many(self, collection_name, documents):
        # insert many
        collection = self.get_collection(collection_name)
        try:
            result = collection.insert_many(documents)
            logger.debug(f"Inserted {len(result.inserted_ids)} documents")
            return result.inserted_ids
        except Exception as e:
            logger.error(f"Error inserting documents into {collection_name}: {e}")
            raise
    
    def update_one(self, collection_name, filter_dict, update_dict, upsert=False):
        # update one
        collection = self.get_collection(collection_name)
        try:
            result = collection.update_one(filter_dict, update_dict, upsert=upsert)
            logger.debug(f"Modified {result.modified_count} document(s)")
            return result.modified_count
        except Exception as e:
            logger.error(f"Error updating document in {collection_name}: {e}")
            raise
    
    def find(self, collection_name, query=None, projection=None):
        # find documents
        collection = self.get_collection(collection_name)
        try:
            return collection.find(query or {}, projection or {})
        except Exception as e:
            logger.error(f"Error finding documents in {collection_name}: {e}")
            raise
    
    def close(self):
        # end connection
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")
            self.client = None
            self.db = None

# Singleton instance for reuse
_connector = None

def get_mongodb_connector():
    # get MongoDB connector
    global _connector
    if _connector is None:
        _connector = MongoDBConnector()
    return _connector 