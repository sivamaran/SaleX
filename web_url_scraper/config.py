import os
import re
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration Variables
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
GOOGLE_SEARCH_ENGINE_ID = os.getenv('GOOGLE_SEARCH_ENGINE_ID')
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
MONGODB_DATABASE_NAME = os.getenv('MONGODB_DATABASE_NAME', 'aiqod-dev')
MONGODB_COLLECTION_NAME = os.getenv('MONGODB_COLLECTION_NAME', 'scraped_urls')

# MVP Constants (hardcoded for MVP)
MAX_PAGES = 2
RESULTS_PER_PAGE = 10

def validate_config():
    """
    Validate that all required configuration variables are properly set.
    Returns True if valid, False otherwise.
    """
    errors = []
    
    # Check Google API Key
    if not GOOGLE_API_KEY:
        errors.append("GOOGLE_API_KEY is not set")
    elif len(GOOGLE_API_KEY.strip()) == 0:
        errors.append("GOOGLE_API_KEY is empty")
    
    # Check Google Search Engine ID
    if not GOOGLE_SEARCH_ENGINE_ID:
        errors.append("GOOGLE_SEARCH_ENGINE_ID is not set")
    elif len(GOOGLE_SEARCH_ENGINE_ID.strip()) == 0:
        errors.append("GOOGLE_SEARCH_ENGINE_ID is empty")
    
    # Check MongoDB URI format
    if not MONGODB_URI:
        errors.append("MONGODB_URI is not set")
    elif not re.match(r'^mongodb(\+srv)?://', MONGODB_URI):
        errors.append("MONGODB_URI format is invalid")
    
    # Check Database Name
    if not MONGODB_DATABASE_NAME:
        errors.append("MONGODB_DATABASE_NAME is not set")
    elif len(MONGODB_DATABASE_NAME.strip()) == 0:
        errors.append("MONGODB_DATABASE_NAME is empty")
    
    # Check Collection Name
    if not MONGODB_COLLECTION_NAME:
        errors.append("MONGODB_COLLECTION_NAME is not set")
    elif len(MONGODB_COLLECTION_NAME.strip()) == 0:
        errors.append("MONGODB_COLLECTION_NAME is empty")
    
    # Print errors if any
    if errors:
        print("Configuration validation failed:")
        for error in errors:
            print(f"  - {error}")
        return False
    
    print("Configuration validation passed!")
    return True

def get_config_summary():
    """
    Return a summary of current configuration (without sensitive data)
    """
    return {
        'google_api_key_set': bool(GOOGLE_API_KEY),
        'google_search_engine_id_set': bool(GOOGLE_SEARCH_ENGINE_ID),
        'mongodb_uri': MONGODB_URI,
        'mongodb_database': MONGODB_DATABASE_NAME,
        'mongodb_collection': MONGODB_COLLECTION_NAME,
        'max_pages': MAX_PAGES,
        'results_per_page': RESULTS_PER_PAGE
    } 