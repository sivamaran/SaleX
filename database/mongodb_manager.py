"""
MongoDB Database Manager for Lead Generation Application
Handles database connections and operations for all scrapers
"""

import os
import json
import re
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from bson import ObjectId
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MongoDBManager:
    """MongoDB database manager for lead generation data"""
    
    def __init__(self, 
                 connection_string: str = None,
                 database_name: str = "aiqod-dev",
                 max_pool_size: int = 100):
        """
        Initialize MongoDB connection
        
        Args:
            connection_string: MongoDB connection string (defaults to localhost)
            database_name: Name of the database
            max_pool_size: Maximum connection pool size
        """
        self.connection_string = connection_string or os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
        self.database_name = database_name
        self.max_pool_size = max_pool_size
        self.client = None
        self.db = None
        
        # Collection names for each scraper
        self.collections = {
            'instagram': 'instagram_leads',
            'linkedin': 'linkedin_leads', 
            'web': 'web_leads',
            'youtube': 'youtube_leads',
            'company_directory': 'company_directory_leads',
            'unified': 'unified_leads'  # New unified collection
        }

        self._connect()
    
    def _connect(self):
        """Establish connection to MongoDB"""
        try:
            self.client = MongoClient(
                self.connection_string,
                maxPoolSize=self.max_pool_size,
                serverSelectionTimeoutMS=5000
            )
            
            # Test connection
            self.client.admin.command('ping')
            self.db = self.client[self.database_name]
            
            logger.info(f"✅ Connected to MongoDB database: {self.database_name}")
            
            # Create indexes for better performance
            self._create_indexes()
            
        except ConnectionFailure as e:
            logger.error(f"❌ Failed to connect to MongoDB: {e}")
            raise
    
    def _create_indexes(self):
        """Create indexes for better query performance
        
        Note: This will continue execution even if some indexes fail to be created
        due to insufficient permissions.
        """
        def safe_create_index(collection, index_spec, **kwargs):
            """Safely create a single index with error handling"""
            try:
                collection.create_index(index_spec, **kwargs)
                index_name = kwargs.get('name', str(index_spec))
                logger.debug(f"✅ Created index {index_name} on {collection.name}")
                return True
            except Exception as e:
                index_name = kwargs.get('name', str(index_spec))
                logger.warning(f"⚠️ Failed to create index {index_name} on {collection.name}: {str(e)}")
                return False

        try:
            # Instagram collection indexes
            instagram_collection = self.db[self.collections['instagram']]
            safe_create_index(instagram_collection, [("url", 1)], unique=True, name="url_1")
            safe_create_index(instagram_collection, [("username", 1)], name="username_1")
            safe_create_index(instagram_collection, [("content_type", 1)], name="content_type_1")
            safe_create_index(instagram_collection, [("scraped_at", -1)], name="scraped_at_-1")
            
            # LinkedIn collection indexes
            linkedin_collection = self.db[self.collections['linkedin']]
            safe_create_index(linkedin_collection, [("url", 1)], unique=True, name="url_1")
            safe_create_index(linkedin_collection, [("username", 1)], name="username_1")
            safe_create_index(linkedin_collection, [("url_type", 1)], name="url_type_1")
            safe_create_index(linkedin_collection, [("scraping_timestamp", -1)], name="scraping_timestamp_-1")
            
            # Web collection indexes
            web_collection = self.db[self.collections['web']]
            try:
                web_collection.drop_index([("url", 1)])
            except Exception:
                pass
            safe_create_index(web_collection, [("source_url", 1)], name="source_url_1")
            safe_create_index(web_collection, [("domain", 1)], name="domain_1")
            safe_create_index(web_collection, [("scraped_at", -1)], name="scraped_at_-1")
            
            # YouTube collection indexes
            youtube_collection = self.db[self.collections['youtube']]
            safe_create_index(youtube_collection, [("url", 1)], unique=True, name="url_1")
            safe_create_index(youtube_collection, [("channel_name", 1)], name="channel_name_1")
            safe_create_index(youtube_collection, [("content_type", 1)], name="content_type_1")
            safe_create_index(youtube_collection, [("scraped_at", -1)], name="scraped_at_-1")

            # Unified collection indexes
            unified_collection = self.db[self.collections['unified']]
            safe_create_index(unified_collection, [("url", 1)], unique=True, name="url_1")
            safe_create_index(unified_collection, [("platform", 1)], name="platform_1")
            safe_create_index(unified_collection, [("content_type", 1)], name="content_type_1")
            safe_create_index(unified_collection, [("profile.username", 1)], name="profile.username_1")
            safe_create_index(unified_collection, [("contact.emails", 1)], name="contact.emails_1")
            safe_create_index(unified_collection, [("metadata.scraped_at", -1)], name="metadata.scraped_at_-1")
            safe_create_index(unified_collection, [("source", 1)], name="source_1")
            
            # Additional field indexes for better query performance
            safe_create_index(unified_collection, [("industry", 1)], name="industry_1")
            safe_create_index(unified_collection, [("company_name", 1)], name="company_name_1")
            safe_create_index(unified_collection, [("lead_category", 1)], name="lead_category_1")
            safe_create_index(unified_collection, [("lead_sub_category", 1)], name="lead_sub_category_1")
            safe_create_index(unified_collection, [("company_type", 1)], name="company_type_1")
            safe_create_index(unified_collection, [("bdr", 1)], name="bdr_1")
            safe_create_index(unified_collection, [("icp_identifier", 1)], name="icp_identifier_1")
            
            logger.info("✅ Index creation process completed. Some indexes may not have been created due to permissions.")
            
        except Exception as e:
            logger.warning(f"⚠️ Error during index creation process: {e}")

    def insert_unified_lead(self, lead_data: Dict[str, Any]) -> bool:
        """
        Insert lead data into the unified collection using standardized schema
        
        Args:
            lead_data: Lead data dictionary following the unified schema
            
        Returns:
            bool: Success status
        """
        try:
            # Validate required fields
            if 'url' not in lead_data:
                raise ValueError("URL is required for unified lead")
            if 'platform' not in lead_data:
                raise ValueError("Platform is required for unified lead")
            
            # Ensure nested objects exist
            if 'profile' not in lead_data:
                lead_data['profile'] = {}
            if 'contact' not in lead_data:
                lead_data['contact'] = {}
            if 'content' not in lead_data:
                lead_data['content'] = {}
            if 'metadata' not in lead_data:
                lead_data['metadata'] = {}
            
            # Add/update metadata
            lead_data['metadata']['scraped_at'] = datetime.utcnow()
            
            # Allow all content types (profile, article, video, etc.) in unified collection
            content_type = lead_data.get('content_type', '').lower()
            
            # Ensure ICP identifier exists
            if 'icp_identifier' not in lead_data:
                lead_data['icp_identifier'] = 'default'
            
            # Validate with generic unified rules before inserting
            if not self._is_valid_unified_lead(lead_data):
                logger.info(f"ℹ️ Skipped invalid unified lead (failed validation): {lead_data.get('url', 'unknown')}")
                return False

            # Insert into unified collection
            result = self.db[self.collections['unified']].insert_one(lead_data)
            
            logger.info(f"✅ Unified lead inserted with ID: {result.inserted_id}")
            return True
            
        except DuplicateKeyError:
            logger.warning(f"⚠️ Unified lead already exists for URL: {lead_data.get('url')}")
            return False
        except Exception as e:
            logger.error(f"❌ Failed to insert unified lead: {e}")
            return False
    
    def insert_batch_unified_leads(self, leads_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Insert multiple leads into unified collection in batch
        
        Args:
            leads_data: List of lead data dictionaries following unified schema
            
        Returns:
            Dict with success and failure counts
        """
        success_count = 0
        failure_count = 0
        duplicate_count = 0
        
        for lead_data in leads_data:
            try:
                # Validate and prepare data
                if 'url' not in lead_data:
                    failure_count += 1
                    logger.error("❌ Missing required field 'url' in lead data")
                    continue
                
                if 'platform' not in lead_data:
                    failure_count += 1
                    logger.error("❌ Missing required field 'platform' in lead data")
                    continue
                
                # Ensure nested objects exist
                if 'profile' not in lead_data:
                    lead_data['profile'] = {}
                if 'contact' not in lead_data:
                    lead_data['contact'] = {}
                if 'content' not in lead_data:
                    lead_data['content'] = {}
                if 'metadata' not in lead_data:
                    lead_data['metadata'] = {}
                
                # Add metadata
                lead_data['metadata']['scraped_at'] = datetime.utcnow()
                
                # Allow all content types (profile, article, video, etc.) in unified collection
                content_type = lead_data.get('content_type', '').lower()
                
                # Ensure ICP identifier exists
                if 'icp_identifier' not in lead_data:
                    lead_data['icp_identifier'] = 'default'
                
                # Validate with generic unified rules before inserting
                if not self._is_valid_unified_lead(lead_data):
                    logger.info(f"ℹ️ Skipped invalid unified lead (failed validation): {lead_data.get('url', 'unknown')}")
                    failure_count += 1
                    continue

                # Insert into unified collection
                result = self.db[self.collections['unified']].insert_one(lead_data)
                success_count += 1
                
            except DuplicateKeyError:
                duplicate_count += 1
                logger.warning(f"⚠️ Duplicate unified lead for URL: {lead_data.get('url')}")
            except Exception as e:
                failure_count += 1
                logger.error(f"❌ Failed to insert unified lead: {e}")
        
        logger.info(f"📊 Unified batch insert completed - Success: {success_count}, Duplicates: {duplicate_count}, Failures: {failure_count}")
        
        return {
            'success_count': success_count,
            'duplicate_count': duplicate_count,
            'failure_count': failure_count,
            'total_processed': len(leads_data)
        }
    
    def get_unified_leads(self, limit: int = 100, skip: int = 0, filters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Get leads from unified collection
        
        Args:
            limit: Maximum number of results
            skip: Number of results to skip
            filters: Optional MongoDB query filters
            
        Returns:
            List of unified lead documents
        """
        try:
            query = filters or {}
            cursor = self.db[self.collections['unified']].find(query).sort('metadata.scraped_at', -1).skip(skip).limit(limit)
            return list(cursor)
        except Exception as e:
            logger.error(f"❌ Failed to get unified leads: {e}")
            return []
    
    def get_collection(self, collection_name: str):
        """Get a collection by name - used by crl.py"""
        if collection_name in self.collections.values():
            return self.db[collection_name]
        elif collection_name in self.collections:
            return self.db[self.collections[collection_name]]
        else:
            # Return the collection directly if it's a valid collection name
            return self.db[collection_name]
            
    def search_unified_leads(self, query: Dict[str, Any], limit: int = 100) -> List[Dict[str, Any]]:
        """
        Search unified leads collection
        
        Args:
            query: MongoDB query dictionary
            limit: Maximum number of results
            
        Returns:
            List of matching unified lead documents
        """
        try:
            cursor = self.db[self.collections['unified']].find(query).limit(limit)
            return list(cursor)
        except Exception as e:
            logger.error(f"❌ Failed to search unified leads: {e}")
            return []

    def insert_instagram_lead(self, lead_data: Dict[str, Any]) -> bool:
        """
        Insert Instagram lead data into MongoDB
        
        Args:
            lead_data: Instagram lead data dictionary
            
        Returns:
            bool: Success status
        """
        try:
            # Add metadata
            lead_data['scraped_at'] = datetime.utcnow()
            lead_data['source'] = 'instagram_scraper'
            
            # Insert into Instagram collection
            result = self.db[self.collections['instagram']].insert_one(lead_data)
            
            logger.info(f"✅ Instagram lead inserted with ID: {result.inserted_id}")
            return True
            
        except DuplicateKeyError:
            logger.warning(f"⚠️ Instagram lead already exists for URL: {lead_data.get('url')}")
            return False
        except Exception as e:
            logger.error(f"❌ Failed to insert Instagram lead: {e}")
            return False
    
    def insert_linkedin_lead(self, lead_data: Dict[str, Any]) -> bool:
        """
        Insert LinkedIn lead data into MongoDB
        
        Args:
            lead_data: LinkedIn lead data dictionary
            
        Returns:
            bool: Success status
        """
        try:
            # Add metadata
            lead_data['scraped_at'] = datetime.utcnow()
            lead_data['source'] = 'linkedin_scraper'
            
            # Insert into LinkedIn collection
            result = self.db[self.collections['linkedin']].insert_one(lead_data)
            
            logger.info(f"✅ LinkedIn lead inserted with ID: {result.inserted_id}")
            return True
            
        except DuplicateKeyError:
            logger.warning(f"⚠️ LinkedIn lead already exists for URL: {lead_data.get('url')}")
            return False
        except Exception as e:
            logger.error(f"❌ Failed to insert LinkedIn lead: {e}")
            return False
    
    def insert_web_lead(self, lead_data: Dict[str, Any]) -> bool:
        """
        Insert web lead data into MongoDB
        
        Args:
            lead_data: Web lead data dictionary
            
        Returns:
            bool: Success status
        """
        try:
            # Add metadata
            lead_data['scraped_at'] = datetime.utcnow()
            lead_data['source'] = 'web_scraper'
            
            # Insert into web collection
            result = self.db[self.collections['web']].insert_one(lead_data)
            
            logger.info(f"✅ Web lead inserted with ID: {result.inserted_id}")
            return True
            
        except DuplicateKeyError:
            logger.warning(f"⚠️ Web lead already exists for URL: {lead_data.get('url')}")
            return False
        except Exception as e:
            logger.error(f"❌ Failed to insert web lead: {e}")
            return False
    
    def insert_youtube_lead(self, lead_data: Dict[str, Any]) -> bool:
        """
        Insert YouTube lead data into MongoDB
        
        Args:
            lead_data: YouTube lead data dictionary
            
        Returns:
            bool: Success status
        """
        try:
            # Add metadata
            lead_data['scraped_at'] = datetime.utcnow()
            lead_data['source'] = 'youtube_scraper'
            
            # Insert into YouTube collection
            result = self.db[self.collections['youtube']].insert_one(lead_data)
            
            logger.info(f"✅ YouTube lead inserted with ID: {result.inserted_id}")
            return True
            
        except DuplicateKeyError:
            logger.warning(f"⚠️ YouTube lead already exists for URL: {lead_data.get('url')}")
            return False
        except Exception as e:
            logger.error(f"❌ Failed to insert YouTube lead: {e}")
            return False
    
    def insert_batch_leads(self, leads_data: List[Dict[str, Any]], source: str) -> Dict[str, int]:
        """
        Insert multiple leads from a batch operation
        
        Args:
            leads_data: List of lead data dictionaries
            source: Source scraper ('instagram', 'linkedin', 'web', 'youtube', 'company_directory')
            
        Returns:
            Dict with success and failure counts
        """
        if source not in self.collections:
            raise ValueError(f"Invalid source: {source}. Must be one of {list(self.collections.keys())}")
        
        success_count = 0
        failure_count = 0
        duplicate_count = 0
        
        for lead_data in leads_data:
            try:
                # Add metadata
                lead_data['scraped_at'] = datetime.utcnow()
                lead_data['source'] = f'{source}_scraper'
                
                # Insert into appropriate collection
                result = self.db[self.collections[source]].insert_one(lead_data)
                success_count += 1
                
            except DuplicateKeyError:
                duplicate_count += 1
                logger.warning(f"⚠️ Duplicate lead for URL: {lead_data.get('url')}")
            except Exception as e:
                failure_count += 1
                logger.error(f"❌ Failed to insert lead: {e}")
        
        logger.info(f"📊 Batch insert completed - Success: {success_count}, Duplicates: {duplicate_count}, Failures: {failure_count}")
        
        return {
            'success_count': success_count,
            'duplicate_count': duplicate_count,
            'failure_count': failure_count,
            'total_processed': len(leads_data)
        }
    
    def get_leads_by_source(self, source: str, limit: int = 100, skip: int = 0) -> List[Dict[str, Any]]:
        """
        Get leads from a specific source
        
        Args:
            source: Source scraper ('instagram', 'linkedin', 'web', 'youtube', 'company_directory')
            limit: Maximum number of results
            skip: Number of results to skip
            
        Returns:
            List of lead documents
        """
        if source not in self.collections:
            raise ValueError(f"Invalid source: {source}. Must be one of {list(self.collections.keys())}")
        
        try:
            cursor = self.db[self.collections[source]].find().sort('scraped_at', -1).skip(skip).limit(limit)
            return list(cursor)
        except Exception as e:
            logger.error(f"❌ Failed to get leads from {source}: {e}")
            return []
    
    def search_leads(self, query: Dict[str, Any], source: str = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Search leads across collections
        
        Args:
            query: MongoDB query dictionary
            source: Specific source to search (optional)
            limit: Maximum number of results
            
        Returns:
            List of matching lead documents
        """
        try:
            if source:
                if source not in self.collections:
                    raise ValueError(f"Invalid source: {source}")
                cursor = self.db[self.collections[source]].find(query).limit(limit)
                return list(cursor)
            else:
                # Search across all collections
                results = []
                for collection_name in self.collections.values():
                    cursor = self.db[collection_name].find(query).limit(limit)
                    results.extend(list(cursor))
                return results
        except Exception as e:
            logger.error(f"❌ Failed to search leads: {e}")
            return []
    
    def get_all_urls(self, source: str = 'web', include_metadata: bool = True) -> List[Dict[str, Any]]:
        """
        Get all URLs from the specified collection
        
        Args:
            source: Source collection to retrieve URLs from ('web', 'instagram', 'linkedin', 'youtube', 'company_directory')
            include_metadata: Whether to include scraped_at and other metadata
            
        Returns:
            List of URL data dictionaries
        """
        if source not in self.collections:
            raise ValueError(f"Invalid source: {source}. Must be one of {list(self.collections.keys())}")
        
        try:
            # Define projection based on source and metadata preference
            projection = {}
            if not include_metadata:
                projection = {'_id': 0, 'scraped_at': 0, 'source': 0}
            
            # Get all documents from the specified collection
            cursor = self.db[self.collections[source]].find({}, projection).sort('scraped_at', -1)
            urls_data = list(cursor)
            
            logger.info(f"✅ Retrieved {len(urls_data)} URLs from {source} collection")
            return urls_data
            
        except Exception as e:
            logger.error(f"❌ Failed to get URLs from {source} collection: {e}")
            return []
    
    def get_urls_by_domain(self, domain: str, source: str = 'web') -> List[Dict[str, Any]]:
        """
        Get URLs filtered by domain
        
        Args:
            domain: Domain to filter by
            source: Source collection ('web', 'instagram', 'linkedin', 'youtube')
            
        Returns:
            List of URL data dictionaries from the specified domain
        """
        if source not in self.collections:
            raise ValueError(f"Invalid source: {source}. Must be one of {list(self.collections.keys())}")
        
        try:
            query = {'domain': domain}
            cursor = self.db[self.collections[source]].find(query).sort('scraped_at', -1)
            urls_data = list(cursor)
            
            logger.info(f"✅ Retrieved {len(urls_data)} URLs from domain '{domain}' in {source} collection")
            return urls_data
            
        except Exception as e:
            logger.error(f"❌ Failed to get URLs from domain '{domain}': {e}")
            return []

    def get_unified_field_statistics(self, collection_name: str = 'unified') -> Dict[str, Any]:
        """
        Get statistics about the new additional fields in the unified collection
        
        Args:
            collection_name: Name of the collection to analyze (default: 'unified')
            
        Returns:
            Dict with field statistics
        """
        try:
            collection = self.db[collection_name]
            
            # Define the additional fields to analyze
            additional_fields = [
                'industry', 'revenue', 'lead_category', 'lead_sub_category',
                'company_name', 'company_type', 'decision_makers', 'bdr',
                'product_interests', 'timeline', 'interest_level'
            ]
            
            stats = {}
            
            for field in additional_fields:
                # Count non-null values
                non_null_count = collection.count_documents({field: {'$ne': None, '$ne': ''}})
                total_count = collection.count_documents({})
                
                # Get distinct values for categorical fields
                distinct_values = []
                if field in ['industry', 'lead_category', 'lead_sub_category', 'company_type', 'bdr']:
                    distinct_values = list(collection.distinct(field, {field: {'$ne': None, '$ne': ''}}))
                
                stats[field] = {
                    'total_count': total_count,
                    'non_null_count': non_null_count,
                    'null_count': total_count - non_null_count,
                    'completion_rate': (non_null_count / total_count * 100) if total_count > 0 else 0,
                    'distinct_values': distinct_values[:10] if distinct_values else []  # Limit to first 10
                }
            
            # Platform-specific statistics
            platform_stats = {}
            for platform in ['web', 'instagram', 'linkedin', 'youtube']:
                platform_count = collection.count_documents({'platform': platform})
                if platform_count > 0:
                    platform_stats[platform] = {
                        'total_leads': platform_count,
                        'with_company_info': collection.count_documents({
                            'platform': platform,
                            'company_name': {'$ne': None, '$ne': ''}
                        }),
                        'with_industry': collection.count_documents({
                            'platform': platform,
                            'industry': {'$ne': None, '$ne': ''}
                        }),
                        'with_lead_category': collection.count_documents({
                            'platform': platform,
                            'lead_category': {'$ne': None, '$ne': ''}
                        })
                    }
            
            stats['platform_breakdown'] = platform_stats
            
            logger.info(f"✅ Field statistics generated for {collection_name}")
            return stats
            
        except Exception as e:
            logger.error(f"❌ Failed to get field statistics: {e}")
            return {'error': str(e)}

    def get_database_stats(self) -> Dict[str, Any]:
        """
        Get database statistics
        
        Returns:
            Dictionary with collection statistics
        """
        try:
            stats = {}
            for source, collection_name in self.collections.items():
                count = self.db[collection_name].count_documents({})
                stats[source] = count
            
            stats['total_leads'] = sum(stats.values())
            return stats
        except Exception as e:
            logger.error(f"❌ Failed to get database stats: {e}")
            return {}
    
    def close_connection(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("🔌 MongoDB connection closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()

    def _validate_instagram_lead(self, instagram_data: Dict[str, Any]) -> bool:
        """
        Validate Instagram lead data before saving to unified_leads collection
        
        Validation rules:
        1. If email or phone is present, the lead is valid
        2. If email and phone are both empty, lead is valid if at least one of username or full_name is present
        3. Otherwise, skip the lead
        
        Args:
            instagram_data: Instagram lead data dictionary
            
        Returns:
            bool: True if lead is valid, False if should be skipped
        """
        try:
            def safe_str(value: Any) -> str:
                return value.strip() if isinstance(value, str) else ''

            # Check if email or phone is present
            business_email = safe_str(instagram_data.get('business_email'))
            business_phone = safe_str(instagram_data.get('business_phone_number'))
            
            if business_email or business_phone:
                logger.debug(f"✅ Instagram lead valid - has contact info: {instagram_data.get('username', 'unknown')}")
                return True
            
            # If no contact info, check minimal profile information per unified rules
            username = safe_str(instagram_data.get('username'))
            full_name = safe_str(instagram_data.get('full_name'))
            
            if username or full_name:
                logger.debug(f"✅ Instagram lead valid - has identifier: {username or full_name}")
                return True
            
            # Lead doesn't meet validation criteria
            logger.warning(f"⚠️ Instagram lead invalid - insufficient data: {username or 'unknown'}")
            logger.warning(f"   - Email: {business_email or 'None'}")
            logger.warning(f"   - Phone: {business_phone or 'None'}")
            logger.warning(f"   - Username: {username or 'None'}")
            logger.warning(f"   - Full name: {full_name or 'None'}")
            logger.warning(f"   - Bio: {bio or 'None'}")
            return False
            
        except Exception as e:
            logger.error(f"❌ Error validating Instagram lead: {e}")
            return False

    def transform_instagram_to_unified(self, instagram_data: Dict[str, Any], icp_identifier: str = 'default') -> Dict[str, Any]:
        """Transform Instagram data to unified schema"""
        unified_data = {
            "url": instagram_data.get('url', ""),
            "platform": "instagram",
            "content_type": instagram_data.get('content_type', ""),
            "source": "instagram-scraper",
            "icp_identifier": icp_identifier,
            "profile": {
                "username": instagram_data.get('username', ""),
                "full_name": instagram_data.get('full_name', ""),
                "bio": instagram_data.get('biography', ""),
                "location": "",
                "job_title": instagram_data.get('business_category_name', ""),
                "employee_count": ""
            },
            "contact": {
                "emails": [instagram_data.get('business_email')] if instagram_data.get('business_email') else [],
                "phone_numbers": [instagram_data.get('business_phone_number')] if instagram_data.get('business_phone_number') else [],
                "address": "",
                "websites": [],
                "social_media_handles": {
                    "instagram": instagram_data.get('username', ""),
                    "twitter": "",
                    "facebook": "",
                    "linkedin": "",
                    "youtube": "",
                    "tiktok": "",
                    "other": []
                },
                "bio_links": instagram_data.get('bio_links', [])
            },
            "content": {
                "caption": instagram_data.get('caption', ""),
                "upload_date": "",
                "channel_name": "",
                "author_name": instagram_data.get('username', "")
            },
            "metadata": {
                "scraped_at": instagram_data.get('scraped_at', ""),
                "data_quality_score": "0.45"
            },
            # Additional fields (empty for Instagram)
            "industry": None,
            "revenue": None,
            "lead_category": None,
            "lead_sub_category": None,
            "company_name": instagram_data.get('full_name', ""),
            "company_type": None,
            "decision_makers": None,
            "bdr": "AKG",
            "product_interests": None,
            "timeline": None,
            "interest_level": None
        }
        
        # Clean up None values in nested objects
        return self._clean_unified_data(unified_data)

    def is_invalid_linkedin_data(self, name: str) -> bool:
        """
        Check if LinkedIn data should be skipped due to invalid/generic names
        """
        full_name = name.strip().lower() if name else ""
        
        # List of invalid/generic names to skip
        invalid_names = [
            'sign up',
            'signup',
            'log in',
            'login',
            'register',
            'join now',
            'get started',
            'create account',
            'sign in',
            'signin',
            'continue',
            'next',
            'submit',
            'loading',
            'please wait',
            'error',
            'page not found',
            '404',
            'access denied',
            'unauthorized',
            'linkedin',
            'connect',
            'follow',
            'view profile'
        ]
        
        # Check if full_name is empty or matches invalid patterns
        return not full_name or full_name in invalid_names

    def transform_linkedin_to_unified(self, linkedin_data: Dict[str, Any], icp_identifier: str = 'default') -> Dict[str, Any]:
        """Transform LinkedIn data to unified schema"""
        # Skip if invalid data
        full_name = linkedin_data.get('author_name') or linkedin_data.get('full_name')
        if self.is_invalid_linkedin_data(full_name):
            return None
            
        unified_data = {
            "url": linkedin_data.get('url', ""),
            "platform": "linkedin", 
            "content_type": self._map_linkedin_content_type(linkedin_data.get('url_type', '')),
            "source": "linkedin-scraper",
            "icp_identifier": icp_identifier,
            "profile": {
                "username": linkedin_data.get('username', ""),
                "full_name": linkedin_data.get('full_name') or linkedin_data.get('author_name', ""),
                "bio": linkedin_data.get('about') or linkedin_data.get('about_us', ""),
                "location": linkedin_data.get('location', ""),
                "job_title": linkedin_data.get('job_title', ""),
                "employee_count": str(linkedin_data.get('employee_count')) if linkedin_data.get('employee_count') else ""
            },
            "contact": {
                "emails": [],
                "phone_numbers": [],
                "address": linkedin_data.get('address', ""),
                "websites": [linkedin_data.get('website')] if linkedin_data.get('website') else [],
                "social_media_handles": {
                    "instagram": "",
                    "twitter": "",
                    "facebook": "",
                    "linkedin": linkedin_data.get('username') or linkedin_data.get('author_url', ""),
                    "youtube": "",
                    "tiktok": "",
                    "other": []
                },
                "bio_links": []
            },
            "content": {
                "caption": linkedin_data.get('headline', ""),
                "upload_date": linkedin_data.get('date_published', ""),
                "channel_name": "",
                "author_name": linkedin_data.get('author_name') or linkedin_data.get('full_name', "")
            },
            "metadata": {
                "scraped_at": datetime.utcnow(),
                "data_quality_score": "0.45"
            },
            # Additional fields (empty for LinkedIn)
            "industry": None,
            "revenue": None,
            "lead_category": None,
            "lead_sub_category": None,
            "company_name": linkedin_data.get('full_name', ""),
            "company_type": None,
            "decision_makers": None,
            "bdr": "AKG",
            "product_interests": None,
            "timeline": None,
            "interest_level": None
        }
        
        return self._clean_unified_data(unified_data)

    def transform_youtube_to_unified(self, youtube_data: Dict[str, Any], icp_identifier: str = 'default') -> Dict[str, Any]:
        """Transform YouTube data to unified schema"""
         # Extract social media handles from the nested structure
        social_media_data = youtube_data.get('social_media_handles', {})
        
        # Helper function to extract first handle from a list or return empty string
        def get_first_handle(handles_list):
            if handles_list and isinstance(handles_list, list) and len(handles_list) > 0:
                return handles_list[0].get('username', '') if isinstance(handles_list[0], dict) else handles_list[0]
            return ""
        
        # Helper function to extract all URLs from social media handles
        def get_bio_links():
            links = []
            for platform, handles in social_media_data.items():
                if handles and isinstance(handles, list):
                    for handle in handles:
                        if isinstance(handle, dict) and 'url' in handle:
                            links.append(handle['url'])
            return links

        unified_data = {
            "url": youtube_data.get('url', ""),
            "platform": "youtube",
            "content_type": youtube_data.get('content_type', ""),
            "source": "youtube-scraper",
            "icp_identifier": icp_identifier,
            "profile": {
                "username": "",
                "full_name": youtube_data.get('channel_name', ""),
                "bio": youtube_data.get('description', ""),
                "location": "",
                "job_title": "",
                "employee_count": ""
            },
            "contact": {
                "emails": [youtube_data.get('email')] if youtube_data.get('email') else [],
                "phone_numbers": [],
                "address": "",
                "websites": [],
                "social_media_handles": {
                    "instagram": get_first_handle(social_media_data.get('instagram')),
                    "twitter": get_first_handle(social_media_data.get('twitter')),
                    "facebook": get_first_handle(social_media_data.get('facebook')),
                    "linkedin": get_first_handle(social_media_data.get('linkedin')),
                    "youtube": youtube_data.get('channel_name') or youtube_data.get('username'),
                    "tiktok": get_first_handle(social_media_data.get('tiktok')),
                    "other": []
                },
                "bio_links": get_bio_links()
            },
            "content": {
                "caption": youtube_data.get('title',""),
                "upload_date": youtube_data.get('upload_date',""),
                "channel_name": youtube_data.get('channel_name', ""),
                "author_name": ""
            },
            "metadata": {
                "scraped_at": datetime.utcnow(),
                "data_quality_score": "0.45"
            },
            # Additional fields (empty for YouTube)
            "industry": None,
            "revenue": None,
            "lead_category": None,
            "lead_sub_category": None,
            "company_name": youtube_data.get('channel_name', ""),
            "company_type": None,
            "decision_makers": None,
            "bdr": "AKG",
            "product_interests": None,
            "timeline": None,
            "interest_level": None
        }
        
        return self._clean_unified_data(unified_data)

    def transform_web_to_unified(self, web_data: Dict[str, Any], icp_identifier: str = 'default') -> Dict[str, Any]:
        """Transform web scraper data to unified schema"""
        
        # Helper function to extract emails from various sources
        def extract_emails(data):
            emails = []
            # From email array
            email_list = data.get('email', [])
            if isinstance(email_list, list):
                emails.extend([email.strip() for email in email_list if email and isinstance(email, str) and email.strip()])
            
            # From ai_leads.ai_contacts
            ai_leads = data.get('ai_leads', [])
            if isinstance(ai_leads, list):
                for ai_lead in ai_leads:
                    if isinstance(ai_lead, dict):
                        ai_contacts = ai_lead.get('ai_contacts', [])
                        if isinstance(ai_contacts, list):
                            for contact in ai_contacts:
                                if isinstance(contact, dict):
                                    email = contact.get('email')
                                    if email and isinstance(email, str) and email.strip():
                                        emails.append(email.strip())
            return list(dict.fromkeys(emails))  # Remove duplicates
        
        # Helper function to extract phone numbers
        def extract_phones(data):
            phones = []
            # From phone array
            phone_list = data.get('phone', [])
            if isinstance(phone_list, list):
                phones.extend([phone.strip() for phone in phone_list if phone and isinstance(phone, str) and phone.strip()])
            
            # From ai_leads.ai_contacts
            ai_leads = data.get('ai_leads', [])
            if isinstance(ai_leads, list):
                for ai_lead in ai_leads:
                    if isinstance(ai_lead, dict):
                        ai_contacts = ai_lead.get('ai_contacts', [])
                        if isinstance(ai_contacts, list):
                            for contact in ai_contacts:
                                if isinstance(contact, dict):
                                    phone = contact.get('phone')
                                    if phone and isinstance(phone, str) and phone.strip():
                                        phones.append(phone.strip())
            return list(dict.fromkeys(phones))  # Remove duplicates
        
        # Helper function to get value with fallback from ai_leads
        def get_value_with_fallback(primary_path: List[str], fallback_key: str, default_value: str = ""):
            ai_leads = web_data.get('ai_leads')
            if ai_leads and isinstance(ai_leads, list) and len(ai_leads) > 0:
                current = ai_leads[0]
                if current and isinstance(current, dict):
                    for key in primary_path:
                        if isinstance(current, dict) and key in current and current[key] is not None:
                            current = current[key]
                        else:
                            current = None
                            break
                    if current is not None:
                        return str(current).strip() if current else default_value
            
            fallback_value = web_data.get(fallback_key)
            return str(fallback_value).strip() if fallback_value else default_value
        
        # Extract social media handles
        social_media = web_data.get('social_media', {})
        if not isinstance(social_media, dict):
            social_media = {}
        
        # Extract lead category & sub-category from ai_leads.ai_contacts
        lead_category, lead_sub_category = '', ''
        ai_leads = web_data.get('ai_leads')
        if ai_leads and isinstance(ai_leads, list):
            for ai_lead in ai_leads:
                if not ai_lead or not isinstance(ai_lead, dict):
                    continue
                ai_contacts = ai_lead.get('ai_contacts')
                if ai_contacts and isinstance(ai_contacts, list):
                    for contact in ai_contacts:
                        if not contact or not isinstance(contact, dict):
                            continue
                        if not lead_category:
                            lead_category = str(contact.get('lead_category', '')).strip()
                        if not lead_sub_category:
                            lead_sub_category = str(contact.get('lead_sub_category', '')).strip()
                        # break early if both found
                        if lead_category and lead_sub_category:
                            break
        
        unified_data = {
            "url": web_data.get('source_url', ''),
            "platform": "web",
            "content_type": "profile",  # Web scraper typically extracts company/profile data
            "source": "web-scraper",
            "icp_identifier": icp_identifier,
            "profile": {
                "username": "",  # Web scraper doesn't typically have usernames
                "full_name": get_value_with_fallback(['organization_info', 'primary_name'], 'business_name'),
                "bio": "",
                "location": get_value_with_fallback(['organization_info', 'location'], 'location'),
                "job_title": "",  # Not typically available in web scraper data
                "employee_count": "1000"
            },
            "contact": {
                "emails": extract_emails(web_data),
                "phone_numbers": extract_phones(web_data),
                "address": get_value_with_fallback(['organization_info', 'location'], ''),  # Only from AI, empty string as fallback
                "websites": [web_data.get('source_url')] if web_data.get('source_url') else [],
                "social_media_handles": {
                    "instagram": social_media.get('instagram'),
                    "twitter": social_media.get('twitter'),
                    "facebook": social_media.get('facebook'),
                    "linkedin": social_media.get('linkedin'),
                    "youtube": social_media.get('youtube'),
                    "tiktok": social_media.get('tiktok'),
                    "other": [v for k, v in social_media.items() if k not in ['instagram', 'twitter', 'facebook', 'linkedin', 'youtube', 'tiktok'] and v]
                },
                "bio_links": []  # Not typically available in web scraper data
            },
            "content": {
                "caption": "",  # Not applicable for web scraper
                "upload_date": "",  # Not applicable for web scraper
                "channel_name": "",  # Not applicable for web scraper
                "author_name": ""  # Not applicable for web scraper
            },
            "metadata": {
                "scraped_at": web_data.get('extraction_timestamp', datetime.utcnow()),
                "data_quality_score": "0.45"
            },
            # Additional fields for web scraper
            "industry": get_value_with_fallback(['organization_info', 'industry'], 'industry'),
            "revenue": "100k",  # Default value as per filter_web_lead.py
            "lead_category": lead_category,
            "lead_sub_category": lead_sub_category,
            "company_name": get_value_with_fallback(['organization_info', 'primary_name'], 'business_name'),
            "company_type": get_value_with_fallback(['organization_info', 'organization_type'], 'company_type'),
            "decision_makers": web_data.get('contact_person', ''),
            "bdr": "AKG",  # Default value as per requirements
            "product_interests": None,  # Will be populated if available
            "timeline": None,  # Will be populated if available
            "interest_level": None  # Will be populated if available
        }
        
        return self._clean_unified_data(unified_data)

    def _calculate_web_data_quality_score(self, data: Dict[str, Any]) -> float:
        """Calculate data quality score specifically for web scraper data"""
        total_fields = 0
        filled_fields = 0
        
        # Check key fields for web data
        key_fields = ['business_name', 'source_url']
        for field in key_fields:
            total_fields += 1
            if data.get(field):
                filled_fields += 1
        
        # Check contact fields
        if data.get('email') and isinstance(data.get('email'), list) and len(data.get('email')) > 0:
            filled_fields += 1
        total_fields += 1
        
        if data.get('phone') and isinstance(data.get('phone'), list) and len(data.get('phone')) > 0:
            filled_fields += 1
        total_fields += 1
        
        # Check ai_leads data quality
        ai_leads = data.get('ai_leads')
        if ai_leads and isinstance(ai_leads, list) and len(ai_leads) > 0:
            filled_fields += 1
        total_fields += 1
        
        return filled_fields / total_fields if total_fields > 0 else 0.0

    def _map_linkedin_content_type(self, url_type: str) -> str:
        """Map LinkedIn URL type to unified content type"""
        mapping = {
            'profile': 'profile',
            'company': 'profile', 
            'post': 'article',
            'newsletter': 'article'
        }
        return mapping.get(url_type, 'profile')

    def _calculate_data_quality_score(self, data: Dict[str, Any]) -> float:
        #Calculate a simple data quality score (0-1) based on available fields
        total_fields = 0
        filled_fields = 0
        
        # Check key fields that indicate data quality
        key_fields = ['full_name', 'username', 'url']
        for field in key_fields:
            total_fields += 1
            if data.get(field):
                filled_fields += 1
        
        # Check contact fields
        contact_fields = ['business_email', 'business_phone_number', 'website']
        for field in contact_fields:
            total_fields += 1
            if data.get(field):
                filled_fields += 1
        
        return filled_fields / total_fields if total_fields > 0 else 0.0

    def _clean_unified_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean unified data by removing empty nested objects and None values where appropriate"""
        # Clean profile section - but keep essential fields as empty strings if None
        if data.get('profile'):
            profile = data['profile']
            # Essential fields that should always exist
            essential_profile_fields = ['username', 'full_name', 'bio', 'location', 'job_title', 'employee_count']
            for field in essential_profile_fields:
                if field not in profile or profile[field] is None:
                    profile[field] = ''
            # Remove other None/empty values
            data['profile'] = {k: v for k, v in profile.items() if v is not None and v != '' or k in essential_profile_fields}
        
        # Clean contact section
        if data.get('contact'):
            contact = data['contact']
            # Keep arrays even if empty, clean None values from other fields
            for key, value in list(contact.items()):
                if isinstance(value, list):
                    contact[key] = [item for item in value if item is not None and item != '']
                elif isinstance(value, dict):
                    contact[key] = {k: v for k, v in value.items() if v is not None and v != ''}
                elif value is None or value == '':
                    if key not in ['emails', 'phone_numbers', 'websites', 'bio_links']:
                        del contact[key]
        
        # Clean content section
        if data.get('content'):
            data['content'] = {k: v for k, v in data['content'].items() if v is not None and v != ''}
        
        # Clean additional fields - keep them even if None for consistency across platforms
        # These fields are important for the unified schema and should be preserved
        additional_fields = [
            'industry', 'revenue', 'lead_category', 'lead_sub_category', 
            'company_name', 'company_type', 'decision_makers', 'bdr',
            'product_interests', 'timeline', 'interest_level'
        ]
        
        for field in additional_fields:
            if field not in data:
                data[field] = None
        
        return data

    def ensure_unified_schema_compliance(self, collection_name: str = 'unified') -> Dict[str, int]:
        """
        Ensure all documents in the unified collection have the new additional fields
        
        Args:
            collection_name: Name of the collection to update (default: 'unified')
            
        Returns:
            Dict with update statistics
        """
        try:
            collection = self.db[collection_name]
            
            # Define the additional fields that should exist
            additional_fields = {
                'industry': None,
                'revenue': None,
                'lead_category': None,
                'lead_sub_category': None,
                'company_name': None,
                'company_type': None,
                'decision_makers': None,
                'bdr': None,
                'product_interests': None,
                'timeline': None,
                'interest_level': None
            }
            
            # Find documents missing any of these fields
            missing_fields_query = {
                '$or': [
                    {'industry': {'$exists': False}},
                    {'revenue': {'$exists': False}},
                    {'lead_category': {'$exists': False}},
                    {'lead_sub_category': {'$exists': False}},
                    {'company_name': {'$exists': False}},
                    {'company_type': {'$exists': False}},
                    {'decision_makers': {'$exists': False}},
                    {'bdr': {'$exists': False}},
                    {'product_interests': {'$exists': False}},
                    {'timeline': {'$exists': False}},
                    {'interest_level': {'$exists': False}}
                ]
            }
            
            # Update documents to add missing fields
            result = collection.update_many(
                missing_fields_query,
                {'$set': additional_fields}
            )
            
            logger.info(f"✅ Schema compliance update completed for {collection_name}")
            logger.info(f"   - Documents updated: {result.modified_count}")
            logger.info(f"   - Total documents matched: {result.matched_count}")
            
            return {
                'updated_count': result.modified_count,
                'matched_count': result.matched_count,
                'collection': collection_name
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to ensure schema compliance: {e}")
            return {'error': str(e)}

    def _is_duplicate_lead(self, unified_data: Dict[str, Any]) -> bool:
        """
        Check if a lead is a duplicate based on the specified criteria
        
        Args:
            unified_data: Transformed unified lead data
            
        Returns:
            bool: True if duplicate, False if unique
        """
        try:
            # Extract contact information
            contact = unified_data.get('contact', {})
            emails = contact.get('emails', [])
            phones = contact.get('phone_numbers', [])
            
            # Clean and normalize emails and phones
            emails = [email.strip().lower() for email in emails if email and isinstance(email, str)]
            phones = [phone.strip() for phone in phones if phone and isinstance(phone, str)]
            
            # Check for duplicates based on contact information
            if emails or phones:
                # Build query for contact-based duplicates
                contact_query = {"$or": []}
                
                # Check email duplicates
                if emails:
                    contact_query["$or"].append({
                        "contact.emails": {"$in": emails}
                    })
                
                # Check phone duplicates
                if phones:
                    contact_query["$or"].append({
                        "contact.phone_numbers": {"$in": phones}
                    })
                
                # Check if any existing lead matches
                existing_lead = self.db[self.collections['unified']].find_one(contact_query)
                if existing_lead:
                    logger.debug(f"🔍 Found duplicate lead by contact info: {existing_lead.get('_id')}")
                    return True
            
            # If no contact info, check by profile + company info
            else:
                profile = unified_data.get('profile', {})
                full_name = profile.get('full_name')
                full_name = full_name.strip() if full_name and isinstance(full_name, str) else ''
                url = unified_data.get('url')
                url = url.strip() if url and isinstance(url, str) else ''
                company_name = unified_data.get('company_name')
                company_name = company_name.strip() if company_name and isinstance(company_name, str) else ''
                company_type = unified_data.get('company_type')
                company_type = company_type.strip() if company_type and isinstance(company_type, str) else ''
                
                # Only check if we have meaningful data
                if not (full_name or url or company_name):
                    return False
                
                # Build query for profile-based duplicates
                profile_query = {"$and": []}
                
                if full_name:
                    profile_query["$and"].append({
                        "profile.full_name": {"$regex": f"^{re.escape(full_name)}$", "$options": "i"}
                    })
                
                if url:
                    profile_query["$and"].append({
                        "url": {"$regex": f"^{re.escape(url)}$", "$options": "i"}
                    })
                
                if company_name:
                    profile_query["$and"].append({
                        "company_name": {"$regex": f"^{re.escape(company_name)}$", "$options": "i"}
                    })
                
                if company_type:
                    profile_query["$and"].append({
                        "company_type": {"$regex": f"^{re.escape(company_type)}$", "$options": "i"}
                    })
                
                # Check if any existing lead matches
                existing_lead = self.db[self.collections['unified']].find_one(profile_query)
                if existing_lead:
                    logger.debug(f"🔍 Found duplicate lead by profile info: {existing_lead.get('_id')}")
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error checking for duplicate lead: {e}")
            return False  # If error, assume not duplicate to avoid data loss

    # Removed transform-and-insert orchestration method; scrapers/orchestrator will prepare unified data,
    # and only insert_batch_unified_leads will be used for persistence.

    def _is_valid_unified_lead(self, unified_data: Dict[str, Any]) -> bool:
        """
        Generic validation for unified lead prior to insertion.
        Rules:
        - Valid if there is at least one email or phone number in contact.
        - If both are empty/missing, valid if at least one identifier among
          profile.full_name, profile.username, company_name, or content.author_name
          is present (non-empty after trim).
        - Otherwise invalid.
        """
        try:
            contact = unified_data.get('contact', {}) or {}
            emails = contact.get('emails') or []
            phones = contact.get('phone_numbers') or []
            if any(isinstance(e, str) and e.strip() for e in emails) or any(isinstance(p, str) and p.strip() for p in phones):
                return True
            profile = unified_data.get('profile', {}) or {}
            full_name = profile.get('full_name') or ''
            username = profile.get('username') or ''
            company_name = (unified_data.get('company_name') or '')
            content = unified_data.get('content', {}) or {}
            author_name = content.get('author_name') or ''
            if (
                (isinstance(full_name, str) and full_name.strip()) or
                (isinstance(username, str) and username.strip()) or
                (isinstance(company_name, str) and company_name.strip()) or
                (isinstance(author_name, str) and author_name.strip())
            ):
                return True
            return False
        except Exception as e:
            logger.error(f"❌ Error during unified lead validation: {e}")
            return False
    
    def get_unified_leads_without_contacts(self, limit: int = 0) -> List[Dict[str, Any]]:
        """Get unified leads that don't have contact information"""
        try:
            # Query for leads without emails or phone numbers
            query = {
                "$or": [
                    {"contact.emails": {"$exists": False}},
                    {"contact.emails": {"$size": 0}},
                    {"contact.phone_numbers": {"$exists": False}},
                    {"contact.phone_numbers": {"$size": 0}},
                    {"contact": {"$exists": False}}
                ]
            }
            
            cursor = self.db[self.collections['unified']].find(query)
            if limit > 0:
                cursor = cursor.limit(limit)
                
            return list(cursor)
        except Exception as e:
            logger.error(f"Error fetching leads without contacts: {e}")
            return []
    
    def get_leads_by_icp_identifier(self, icp_identifier: str, limit: int = 100, skip: int = 0) -> List[Dict[str, Any]]:
        """
        Get leads from unified collection filtered by ICP identifier
        
        Args:
            icp_identifier: ICP identifier to filter by
            limit: Maximum number of results
            skip: Number of results to skip
            
        Returns:
            List of unified lead documents for the specified ICP
        """
        try:
            from bson import ObjectId
            query = {"icp_identifier": icp_identifier}
            cursor = self.db[self.collections['unified']].find(query).sort('metadata.scraped_at', -1).skip(skip).limit(limit)
            leads = []
            for doc in cursor:
                doc['_id'] = str(doc['_id'])   # convert ObjectId
                if 'metadata' in doc and 'scraped_at' in doc['metadata']:
                    doc['metadata']['scraped_at'] = doc['metadata']['scraped_at'].isoformat()
                leads.append(doc)
            return leads
        except Exception as e:
            logger.error(f"❌ Failed to get leads by ICP identifier: {e}")
            return []
    
    def get_icp_statistics(self, icp_identifier: str) -> Dict[str, Any]:
        """
        Get statistics for a specific ICP identifier
        
        Args:
            icp_identifier: ICP identifier to get statistics for
            
        Returns:
            Dict with statistics for the ICP
        """
        try:
            collection = self.db[self.collections['unified']]
            
            # Total leads for this ICP
            total_leads = collection.count_documents({"icp_identifier": icp_identifier})
            
            # Leads by platform
            platform_stats = {}
            for platform in ['web', 'instagram', 'linkedin', 'youtube']:
                count = collection.count_documents({
                    "icp_identifier": icp_identifier,
                    "platform": platform
                })
                if count > 0:
                    platform_stats[platform] = count
            
            # Leads with contact information
            leads_with_emails = collection.count_documents({
                "icp_identifier": icp_identifier,
                "contact.emails": {"$exists": True, "$ne": [], "$not": {"$size": 0}}
            })
            
            leads_with_phones = collection.count_documents({
                "icp_identifier": icp_identifier,
                "contact.phone_numbers": {"$exists": True, "$ne": [], "$not": {"$size": 0}}
            })
            
            # Recent activity (last 24 hours)
            from datetime import datetime, timedelta
            yesterday = datetime.utcnow() - timedelta(days=1)
            recent_leads = collection.count_documents({
                "icp_identifier": icp_identifier,
                "metadata.scraped_at": {"$gte": yesterday}
            })
            
            return {
                "icp_identifier": icp_identifier,
                "total_leads": total_leads,
                "platform_breakdown": platform_stats,
                "leads_with_emails": leads_with_emails,
                "leads_with_phones": leads_with_phones,
                "recent_leads_24h": recent_leads,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to get ICP statistics: {e}")
            return {"error": str(e)}
    
    def bulk_update_unified_leads(self, operations: List[Dict]) -> Dict[str, int]:
        """Perform bulk update operations"""
        try:
            from pymongo import UpdateOne
            
            bulk_ops = []
            for op in operations:
                bulk_ops.append(
                    UpdateOne(
                        op["filter"],
                        op["update"],
                        upsert=op.get("upsert", False)
                    )
                )
            
            if bulk_ops:
                result = self.db[self.collections['unified']].bulk_write(bulk_ops)
                return {
                    "matched_count": result.matched_count,
                    "modified_count": result.modified_count
                }
            return {"matched_count": 0, "modified_count": 0}
        except Exception as e:
            logger.error(f"Bulk update error: {e}")
            return {"matched_count": 0, "modified_count": 0}

# Global MongoDB manager instance
mongodb_manager = None

def get_mongodb_manager() -> MongoDBManager:
    """Get or create global MongoDB manager instance"""
    global mongodb_manager
    if mongodb_manager is None:
        mongodb_manager = MongoDBManager()
    return mongodb_manager
