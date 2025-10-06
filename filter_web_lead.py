import os
from typing import List, Dict, Any, Optional
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import logging
from bson import ObjectId
import sys

# Add parent directory to path to import database module
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.mongodb_manager import get_mongodb_manager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MongoDBLeadProcessor:
    def __init__(self, mongodb_uri: str = None, database_name: str = None, mongodb_manager=None):
        """
        Initialize MongoDB connection
        """
        self.mongodb_uri = mongodb_uri or os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
        self.database_name = database_name or os.getenv('MONGODB_DATABASE_NAME', 'aiqod-dev')
        self.source_collection = os.getenv('MONGODB_COLLECTION', 'web_leads')
        self.target_collection = 'leadgen_leads'
        self.unified_leads_collection = 'unified_leads'
        
        # Store the unified mongodb_manager if provided
        self.mongodb_manager = mongodb_manager
        
        try:
            self.client = MongoClient(self.mongodb_uri)
            self.db = self.client[self.database_name]
            # Test connection
            self.client.admin.command('ismaster')
            logger.info(f"Connected to MongoDB: {self.database_name}")
            
            # Initialize unified MongoDB manager if not provided
            if not self.mongodb_manager:
                try:
                    self.mongodb_manager = get_mongodb_manager()
                    logger.info("✅ Unified MongoDB manager initialized")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to initialize unified MongoDB manager: {e}")
                    self.mongodb_manager = None
                
        except PyMongoError as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            raise

    def has_email(self, web_lead: Dict[str, Any]) -> bool:
        """
        Check if the web_lead has valid email addresses in either email array or ai_leads.ai_contacts
        """
        if not web_lead or not isinstance(web_lead, dict):
            return False
        
        # Check email array
        email_list = web_lead.get('email')
        if email_list and isinstance(email_list, list) and len(email_list) > 0:
            # Filter out empty, None, or invalid emails
            valid_emails = []
            for email in email_list:
                if email and isinstance(email, str) and email.strip():
                    valid_emails.append(email)
            if valid_emails:
                return True
        
        # Check ai_leads.ai_contacts for emails
        ai_leads = web_lead.get('ai_leads')
        if ai_leads and isinstance(ai_leads, list):
            for ai_lead in ai_leads:
                if not ai_lead or not isinstance(ai_lead, dict):
                    continue
                ai_contacts = ai_lead.get('ai_contacts')
                if ai_contacts and isinstance(ai_contacts, list):
                    for contact in ai_contacts:
                        if not contact or not isinstance(contact, dict):
                            continue
                        email = contact.get('email')
                        if email and isinstance(email, str) and email.strip():
                            return True
        
        return False

    def has_phone(self, web_lead: Dict[str, Any]) -> bool:
        """
        Check if the web_lead has valid phone numbers in either phone array or ai_leads.ai_contacts
        """
        if not web_lead or not isinstance(web_lead, dict):
            return False
        
        # Check phone array
        phone_list = web_lead.get('phone')
        if phone_list and isinstance(phone_list, list) and len(phone_list) > 0:
            # Filter out empty, None, or invalid phones
            valid_phones = []
            for phone in phone_list:
                if phone and isinstance(phone, str) and phone.strip():
                    valid_phones.append(phone)
            if valid_phones:
                return True
        
        # Check ai_leads.ai_contacts for phones
        ai_leads = web_lead.get('ai_leads')
        if ai_leads and isinstance(ai_leads, list):
            for ai_lead in ai_leads:
                if not ai_lead or not isinstance(ai_lead, dict):
                    continue
                ai_contacts = ai_lead.get('ai_contacts')
                if ai_contacts and isinstance(ai_contacts, list):
                    for contact in ai_contacts:
                        if not contact or not isinstance(contact, dict):
                            continue
                        phone = contact.get('phone')
                        if phone and isinstance(phone, str) and phone.strip():
                            return True
        
        return False

    def count_non_empty_fields(self, lead: Dict[str, Any]) -> int:
        """
        Count non-empty fields in a lead to determine which lead has more information
        """
        count = 0
        for key, value in lead.items():
            if key in ['_id', 'original_web_lead_id', 'processed_at', 'primary_contact_type']:
                continue  # Skip metadata fields
            if value and str(value).strip():
                if isinstance(value, dict) and value:
                    count += 1
                elif isinstance(value, str) and value.strip():
                    count += 1
                elif value is not None and not isinstance(value, (dict, list, str)):
                    count += 1
        return count

    def check_and_handle_duplicate(self, target_coll, new_lead: Dict[str, Any]) -> bool:
        """
        Check for duplicate leads and handle them by keeping the one with more information
        
        Returns:
            True if lead should be inserted, False if it's a duplicate with less info
        """
        try:
            email = new_lead.get('Email Address', '').strip()
            phone = new_lead.get('Phone Number', '').strip()
            
            if not email and not phone:
                return True  # No contact info to check against
            
            # Build query to find duplicates
            duplicate_query = {'$or': []}
            
            if email:
                duplicate_query['$or'].append({'Email Address': email})
            if phone:
                duplicate_query['$or'].append({'Phone Number': phone})
            
            # Find existing leads with same email or phone
            existing_leads = list(target_coll.find(duplicate_query))
            
            if not existing_leads:
                return True  # No duplicates found
            
            # Compare information richness
            new_lead_score = self.count_non_empty_fields(new_lead)
            
            for existing_lead in existing_leads:
                existing_lead_score = self.count_non_empty_fields(existing_lead)
                
                # If new lead has more information, replace the existing one
                if new_lead_score > existing_lead_score:
                    logger.info(f"Replacing existing lead {existing_lead.get('_id')} with new lead (more info: {new_lead_score} vs {existing_lead_score})")
                    target_coll.delete_one({'_id': existing_lead['_id']})
                    return True
                elif new_lead_score <= existing_lead_score:
                    logger.info(f"Skipping new lead as existing lead has equal/more info ({existing_lead_score} vs {new_lead_score})")
                    return False
            
            return True
            
        except Exception as e:
            logger.warning(f"Error in duplicate check: {str(e)}")
            return True  # Default to inserting if check fails

    def has_email_or_phone(self, web_lead: Dict[str, Any]) -> bool:
        """
        Check if the web_lead has either valid email addresses or phone numbers
        """
        return self.has_email(web_lead) or self.has_phone(web_lead)

    def extract_lead_data(self, web_lead: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract lead data from filtered web_leads and create separate leads for each email/phone
        """
        if not web_lead or not isinstance(web_lead, dict):
            return []
        
        extracted_leads = []
        
        # Helper function to safely get data with fallback
        def get_value_with_fallback(primary_path: List[str], fallback_key: str, default_value: Any = None) -> Any:
            # Try to get from ai_leads first
            ai_leads = web_lead.get('ai_leads')
            if ai_leads and isinstance(ai_leads, list) and len(ai_leads) > 0:
                current = ai_leads[0]  # Take first ai_lead
                if current and isinstance(current, dict):
                    for key in primary_path:
                        if isinstance(current, dict) and key in current and current[key] is not None:
                            current = current[key]
                        else:
                            current = None
                            break
                    if current is not None:
                        return current
            
            # Fallback to direct key in web_lead
            fallback_value = web_lead.get(fallback_key)
            return fallback_value if fallback_value is not None else default_value
        
        # Helper function to safely get string value
        def safe_str(value: Any, default: str = '') -> str:
            if value is None:
                return default
            return str(value).strip() if str(value).strip() else default
        
        # Helper function to safely get contact name (only single values, not lists)
        def safe_contact_name(value: Any, default: str = '') -> str:
            if value is None:
                return default
            if isinstance(value, list):
                return default  # Return empty if it's a list
            return str(value).strip() if str(value).strip() else default
        
        # Extract company information
        company_name = safe_str(get_value_with_fallback(['organization_info', 'primary_name'], 'business_name'))
        industry = safe_str(get_value_with_fallback(['organization_info', 'industry'], 'industry'))
        company_type = safe_str(get_value_with_fallback(['organization_info', 'organization_type'], 'company_type'))

        # Extract lead category & sub-category from ai_leads.ai_contacts
        lead_category, lead_sub_category = '', ''
        ai_leads = web_lead.get('ai_leads')
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
                            lead_category = safe_str(contact.get('lead_category'))
                        if not lead_sub_category:
                            lead_sub_category = safe_str(contact.get('lead_sub_category'))
                        # break early if both found
                        if lead_category and lead_sub_category:
                            break
    
        # Collect all emails and phones from both sources
        all_emails = []
        all_phones = []
         
        # From email array
        email_list = web_lead.get('email')
        if email_list and isinstance(email_list, list):
            for email in email_list:
                if email and isinstance(email, str):
                    clean_email = email.strip()
                    if clean_email:
                        all_emails.append(clean_email)
        
        # From phone array
        phone_list = web_lead.get('phone')
        if phone_list and isinstance(phone_list, list):
            for phone in phone_list:
                if phone and isinstance(phone, str):
                    clean_phone = phone.strip()
                    if clean_phone:
                        all_phones.append(clean_phone)
        
        # From ai_contacts
        ai_leads = web_lead.get('ai_leads')
        contact_info = {}  # Map email/phone to contact info
        
        if ai_leads and isinstance(ai_leads, list):
            for ai_lead in ai_leads:
                if not ai_lead or not isinstance(ai_lead, dict):
                    continue
                ai_contacts = ai_lead.get('ai_contacts')
                if ai_contacts and isinstance(ai_contacts, list):
                    for contact in ai_contacts:
                        if not contact or not isinstance(contact, dict):
                            continue
                        email = contact.get('email')
                        phone = contact.get('phone')
                        contact_name = safe_str(contact.get('name'))
                        
                        if email and isinstance(email, str):
                            clean_email = email.strip()
                            if clean_email:
                                all_emails.append(clean_email)
                                contact_info[clean_email] = {
                                    'name': safe_contact_name(contact.get('name')),
                                    'phone': safe_str(contact.get('phone')),
                                    'type': 'email'
                                }
                        
                        if phone and isinstance(phone, str):
                            clean_phone = phone.strip()
                            if clean_phone:
                                all_phones.append(clean_phone)
                                contact_info[clean_phone] = {
                                    'name': safe_contact_name(contact.get('name')),
                                    'email': safe_str(contact.get('email')),
                                    'type': 'phone'
                                }
        
        # Remove duplicates while preserving order
        unique_emails = list(dict.fromkeys(all_emails))
        unique_phones = list(dict.fromkeys(all_phones))
        
        # Determine contact strategy: emails first, then phones as fallback
        contacts_to_process = []
        
        if unique_emails:
            # Process emails first
            for email in unique_emails:
                contacts_to_process.append({'contact': email, 'type': 'email'})
        elif unique_phones:
            # Only use phones if no emails exist
            for phone in unique_phones:
                contacts_to_process.append({'contact': phone, 'type': 'phone'})
        
        # If neither emails nor phones found, return empty list
        if not contacts_to_process:
            return []
        
        # Format extraction timestamp
        extraction_timestamp = web_lead.get('extraction_timestamp')
        date_captured = ''
        if extraction_timestamp and isinstance(extraction_timestamp, dict) and '$date' in extraction_timestamp:
            try:
                date_str = extraction_timestamp['$date']
                if isinstance(date_str, str):
                    date_captured = datetime.fromisoformat(date_str.replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                date_captured = safe_str(extraction_timestamp.get('$date', ''))
        elif extraction_timestamp:
            date_captured = safe_str(extraction_timestamp)
        
        # Fallback: if still empty, use today's date
        if not date_captured:
            date_captured = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Safely handle social_media
        social_media = web_lead.get('social_media')
        if social_media is None:
            social_media = {}

        original_id = web_lead.get('_id')
        if isinstance(original_id, ObjectId):
            original_id = str(original_id)
        elif isinstance(original_id, dict):
            # In case it's stored as {"$oid": "..."}
            original_id = original_id.get('$oid', '')
        else:
            original_id = str(original_id) if original_id else web_lead.get('id', '')
        
        # Create a lead for each contact
        for contact_item in contacts_to_process:
            contact = contact_item['contact']
            contact_type = contact_item['type']
            
            # Get contact info for this email/phone
            contact_data = contact_info.get(contact, {})
            contact_name = contact_data.get('name', '')
            
            # Set email and phone based on contact type
            if contact_type == 'email':
                email_address = contact
                phone_number = contact_data.get('phone', '')
            else:  # phone
                email_address = contact_data.get('email', '')
                phone_number = contact
            
            # If no contact name from ai_contacts, try to use contact_person
            if not contact_name:
                contact_person = web_lead.get('contact_person')
                contact_name = safe_contact_name(contact_person)

            extracted_lead = {
                # Mandatory fields
                'Company Name': company_name,
                'Industry': industry,
                'No of Employees': '10-1000',
                'Revenue': '100k',
                'Lead Category': lead_category,
                'Lead Sub Category': lead_sub_category,
                'Company Type': company_type,
                'Lead Source': safe_str(web_lead.get('source_url')),
                'Product Interests': None,
                'Timeline': None,
                'Interest Level': None,
                'Contact Name': contact_name,
                'Email Address': email_address,
                
                # Optional fields
                'Phone Number': phone_number,
                'Company Website': safe_str(web_lead.get('source_url')),
                'Date Captured': date_captured,
                'Decision Makers': safe_contact_name(web_lead.get('contact_person')),
                'Lead Score': web_lead.get('lead_score', '52'),
                'Social Media Link': social_media,    
                # Metadata
                'original_web_lead_id': original_id,
                'processed_at': datetime.utcnow().isoformat(),
                'primary_contact_type': contact_type  # Track whether this lead is primarily email or phone based
            }
            
            extracted_leads.append(extracted_lead)
        
        return extracted_leads

    def process_leads(self, query_filter: Dict[str, Any] = None, batch_size: int = 100) -> Dict[str, int]:
        """
        Main function to filter web_leads from MongoDB and extract lead information
        
        Args:
            query_filter: MongoDB query filter to apply to source collection
            batch_size: Number of documents to process in each batch
            
        Returns:
            Dictionary with processing statistics
        """
        try:
            source_coll = self.db[self.source_collection]
            target_coll = self.db[self.target_collection]
            
            # Use provided filter or default to all documents
            if query_filter is None:
                query_filter = {}
            
            # Get total count
            total_count = source_coll.count_documents(query_filter)
            logger.info(f"Total web_leads records to process: {total_count}")
            
            if total_count == 0:
                logger.warning("No documents found matching the filter criteria")
                return {'total': 0, 'filtered': 0, 'extracted': 0, 'inserted': 0, 'email_based': 0, 'phone_based': 0}
            
            filtered_count = 0
            extracted_count = 0
            inserted_count = 0
            email_based_count = 0
            phone_based_count = 0
            batch_leads = []
            unified_batch = []  # For unified leads processing
            
            # Process in batches
            for skip in range(0, total_count, batch_size):
                logger.info(f"Processing batch: {skip + 1} to {min(skip + batch_size, total_count)}")
                
                # Get batch of documents
                cursor = source_coll.find(query_filter).skip(skip).limit(batch_size)
                
                for web_lead in cursor:
                    try:
                        # Step 1: Filter leads that have valid emails OR phones
                        if not self.has_email_or_phone(web_lead):
                            continue
                        
                        filtered_count += 1
                        
                        # Step 2: Extract data from filtered lead for leadgen_leads collection
                        extracted = self.extract_lead_data(web_lead)
                        
                        # Step 3: Check for duplicates and filter out leads with less information
                        valid_extracted = []
                        for lead in extracted:
                            if self.check_and_handle_duplicate(target_coll, lead):
                                valid_extracted.append(lead)
                        
                        extracted_count += len(valid_extracted)
                        
                        # Count email vs phone based leads
                        for lead in valid_extracted:
                            if lead.get('primary_contact_type') == 'email':
                                email_based_count += 1
                            else:
                                phone_based_count += 1
                        
                        batch_leads.extend(valid_extracted)
                        
                        # Step 4: Add to unified batch if we have the mongodb_manager
                        if self.mongodb_manager and self.has_email_or_phone(web_lead):
                            unified_batch.append(web_lead)
                        
                    except Exception as e:
                        logger.warning(f"Error processing lead {web_lead.get('_id', 'unknown')}: {str(e)}")
                        continue
                
                # Insert batch if we have leads or if this is the last batch
                if batch_leads and (len(batch_leads) >= batch_size or skip + batch_size >= total_count):
                    try:
                        result = target_coll.insert_many(batch_leads)
                        inserted_count += len(result.inserted_ids)
                        logger.info(f"Inserted {len(result.inserted_ids)} leads to {self.target_collection}")
                        batch_leads = []  # Clear batch
                    except Exception as e:
                        logger.error(f"Error inserting batch: {str(e)}")
                
                # Process unified batch
                if unified_batch and (len(unified_batch) >= batch_size or skip + batch_size >= total_count):
                    try:
                        if self.mongodb_manager:
                            unified_stats = self.mongodb_manager.insert_and_transform_to_unified(unified_batch, 'web')
                            logger.info(f"✅ Unified leads batch processed:")
                            logger.info(f"   - Successfully transformed & inserted: {unified_stats['success_count']}")
                            logger.info(f"   - Duplicates skipped: {unified_stats['duplicate_count']}")
                            logger.info(f"   - Failed transformations: {unified_stats['failure_count']}")
                        unified_batch = []  # Clear unified batch
                    except Exception as e:
                        logger.error(f"Error processing unified batch: {str(e)}")
            
            # Insert any remaining leads
            if batch_leads:
                try:
                    result = target_coll.insert_many(batch_leads)
                    inserted_count += len(result.inserted_ids)
                    logger.info(f"Inserted final {len(result.inserted_ids)} leads to {self.target_collection}")
                except Exception as e:
                    logger.error(f"Error inserting final batch: {str(e)}")
            
            # Process any remaining unified batch
            if unified_batch and self.mongodb_manager:
                try:
                    unified_stats = self.mongodb_manager.insert_and_transform_to_unified(unified_batch, 'web')
                    logger.info(f"✅ Final unified leads batch processed:")
                    logger.info(f"   - Successfully transformed & inserted: {unified_stats['success_count']}")
                    logger.info(f"   - Duplicates skipped: {unified_stats['duplicate_count']}")
                    logger.info(f"   - Failed transformations: {unified_stats['failure_count']}")
                except Exception as e:
                    logger.error(f"Error processing final unified batch: {str(e)}")
            
            stats = {
                'total': total_count,
                'filtered': filtered_count,
                'extracted': extracted_count,
                'inserted': inserted_count,
                'email_based': email_based_count,
                'phone_based': phone_based_count
            }
            
            logger.info(f"Processing complete: {stats}")
            return stats
            
        except PyMongoError as e:
            logger.error(f"MongoDB error during processing: {str(e)}")
            return {'total': 0, 'filtered': 0, 'extracted': 0, 'inserted': 0, 'email_based': 0, 'phone_based': 0, 'error': str(e)}
        except Exception as e:
            logger.error(f"Error processing leads: {str(e)}")
            return {'total': 0, 'filtered': 0, 'extracted': 0, 'inserted': 0, 'email_based': 0, 'phone_based': 0, 'error': str(e)}

    def create_indexes(self):
        """
        Create useful indexes on the target collection
        """
        try:
            target_coll = self.db[self.target_collection]
            
            # Create indexes for common queries
            indexes = [
                [('Email Address', 1)],  # For email lookups
                [('Phone Number', 1)],   # For phone lookups
                [('Company Name', 1)],   # For company searches
                [('Industry', 1)],       # For industry filtering
                [('Date Captured', -1)], # For date sorting (newest first)
                [('original_web_lead_id', 1)], # For tracking original source
                [('processed_at', -1)],  # For processing time sorting
                [('primary_contact_type', 1)]  # For filtering by contact type
            ]
            
            for index_fields in indexes:
                try:
                    target_coll.create_index(index_fields)
                    logger.info(f"Created index: {index_fields}")
                except Exception as e:
                    logger.warning(f"Index creation failed for {index_fields}: {str(e)}")
                    
        except Exception as e:
            logger.error(f"Error creating indexes: {str(e)}")

    def get_processing_stats(self) -> Dict[str, Any]:
        """
        Get statistics about processed leads
        """
        try:
            source_coll = self.db[self.source_collection]
            target_coll = self.db[self.target_collection]
            
            total_web_leads = source_coll.count_documents({})
            total_extracted_leads = target_coll.count_documents({})
            
            # Get some sample stats
            unique_companies = len(target_coll.distinct('Company Name'))
            unique_industries = len(target_coll.distinct('Industry'))
            
            # Get email vs phone based counts
            email_based_leads = target_coll.count_documents({'primary_contact_type': 'email'})
            phone_based_leads = target_coll.count_documents({'primary_contact_type': 'phone'})
            
            return {
                'total_web_leads': total_web_leads,
                'total_extracted_leads': total_extracted_leads,
                'unique_companies': unique_companies,
                'unique_industries': unique_industries,
                'email_based_leads': email_based_leads,
                'phone_based_leads': phone_based_leads,
                'source_collection': self.source_collection,
                'target_collection': self.target_collection
            }
        except Exception as e:
            logger.error(f"Error getting stats: {str(e)}")
            return {'error': str(e)}

    def clear_target_collection(self):
        """
        Clear the target collection (useful for re-processing)
        """
        try:
            target_coll = self.db[self.target_collection]
            result = target_coll.delete_many({})
            logger.info(f"Cleared {result.deleted_count} documents from {self.target_collection}")
            return result.deleted_count
        except Exception as e:
            logger.error(f"Error clearing target collection: {str(e)}")
            return 0

    def close_connection(self):
        """
        Close MongoDB connection
        """
        if hasattr(self, 'client'):
            self.client.close()
            logger.info("MongoDB connection closed")

def main():
    """
    Main execution function
    """
    processor = None
    
    try:
        # Initialize processor
        processor = MongoDBLeadProcessor()
        
        # Create indexes (safe to run multiple times)
        logger.info("Creating indexes...")
        processor.create_indexes()
        
        # Get initial stats
        logger.info("Getting initial statistics...")
        initial_stats = processor.get_processing_stats()
        logger.info(f"Initial stats: {initial_stats}")
        
        # Option to clear target collection (uncomment if needed)
        # logger.info("Clearing target collection...")
        # processor.clear_target_collection()
        
        # Process leads with optional filter
        logger.info("Starting lead processing...")
        
        # Example filters (uncomment and modify as needed):
        # query_filter = {'status': 'new'}  # Only process new leads
        # query_filter = {'extraction_timestamp': {'$gte': datetime(2025, 8, 1)}}  # Only recent leads
        query_filter = {}  # Process all leads
        
        results = processor.process_leads(query_filter=query_filter, batch_size=50)
        
        # Get final stats
        logger.info("Getting final statistics...")
        final_stats = processor.get_processing_stats()
        logger.info(f"Final stats: {final_stats}")
        
        # Print summary
        print("\n" + "="*50)
        print("PROCESSING SUMMARY")
        print("="*50)
        print(f"Total web_leads processed: {results['total']}")
        print(f"Leads with valid emails or phones: {results['filtered']}")
        print(f"Individual leads extracted: {results['extracted']}")
        print(f"Leads inserted to {processor.target_collection}: {results['inserted']}")
        print(f"Email-based leads: {results.get('email_based', 0)}")
        print(f"Phone-based leads: {results.get('phone_based', 0)}")
        print(f"Unique companies: {final_stats.get('unique_companies', 'N/A')}")
        print(f"Unique industries: {final_stats.get('unique_industries', 'N/A')}")
        print("="*50)
        
        if 'error' in results:
            print(f"Errors encountered: {results['error']}")
        
    except Exception as e:
        logger.error(f"Fatal error in main execution: {str(e)}")
        print(f"Error: {str(e)}")
    
    finally:
        if processor:
            processor.close_connection()

def process_specific_leads(lead_ids: List[str] = None, status_filter: str = None):
    """
    Process specific leads by ID or status
    
    Args:
        lead_ids: List of specific lead IDs to process
        status_filter: Filter by status (e.g., 'new', 'processed', etc.)
    """
    processor = None
    
    try:
        processor = MongoDBLeadProcessor()
        
        # Build query filter
        query_filter = {}
        
        if lead_ids:
            
            # Handle both string and ObjectId formats
            oid_list = []
            for lead_id in lead_ids:
                try:
                    if isinstance(lead_id, str) and len(lead_id) == 24:
                        oid_list.append(ObjectId(lead_id))
                    else:
                        # Also search by the 'id' field
                        query_filter = {'$or': [
                            {'_id': {'$in': oid_list}} if oid_list else {},
                            {'id': {'$in': lead_ids}}
                        ]}
                except Exception:
                    # If ObjectId conversion fails, search by 'id' field
                    query_filter = {'id': {'$in': lead_ids}}
                    break
            
            if oid_list and '$or' not in query_filter:
                query_filter = {'_id': {'$in': oid_list}}
        
        if status_filter:
            if query_filter:
                query_filter = {'$and': [query_filter, {'status': status_filter}]}
            else:
                query_filter = {'status': status_filter}
        
        logger.info(f"Processing with filter: {query_filter}")
        results = processor.process_leads(query_filter=query_filter)
        
        return results
        
    except Exception as e:
        logger.error(f"Error in process_specific_leads: {str(e)}")
        return {'error': str(e)}
    
    finally:
        if processor:
            processor.close_connection()

if __name__ == "__main__":
    # Main processing
    main()
    
    # Example of processing specific leads (uncomment to use):
    # process_specific_leads(lead_ids=['c4781c67-334a-4f77-a4b5-b8e20834b34f'])
    # process_specific_leads(status_filter='new')