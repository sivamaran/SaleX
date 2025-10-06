#!/usr/bin/env python3
"""
Main Application Orchestrator for Web Scraper
Handles the complete flow from URL list to lead extraction and export.
"""

from __future__ import annotations

import json
import csv
import hashlib
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from loguru import logger
from pydantic import ValidationError

from web_scraper.utils.classification import classify_url
from web_scraper.scrapers.scraper_static import StaticScraper
from web_scraper.scrapers.scraper_dynamic import fetch_dynamic
from web_scraper.processors.processing import process_content
from web_scraper.ai_integration.ai import disambiguate_business_entities, generate_extraction_strategy, validate_and_enhance, extract_client_info_from_sections
from web_scraper.extractors.lead_extraction import extract_lead_information, smart_filter_sections
from web_scraper.processors.data_quality import process_leads_with_quality_engine
from web_scraper.storage.storage import LeadModel, LeadStorage
from web_scraper.storage.export import ExportManager

import os
import re
import json
import sys
from urllib.parse import urlparse

# Add parent directory to path to import database module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.mongodb_manager import get_mongodb_manager

class WebScraperOrchestrator:
    """Main orchestrator for the web scraping pipeline"""
    
    def __init__(self, 
                 storage_path: str = "leads_data",
                 enable_ai: bool = True,
                 enable_quality_engine: bool = True,
                 max_workers: int = 5,
                 delay_between_requests: float = 1.0,
                 use_mongodb: bool = True,
                 max_retries: int = 2,
                 enable_retry: bool = True):
        """
        Initialize the orchestrator
        
        Args:
            storage_path: Path to store lead data
            enable_ai: Whether to use AI enhancement features
            enable_quality_engine: Whether to use data quality engine
            max_workers: Maximum concurrent workers for processing
            delay_between_requests: Delay between requests to avoid rate limiting
            use_mongodb: Whether to save data to MongoDB (default: True)
            max_retries: Maximum number of retries for network errors (default: 2)
            enable_retry: Whether to enable retry mechanism for network errors (default: True)
        """
        self.storage = LeadStorage(storage_path)
        self.export_manager = ExportManager(self.storage)
        self.static_scraper = StaticScraper()
        
        self.enable_ai = enable_ai
        self.enable_quality_engine = enable_quality_engine
        self.max_workers = max_workers
        self.delay_between_requests = delay_between_requests
        self.use_mongodb = use_mongodb
        self.max_retries = max_retries
        self.enable_retry = enable_retry
        
        # Track processed URLs to avoid duplicates
        self.processed_urls: Set[str] = set()
        self.duplicate_leads: List[Dict[str, Any]] = []
        
        # Initialize MongoDB manager if needed
        if self.use_mongodb:
            try:
                self.mongodb_manager = get_mongodb_manager()
                logger.info("✅ MongoDB connection initialized")
            except Exception as e:
                logger.warning(f"⚠️ Failed to initialize MongoDB: {e}")
                self.use_mongodb = False
        
        logger.info(f"Initialized WebScraperOrchestrator with storage at {storage_path}")
    
    def load_urls_from_file(self, file_path: str) -> List[str]:
        """Load URLs from various file formats"""
        file_path = Path(file_path)
        urls = []
        
        try:
            if file_path.suffix.lower() == '.json':
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        urls = [str(url) for url in data]
                    elif isinstance(data, dict) and 'urls' in data:
                        urls = [str(url) for url in data['urls']]
                    else:
                        logger.error(f"Invalid JSON format in {file_path}")
                        
            elif file_path.suffix.lower() == '.csv':
                with open(file_path, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    for row in reader:
                        if row and row[0].strip():  # Skip empty rows
                            urls.append(row[0].strip())
                            
            elif file_path.suffix.lower() == '.txt':
                with open(file_path, 'r', encoding='utf-8') as f:
                    urls = [line.strip() for line in f if line.strip()]
            else:
                logger.error(f"Unsupported file format: {file_path.suffix}")
                
        except Exception as e:
            logger.error(f"Failed to load URLs from {file_path}: {e}")
            
        logger.info(f"Loaded {len(urls)} URLs from {file_path}")
        return urls
    
    def classify_and_fetch_content(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Step 2-4: Classify URL and fetch content (static or dynamic)
        
        Args:
            url: URL to process
        
        Returns:
            Dictionary with classification, page content, and processing results
        """
        try:
            # Step 2: Classify URL
            classification = classify_url(url)
            logger.info(f"URL {url} classified as {classification.classification} (confidence: {classification.confidence:.2f})")
            
            page_content = None
            
            # Step 3-4: Fetch content based on classification with retry logic
            if classification.classification == "static":
                page_content = self._fetch_with_retry(url, "static", self.max_retries if self.enable_retry else 0)
            else:
                page_content = self._fetch_with_retry(url, "dynamic", self.max_retries if self.enable_retry else 0)
            
            if not page_content:
                return None
                
            return {
                "url": url,
                "classification": classification,
                "page_content": page_content,
                "fetch_timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to process URL {url}: {e}")
            return None
    
    def _fetch_with_retry(self, url: str, fetch_type: str, max_retries: int) -> Optional[Any]:
        """
        Fetch content with retry logic for network errors
        
        Args:
            url: URL to fetch
            fetch_type: "static" or "dynamic"
            max_retries: Maximum number of retries
            
        Returns:
            PageContent or None if all retries failed
        """
        for attempt in range(max_retries + 1):
            try:
                if fetch_type == "static":
                    page_content = self.static_scraper.fetch(url)
                    logger.info(f"Static fetch successful for {url}")
                    return page_content
                else:
                    page_content = fetch_dynamic(url)
                    logger.info(f"Dynamic fetch successful for {url}")
                    return page_content
                    
            except Exception as e:
                error_str = str(e).lower()
                is_network_error = any(network_error in error_str for network_error in [
                    'net::err_http2_protocol_error', 'net::err_name_not_resolved', 
                    'timeout', 'connection', 'network', 'dns', 'refused'
                ])
                
                if is_network_error:
                    if attempt < max_retries:
                        # Calculate exponential backoff delay
                        delay = min(2 ** attempt, 3)  # Max 3 seconds
                        logger.warning(f"Network error for {url} (attempt {attempt + 1}/{max_retries + 1}), retrying in {delay}s: {e}")
                        time.sleep(delay)
                        continue
                    else:
                        logger.warning(f"Network error for {url} after {max_retries + 1} attempts, skipping: {e}")
                        return None
                else:
                    # Non-network error, don't retry
                    if fetch_type == "static":
                        logger.warning(f"Static fetch failed for {url}, trying dynamic: {e}")
                        # Try dynamic as fallback
                        try:
                            page_content = fetch_dynamic(url)
                            logger.info(f"Dynamic fallback successful for {url}")
                            return page_content
                        except Exception as e2:
                            error_str2 = str(e2).lower()
                            if any(network_error in error_str2 for network_error in [
                                'net::err_http2_protocol_error', 'net::err_name_not_resolved', 
                                'timeout', 'connection', 'network', 'dns', 'refused'
                            ]):
                                logger.warning(f"Network error for {url} during dynamic fallback, skipping: {e2}")
                            else:
                                logger.error(f"Both static and dynamic fetch failed for {url}: {e2}")
                            return None
                    else:
                        logger.error(f"Dynamic fetch failed for {url}: {e}")
                        return None
        
        return None
    

    def save_debug_results(self, url: str, page_html: str = None, sections: list = None):
        """
        Save HTML and extracted sections into debug_results/ folder.

        Args:
            url (str): URL of the page (used for filename).
            page_html (str): Raw HTML content.
            sections (list, optional): Extracted sections to save. Defaults to [].
        
        Returns:
            tuple: (html_file_path, sections_file_path) - paths to saved files
        """
        if sections is None:
            sections = []

        # Ensure debug_results directory exists at the project root level
        debug_dir = Path("debug_results")  # Use Path object for better cross-platform compatibility
        debug_dir.mkdir(exist_ok=True)

        # Extract hostname and path for identifier
        try:
            parsed = urlparse(url)
            hostname = parsed.hostname or "unknown_host"
            hostname = re.sub(r'^www\.', '', hostname)  # remove www.
            parts = hostname.split('.')  # break into words
            identifier = "_".join(parts[:3]) if parts else "unknown"  # take first 3 parts (adjustable)

            # Add first path segment if available
            path_part = parsed.path.strip("/").split("/")[0] if parsed.path else ""
            if path_part:
                identifier += f"_{path_part}"

            # Sanitize identifier for safe filename
            safe_filename = re.sub(r'[^a-zA-Z0-9_-]', '_', identifier)
            
            # Ensure filename is not empty and not too long
            if not safe_filename or safe_filename == "_":
                safe_filename = "unknown_site"
            safe_filename = safe_filename[:50]  # Limit length to avoid filesystem issues
            
            # Add timestamp to avoid overwriting files with same name
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_filename = f"{safe_filename}_{timestamp}"
            
        except Exception as e:
            logger.warning(f"Error parsing URL {url} for filename: {e}")
            safe_filename = f"unknown_site_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # File paths - use Path objects for cross-platform compatibility
        html_file_path = None
        sections_file_path = debug_dir / f"{safe_filename}_sections.json"

        # Save raw HTML if provided
        if page_html:
            html_file_path = debug_dir / f"{safe_filename}.html"
            try:
                with open(html_file_path, "w", encoding="utf-8") as f:
                    f.write(page_html)
                logger.debug(f"HTML saved to {html_file_path}")
            except Exception as e:
                logger.error(f"Failed to save HTML for {url}: {e}")
                html_file_path = None

        # Save sections (JSON formatted for readability)
        try:
            with open(sections_file_path, "w", encoding="utf-8") as f:
                json.dump(sections, f, indent=2, ensure_ascii=False, default=str)
            logger.debug(f"Sections saved to {sections_file_path}")
        except Exception as e:
            logger.error(f"Failed to save sections for {url}: {e}")
            sections_file_path = None

        return str(html_file_path) if html_file_path else None, str(sections_file_path) if sections_file_path else None


    def process_and_extract_leads(self, fetch_result: Dict[str, Any]) -> Optional[LeadModel]:
        """
        Step 5-6: Process content and extract lead information
        
        Args:
            fetch_result: Result from classify_and_fetch_content.
            it is of structure = {
                "url": url,
                "classification": classification,
                "page_content": page_content,
                "fetch_timestamp": datetime.now().isoformat()
            }

            here page_content is of structure =

            PageContent(
			url=url,
			status_code=status,
			elapsed_seconds=elapsed,
			encoding="utf-8",
			content_type="text/html",
			html=html,
			text=text_only,
			metadata={},
		)
            
        Returns:
            LeadModel instance or None if extraction failed
        """
        try:
            url = fetch_result["url"]
            page_content = fetch_result["page_content"]
            
            # Step 5: Process and structure page data
            processed_content = process_content(page_content.html)
            logger.debug(f"Content processed for {url}")

            '''
            # AI enhancement if enabled
            if self.enable_ai:
                try:
                    entities = disambiguate_business_entities(
                        processed_content.get("emails", []), 
                        processed_content.get("phones", [])
                    )
                    strategy = generate_extraction_strategy(page_content.html)
                    validation = validate_and_enhance(
                        processed_content.get("emails", []), 
                        processed_content.get("phones", [])
                    )
                    logger.debug(f"AI enhancement completed for {url}")
                except Exception as e:
                    logger.warning(f"AI enhancement failed for {url}: {e}")
            
            '''
            
            logger.debug(f"moved to step 6 towards extraction of lead information for {url}")

            # Debug the cleaned_text to see what type it is
            cleaned_text = processed_content.get("cleaned_text", "")
            if isinstance(cleaned_text, dict):
                print(f' cleaned_text keys = {list(cleaned_text.keys())}')

            # Ensure cleaned_text is a string before passing it
            if not isinstance(cleaned_text, str):
                if isinstance(cleaned_text, dict):
                    # Extract text content from the dict
                    text_parts = []
                    for key, value in cleaned_text.items():
                        if isinstance(value, str):
                            text_parts.append(value)
                    cleaned_text = " ".join(text_parts) if text_parts else ""
                else:
                    cleaned_text = str(cleaned_text) if cleaned_text is not None else ""

            # Step 6: Extract lead information with AI integration
            lead_info = extract_lead_information(
                page_content.html, 
                cleaned_text, 
                url,
                sections=processed_content.get("sections", []),
                structured_data=processed_content.get("structured_data", [])  # Pass structured data
            )
            lead_info["extraction_metadata"]["extraction_timestamp"] = datetime.now().isoformat()
            logger.debug(f"Extract lead information for {url}")

            # AI Integration Pipeline (Phases 3-4)
            lead_info["ai_leads"] = []
            if lead_info.get("ai_lead_info") or lead_info.get("structured_data_summary"):
                try:
                    # Phase 3: Smart filtration of sections
                    filtered_ai_lead_info = smart_filter_sections(lead_info["ai_lead_info"])
                    # NEW: Add structured data summary for AI analysis
                    ai_input_data = {
                        "sections": filtered_ai_lead_info,
                        "structured_data": lead_info.get("structured_data_summary")
                    }
                    logger.debug(f"Filtered {len(filtered_ai_lead_info)} sections + structured data for AI analysis")

                    # Phase 4: AI extraction from filtered sections + structured data
                    if filtered_ai_lead_info or lead_info.get("structured_data_summary"):
                        ai_extracted_data = extract_client_info_from_sections(ai_input_data, url) 
                        # print("======================== inside main_app.py Lead info after ai.py retutns========================================")
                        # print(f' AI leads = {ai_extracted_data}')
                        # print("======================== inside main_app.py Lead info ========================================")
                        # Phase 5: Store AI leads under "ai_leads" key
                        if ai_extracted_data:
                            lead_info["ai_leads"] = [{
                                "ai_contacts": ai_extracted_data.get("contacts", []),
                                "organization_info": ai_extracted_data.get("organization_info", {}),
                                "addresses": ai_extracted_data.get("addresses", [])
                            }]
                            
                            logger.info(f"AI extracted {len(lead_info['ai_leads'])} potential client leads from {url}")
                        else:
                            lead_info["ai_leads"] = []
                    else:
                        lead_info["ai_leads"] = []
                        logger.debug(f"No high-value sections or structured data found for AI analysis for {url}")
                       
                except Exception as e:
                    logger.warning(f"AI integration failed for {url}: {e}")
                    lead_info["ai_leads"] = []
            else:
                lead_info["ai_leads"] = []
            
            # Apply data quality engine if enabled
            if self.enable_quality_engine:
                try:
                    quality_results = process_leads_with_quality_engine([lead_info])
                    if quality_results and len(quality_results) > 0:
                        lead_info = quality_results[0]  # Use quality-enhanced version
                    logger.debug(f"Data quality processing completed for {url}")
                except Exception as e:
                    logger.warning(f"Data quality processing failed for {url}: {e}")
            # print("======================== inside main_app.py Lead info ========================================")
            # print(f' AI leads = {lead_info.get("ai_leads")}')
            # print("======================== inside main_app.py Lead info ========================================")

            # Convert to LeadModel
            lead_model = LeadModel.from_extraction_data(lead_info, url)
            logger.info(f"Lead extracted successfully from {url}")
            
            return lead_model
            
        except Exception as e:
            logger.error(f"Failed to extract lead from {fetch_result.get('url', 'unknown')}: {e}")
            return None

    def detect_duplicate_lead(self, new_lead: LeadModel, existing_leads: List[LeadModel]) -> Optional[LeadModel]:
        """
        Detect if a lead is a duplicate based on multiple criteria
        Returns:
        Existing duplicate lead if found, None otherwise
        """
        
        def normalize_field(field):
            """Helper function to normalize fields that might be strings or lists"""
            if field is None:
                return []
            if isinstance(field, str):
                return [field]
            if isinstance(field, list):
                return field
            return []
        
        def get_emails(lead):
            """Extract emails as a list of normalized strings"""
            emails = normalize_field(lead.email)
            return [email.lower().strip() for email in emails if email]
        
        def get_websites(lead):
            """Extract websites as a list of normalized strings"""
            websites = normalize_field(lead.website)
            return [website.strip() for website in websites if website]
        
        def get_business_name(lead):
            """Extract business name as a normalized string"""
            business_name = lead.business_name
            if isinstance(business_name, list):
                # If it's a list, take the first non-empty item
                business_name = next((name for name in business_name if name), None)
            return business_name.lower().strip() if business_name else None

        for existing_lead in existing_leads:
            # Check for exact email match
            new_emails = get_emails(new_lead)
            existing_emails = get_emails(existing_lead)
            
            # Check if any email matches
            if new_emails and existing_emails:
                for new_email in new_emails:
                    if new_email in existing_emails:
                        return existing_lead
            
            # Check for exact phone match
            if (new_lead.phone and existing_lead.phone):
                # Handle phone as potential list
                new_phones = normalize_field(new_lead.phone)
                existing_phones = normalize_field(existing_lead.phone)
                
                for new_phone in new_phones:
                    for existing_phone in existing_phones:
                        if (new_phone and existing_phone and
                            self._normalize_phone(new_phone) == self._normalize_phone(existing_phone)):
                            return existing_lead
            
            # Check for business name + website combination
            new_business_name = get_business_name(new_lead)
            existing_business_name = get_business_name(existing_lead)
            new_websites = get_websites(new_lead)
            existing_websites = get_websites(existing_lead)
            
            if (new_business_name and existing_business_name and
                new_websites and existing_websites and
                new_business_name == existing_business_name):
                # Check if any website matches
                for new_website in new_websites:
                    if new_website in existing_websites:
                        return existing_lead
        
        return None

    def _normalize_phone(self, phone: str) -> str:
        """Normalize phone number for comparison"""
        return ''.join(filter(str.isdigit, phone))
    
    def merge_duplicate_leads(self, existing_lead: LeadModel, new_lead: LeadModel) -> LeadModel:
        """
        Merge information from duplicate leads, keeping the best data
        """
        merged_data = existing_lead.dict()
        new_data = new_lead.dict()
        
        # Merge data sources
        merged_data["data_sources"] = list(set(
            merged_data.get("data_sources", []) + new_data.get("data_sources", [])
        ))
        
        # Keep higher confidence scores and better data
        for field in ["email", "phone", "address", "website", "business_name"]:
            existing_confidence = existing_lead.confidence_scores.get(field, 0.0)
            new_confidence = new_lead.confidence_scores.get(field, 0.0)
            
            if new_confidence > existing_confidence and new_data.get(field):
                merged_data[field] = new_data[field]
                merged_data["confidence_scores"][field] = new_confidence
        
        # Merge services and intent indicators
        merged_data["services"] = list(set(
            merged_data.get("services", []) + new_data.get("services", [])
        ))
        merged_data["intent_indicators"] = list(set(
            merged_data.get("intent_indicators", []) + new_data.get("intent_indicators", [])
        ))
        
        # Use higher lead score
        if new_data.get("lead_score", 0) > merged_data.get("lead_score", 0):
            merged_data["lead_score"] = new_data["lead_score"]
        
        # Merge social media profiles
        for platform, profile in new_data.get("social_media", {}).items():
            if platform not in merged_data.get("social_media", {}):
                merged_data.setdefault("social_media", {})[platform] = profile
        
        return LeadModel(**merged_data)
    
    def process_urls_batch(self, urls: List[str]) -> Tuple[List[LeadModel], List[Dict[str, Any]]]:
        """
        Process a batch of URLs with concurrent execution
        
        Returns:
            Tuple of (successful_leads, failed_urls)
        """
        successful_leads = []
        failed_urls = []
        existing_leads = self.storage.load_all_leads()
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all URL processing tasks
            future_to_url = {
                executor.submit(self.classify_and_fetch_content, url): url 
                for url in urls if url not in self.processed_urls
            }
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                
                try:
                    # Add delay to avoid rate limiting
                    time.sleep(self.delay_between_requests)
                    
                    fetch_result = future.result()
                    if not fetch_result:
                        failed_urls.append({"url": url, "error": "Failed to fetch content"})
                        continue
                    
                    # Extract lead information
                    lead = self.process_and_extract_leads(fetch_result)
                    if not lead:
                        failed_urls.append({"url": url, "error": "Failed to extract lead"})
                        continue
                    
                    # Check for duplicates
                    duplicate = self.detect_duplicate_lead(lead, existing_leads + successful_leads)
                    if duplicate:
                        logger.info(f"Duplicate lead detected for {url}, merging with existing")
                        merged_lead = self.merge_duplicate_leads(duplicate, lead)
                        
                        # Update in successful_leads if it's there, otherwise update existing
                        if duplicate in successful_leads:
                            idx = successful_leads.index(duplicate)
                            successful_leads[idx] = merged_lead
                        else:
                            # Update existing lead in storage
                            self.storage.save_lead(merged_lead)
                            
                            # Save to MongoDB if enabled
                            if self.use_mongodb:
                                try:
                                    lead_dict = merged_lead.dict()
                                    lead_dict['domain'] = urlparse(merged_lead.source_url).netloc
                                    self.mongodb_manager.insert_web_lead(lead_dict)
                                except Exception as e:
                                    logger.error(f"❌ Error saving to MongoDB: {e}")
                        
                        self.duplicate_leads.append({
                            "original_url": duplicate.source_url,
                            "duplicate_url": url,
                            "merge_timestamp": datetime.now().isoformat()
                        })
                    else:
                        successful_leads.append(lead)
                    
                    self.processed_urls.add(url)
                    
                except Exception as e:
                    logger.error(f"Error processing {url}: {e}")
                    failed_urls.append({"url": url, "error": str(e)})
        
        return successful_leads, failed_urls
    
    def generate_final_leads(self, all_successful_leads: List[LeadModel], export_path: str = None) -> Optional[Tuple[str, List[Dict[str, Any]]]]:
        """
        Generate final leads JSON by merging traditional and AI extraction results
        
        Args:
            all_successful_leads: List of all successful leads from the pipeline
            export_path: Path for the exported test results file
            
        Returns:
            Path to the generated final leads file or None if failed
        """
        try:
            logger.info("Starting final leads generation...")
            
            # Determine the final leads output path
            if export_path:
                # Use the same directory as the export path
                export_dir = Path(export_path).parent
                final_leads_path = export_dir / "final_leads.json"
            else:
                # Use the storage directory
                final_leads_path = Path(self.storage.storage_path) / "final_leads.json"
            
            # Create final leads structure
            final_leads = []
            processed_urls = set()
            
            for lead in all_successful_leads:
                source_url = lead.source_url
                
                # Skip if we've already processed this URL (avoid duplicates)
                if source_url in processed_urls:
                    logger.debug(f"Skipping duplicate URL: {source_url}")
                    continue
                
                processed_urls.add(source_url)
                
                # Create the main lead entry
                main_lead = self._create_final_lead_entry(lead)
                final_leads.append(main_lead)
                
                # Create additional leads from AI contacts if they represent different entities
                if lead.ai_leads:
                    for i, ai_lead in enumerate(lead.ai_leads):
                        if isinstance(ai_lead, dict):
                            # Process AI contacts if they exist
                            ai_contacts = ai_lead.get("ai_contacts", [])
                            if ai_contacts:
                                for j, contact in enumerate(ai_contacts):
                                    # Check if contact has meaningful data (name, email, or phone)
                                    has_name = contact.get("name") and contact.get("name") != main_lead["contact_person"]
                                    has_email = contact.get("email") and contact.get("email") != main_lead["email"]
                                    has_phone = contact.get("phone") and contact.get("phone") != main_lead["phone"]
                                    
                                    if has_name or has_email or has_phone:
                                        # Create a new lead for this contact
                                        additional_lead = self._create_final_lead_entry(lead, ai_lead, contact)
                                        additional_lead["id"] = f"{lead.id}_contact_{j}"
                                        additional_lead["contact_person"] = contact.get("name") or main_lead["contact_person"]
                                        additional_lead["email"] = contact.get("email") or main_lead["email"]
                                        additional_lead["phone"] = contact.get("phone") or main_lead["phone"]
                                        final_leads.append(additional_lead)
                            
                            # Also check if there's valuable organization_info even without contacts
                            org_info = ai_lead.get("organization_info", {})
                            if org_info and (org_info.get("primary_name") or org_info.get("industry") or org_info.get("services")):
                                # Only create org lead if it's different from main lead
                                org_name = org_info.get("primary_name")
                                if org_name and org_name != main_lead.get("business_name"):
                                    # Create a lead based on organization info
                                    org_lead = self._create_final_lead_entry(lead, ai_lead)
                                    org_lead["id"] = f"{lead.id}_org_{i}"
                                    org_lead["business_name"] = org_name
                                    org_lead["contact_person"] = main_lead["contact_person"]  # Keep original contact
                                    org_lead["email"] = main_lead["email"]
                                    org_lead["phone"] = main_lead["phone"]
                                    final_leads.append(org_lead)
            
            # Create the final structure
            final_data = {
                "leads": final_leads,
                "metadata": {
                    "generated_timestamp": datetime.now().isoformat(),
                    "total_leads": len(final_leads),
                    "source_file": str(export_path) if export_path else "pipeline_generated",
                    "generated_by": "WebScraperOrchestrator",
                    "traditional_leads": len([l for l in final_leads if not l["id"].endswith(("_ai_", "_contact_"))]),
                    "ai_extracted_leads": len([l for l in final_leads if l["id"].endswith(("_ai_", "_contact_"))])
                }
            }
            
            # Save to file
            final_leads_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(final_leads_path, 'w', encoding='utf-8') as f:
                json.dump(final_data, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"Final leads saved to: {final_leads_path}")
            return str(final_leads_path), final_data["leads"]
            
        except Exception as e:
            logger.error(f"Failed to generate final leads: {e}")
            return None
    
    def _create_final_lead_entry(self, lead: LeadModel, ai_lead_data: Dict[str, Any] = None, contact: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Create a final lead entry by merging traditional and AI data
        
        Args:
            lead: LeadModel from traditional extraction
            ai_lead_data: AI lead data (optional)
            contact: Specific contact from AI data (optional)
            
        Returns:
            Formatted lead entry for final_leads.json
        """
        
        # Start with traditional data
        final_lead = {
            "id": lead.id,
            "source_url": lead.source_url,
            "contact_person": lead.contact_person,
            "business_name": lead.business_name,
            "email": lead.email,
            "phone": lead.phone,
            "address": None,  # Will be populated from AI leads only
            "website": lead.website,
            "social_media": lead.social_media,
            "industry": lead.industry,
            "services": lead.services or [],
            "confidence_score": self._calculate_overall_confidence(lead.confidence_scores)
        }
        
        # Merge AI lead data if available
        if ai_lead_data:
            # Update address from AI leads only (as per requirements)
            if ai_lead_data.get("addresses"):
                # Take the highest confidence address
                best_address = max(ai_lead_data["addresses"], 
                                 key=lambda x: x.get("confidence", 0))
                final_lead["address"] = best_address.get("address")
            
            # Update industry if AI found one
            if ai_lead_data.get("organization_info", {}).get("industry"):
                final_lead["industry"] = ai_lead_data["organization_info"]["industry"]
            
            # Update services if AI found any
            if ai_lead_data.get("organization_info", {}).get("services"):
                ai_services = ai_lead_data["organization_info"]["services"]
                existing_services = set(final_lead["services"])
                final_lead["services"] = list(existing_services.union(set(ai_services)))
        
        return final_lead
    
    def _calculate_overall_confidence(self, confidence_scores: Dict[str, float]) -> float:
        """Calculate overall confidence score from individual field confidences"""
        if not confidence_scores:
            return 0.5  # Default confidence
        
        scores = list(confidence_scores.values())
        return sum(scores) / len(scores) if scores else 0.5

    def _transform_web_final_to_unified(self, lead: Dict[str, Any], icp_identifier: str = 'default') -> Optional[Dict[str, Any]]:
        """Transform final web lead entry to unified schema (local to scraper)."""
        try:
            url = lead.get('source_url') or lead.get('website') or ''
            if not url:
                return None
            emails = []
            if lead.get('email'):
                if isinstance(lead['email'], list):
                    emails = [e for e in lead['email'] if e]
                else:
                    emails = [lead['email']]
            phones = []
            if lead.get('phone'):
                if isinstance(lead['phone'], list):
                    phones = [p for p in lead['phone'] if p]
                else:
                    phones = [lead['phone']]
            unified = {
                "url": url,
                "platform": "web",
                "content_type": "profile",
                "source": "web-scraper",
                "icp_identifier": icp_identifier,
                "profile": {
                    "username": "",
                    "full_name": lead.get('business_name') or lead.get('contact_person') or "",
                    "bio": "",
                    "location": lead.get('address') or "",
                    "job_title": "",
                    "employee_count": ""
                },
                "contact": {
                    "emails": emails,
                    "phone_numbers": phones,
                    "address": lead.get('address') or "",
                    "websites": [url] if url else [],
                    "social_media_handles": lead.get('social_media') or {},
                    "bio_links": []
                },
                "content": {
                    "caption": "",
                    "upload_date": "",
                    "channel_name": "",
                    "author_name": ""
                },
                "metadata": {
                    "scraped_at": datetime.utcnow().isoformat(),
                    "data_quality_score": f"{self._calculate_overall_confidence(lead.get('confidence_scores', {})):.2f}"
                },
                "industry": lead.get('industry'),
                "revenue": None,
                "lead_category": None,
                "lead_sub_category": None,
                "company_name": lead.get('business_name') or "",
                "company_type": None,
                "decision_makers": lead.get('contact_person') or "",
                "bdr": "AKG",
                "product_interests": None,
                "timeline": None,
                "interest_level": None
            }
            return unified
        except Exception:
            return None

    def run_complete_pipeline(self, 
                            urls: List[str] = None,
                            url_file: str = None,
                            batch_size: int = 50,
                            export_format: str = "json",
                            export_path: str = None,
                            generate_final_leads: bool = True,
                            icp_identifier: str = 'default') -> Dict[str, Any]:
        """
        Run the complete pipeline from URLs to exported leads
        
        Args:
            urls: List of URLs to process
            url_file: Path to file containing URLs
            batch_size: Number of URLs to process in each batch
            export_format: Format for export (json, csv, excel)
            export_path: Path for exported file
            generate_final_leads: Whether to generate final leads JSON
            icp_identifier: ICP identifier for tracking which ICP this data belongs to
            
        Returns:
            Dictionary with pipeline results and statistics
        """
        start_time = datetime.now()
        logger.info("Starting complete web scraper pipeline")
        
        # Step 1: Load URLs
        if url_file:
            urls = self.load_urls_from_file(url_file)
        elif not urls:
            raise ValueError("Either urls list or url_file must be provided")
        
        if not urls:
            raise ValueError("No URLs to process")
        
        logger.info(f"Processing {len(urls)} URLs in batches of {batch_size}")
        
        all_successful_leads = []
        all_failed_urls = []
        
        # Process URLs in batches
        for i in range(0, len(urls), batch_size):
            batch_urls = urls[i:i + batch_size]
            batch_num = i // batch_size + 1
            
            logger.info(f"Processing batch {batch_num}/{(len(urls) + batch_size - 1) // batch_size}")
            
            successful_leads, failed_urls = self.process_urls_batch(batch_urls)
            
            # Save successful leads to storage
            if successful_leads:
                self.storage.save_leads_batch(successful_leads)
                all_successful_leads.extend(successful_leads)
            
            all_failed_urls.extend(failed_urls)
            
            logger.info(f"Batch {batch_num} completed: {len(successful_leads)} successful, {len(failed_urls)} failed")
        
        # Export results if requested
        exported_file = None
        # if export_path and all_successful_leads:
        #     try:
        #         exported_file = self.export_manager.export_filtered_leads(
        #             output_path=export_path,
        #             export_format=export_format
        #         )
        #         logger.info(f"Results exported to {exported_file}")
        #     except Exception as e:
        #         logger.error(f"Export failed: {e}")
        
        # Generate final leads if requested
        final_leads_file, final_leads = None, []
        if generate_final_leads and all_successful_leads:
            try:
                result = self.generate_final_leads(all_successful_leads, export_path)
                if result:
                    final_leads_file, final_leads = result  # unpack (path, leads)
                    logger.info(f"Final leads generated: {final_leads_file} with {len(final_leads)} leads")
            except Exception as e:
                logger.error(f"Final leads generation failed: {e}")
        
        # Build unified leads locally from final leads and save
        unified_leads = []
        for lead in final_leads:
            lead_dict = lead if isinstance(lead, dict) else lead.dict()
            u = self._transform_web_final_to_unified(lead_dict, icp_identifier)
            if u:
                unified_leads.append(u)
                
        # Save to MongoDB if enabled
        if self.use_mongodb:
            try:
                # Save original web leads (optional)
                web_leads_data = []
                for lead in final_leads:
                    lead_dict = lead if isinstance(lead, dict) else lead.dict()
                    if lead_dict.get('source_url'):
                        lead_dict['domain'] = urlparse(lead_dict['source_url']).netloc
                    lead_dict['icp_identifier'] = icp_identifier
                    web_leads_data.append(lead_dict)
                if web_leads_data:
                    mongodb_stats = self.mongodb_manager.insert_batch_leads(web_leads_data, 'web')
                    logger.info(f"✅ Batch saved to MongoDB (web_leads) - Success: {mongodb_stats['success_count']}, Duplicates: {mongodb_stats['duplicate_count']}, Failures: {mongodb_stats['failure_count']}")


                # if unified_leads:
                #     unified_stats = self.mongodb_manager.insert_batch_unified_leads(unified_leads)
                #     logger.info(f"✅ Unified leads saved - Success: {unified_stats['success_count']}, Duplicates: {unified_stats['duplicate_count']}, Failures: {unified_stats['failure_count']}")
            except Exception as e:
                logger.error(f"❌ Error saving to MongoDB: {e}")

        # Generate pipeline statistics
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        stats = {
            "pipeline_summary": {
                "total_urls": len(urls),
                "successful_leads": len(all_successful_leads),
                "failed_urls": len(all_failed_urls),
                "duplicate_leads": len(self.duplicate_leads),
                "processing_time_seconds": duration,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat()
            },
            "successful_leads": [lead.dict() for lead in all_successful_leads],
            "failed_urls": all_failed_urls,
            "duplicate_info": self.duplicate_leads,
            "exported_file": exported_file,
            "final_leads_file": final_leads_file,
            "storage_stats": self.storage.get_storage_stats(),
            "unified_leads": unified_leads
        }
        
        logger.info(f"Pipeline completed: {len(all_successful_leads)}/{len(urls)} successful in {duration:.2f}s")
        return stats


def main():
    """CLI interface for the orchestrator"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Web Scraper Orchestrator - Complete Pipeline")
    parser.add_argument("--urls", nargs="+", help="List of URLs to process")
    parser.add_argument("--url-file", help="File containing URLs")
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size for processing")
    parser.add_argument("--export-format", choices=["json", "csv", "excel"], default="json", help="Export format")
    parser.add_argument("--export-path", help="Path for exported results")
    parser.add_argument("--storage-path", default="leads_data", help="Path for lead storage")
    parser.add_argument("--max-workers", type=int, default=5, help="Maximum concurrent workers")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests (seconds)")
    parser.add_argument("--disable-ai", action="store_true", help="Disable AI enhancement")
    parser.add_argument("--disable-quality", action="store_true", help="Disable data quality engine")
    parser.add_argument("--disable-final-leads", action="store_true", help="Disable final leads generation")
    parser.add_argument("--disable-retry", action="store_true", help="Disable retry mechanism for network errors")
    parser.add_argument("--max-retries", type=int, default=2, help="Maximum number of retries for network errors")
    parser.add_argument("--output-stats", help="Path to save pipeline statistics JSON")
    
    args = parser.parse_args()
    
    if not args.urls and not args.url_file:
        parser.error("Either --urls or --url-file must be provided")
    
    # Initialize orchestrator
    orchestrator = WebScraperOrchestrator(
        storage_path=args.storage_path,
        enable_ai=not args.disable_ai,
        enable_quality_engine=not args.disable_quality,
        max_workers=args.max_workers,
        delay_between_requests=args.delay,
        max_retries=args.max_retries,
        enable_retry=not args.disable_retry
    )
    
    try:
        # Run pipeline
        results = orchestrator.run_complete_pipeline(
            urls=args.urls,
            url_file=args.url_file,
            batch_size=args.batch_size,
            export_format=args.export_format,
            export_path=args.export_path,
            generate_final_leads=not args.disable_final_leads
        )
        
        # Output results
        print(json.dumps(results["pipeline_summary"], indent=2))
        
        # Show final leads information if generated
        if results.get("final_leads_file"):
            print(f"\n✅ Final leads generated: {results['final_leads_file']}")
        
        # Save detailed stats if requested
        if args.output_stats:
            with open(args.output_stats, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False, default=str)
            logger.info(f"Detailed statistics saved to {args.output_stats}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()
