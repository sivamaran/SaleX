"""
Instagram Scraper - Optimized Main Entry Point
High-performance Instagram scraper with concurrency, browser pooling, and batch processing

Key Optimizations:
1. Async concurrency with worker pool pattern
2. Browser context pooling (3-5 contexts)
3. Batch processing (5-10 concurrent URLs)
4. Resource optimization and cleanup
5. Fail-fast strategy with graceful degradation
6. Coordinated rate limiting

Usage:
    from main_optimized import OptimizedInstagramScraper
    
    scraper = OptimizedInstagramScraper()
    result = await scraper.scrape(urls)
    
    # Or use the convenience function
    result = await scrape_instagram_urls_optimized(urls)
"""

import asyncio
import json
import time
import sys
import os
import random
from typing import List, Dict, Any, Optional, Tuple
import re
from urllib.parse import urlparse
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import weakref
import gc

# Add parent directory to path to import database module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from instagram_scraper.src.advanced_graphql_extractor import AdvancedGraphQLExtractor
# Removed direct MongoDB dependency from scraper. Orchestrator will handle DB.


@dataclass
class ScrapingConfig:
    """Configuration for optimized scraping with enhanced network resilience"""
    max_workers: int = 3  # Reduced for better stability
    batch_size: int = 5   # Reduced for better stability
    context_pool_size: int = 3  # Reduced for better stability
    context_reuse_limit: int = 15  # Reduced for better stability
    rate_limit_delay: float = 2.0  # Increased for better rate limiting
    max_retries: int = 2  # Increased for better resilience
    timeout_seconds: int = 45  # Increased for better stability
    cleanup_interval: int = 10
    network_retry_delay: float = 5.0  # Base delay for network retries
    max_network_retries: int = 3  # Max retries for network errors
    jitter_factor: float = 0.3  # Jitter for request timing


class BrowserContextPool:
    """Manages a pool of browser contexts for concurrent processing"""
    
    def __init__(self, pool_size: int = 5, headless: bool = True, 
                 enable_anti_detection: bool = True, is_mobile: bool = False):
        self.pool_size = pool_size
        self.headless = headless
        self.enable_anti_detection = enable_anti_detection
        self.is_mobile = is_mobile
        self.contexts = []
        self.context_usage_count = {}
        self.context_lock = asyncio.Lock()
        self.rate_limiter = asyncio.Semaphore(3)  # Global rate limiter
        
    async def initialize(self):
        """Initialize the context pool"""
        print(f"🔧 Initializing browser context pool ({self.pool_size} contexts)...")
        
        for i in range(self.pool_size):
            try:
                extractor = AdvancedGraphQLExtractor(
                    headless=self.headless,
                    enable_anti_detection=self.enable_anti_detection,
                    is_mobile=self.is_mobile
                )
                await extractor.start()
                
                self.contexts.append(extractor)
                self.context_usage_count[id(extractor)] = 0
                print(f"✅ Context {i+1}/{self.pool_size} initialized")
                
            except Exception as e:
                print(f"❌ Failed to initialize context {i+1}: {e}")
                continue
        
        print(f"✅ Context pool initialized with {len(self.contexts)} contexts")
    
    async def get_context(self) -> Optional[AdvancedGraphQLExtractor]:
        """Get an available context from the pool"""
        async with self.context_lock:
            # Find context with least usage
            if not self.contexts:
                return None
                
            min_usage = min(self.context_usage_count.values())
            for context in self.contexts:
                if self.context_usage_count[id(context)] == min_usage:
                    self.context_usage_count[id(context)] += 1
                    return context
            return self.contexts[0] if self.contexts else None
    
    async def return_context(self, context: AdvancedGraphQLExtractor):
        """Return context to pool (no-op for now, contexts are reused)"""
        pass
    
    async def cleanup_context(self, context: AdvancedGraphQLExtractor):
        """Clean up a context that has reached usage limit"""
        try:
            await context.stop()
            if context in self.contexts:
                self.contexts.remove(context)
            if id(context) in self.context_usage_count:
                del self.context_usage_count[id(context)]
            print(f"🧹 Context cleaned up (usage limit reached)")
        except Exception as e:
            print(f"⚠️ Error cleaning up context: {e}")
    
    async def cleanup_all(self):
        """Clean up all contexts"""
        print("🧹 Cleaning up all browser contexts...")
        for context in self.contexts:
            try:
                await context.stop()
            except Exception as e:
                print(f"⚠️ Error cleaning up context: {e}")
        self.contexts.clear()
        self.context_usage_count.clear()
        print("✅ All contexts cleaned up")


class OptimizedInstagramScraper:
    """
    Optimized Instagram scraper with concurrency, pooling, and batch processing
    """
    
    def __init__(self, 
                 headless: bool = True, 
                 enable_anti_detection: bool = True,
                 is_mobile: bool = False,
                 output_file: Optional[str] = None,
                 use_mongodb: bool = True,
                 config: Optional[ScrapingConfig] = None,
                 icp_identifier: str = 'default'):
        """
        Initialize the optimized Instagram scraper
        
        Args:
            headless: Run browser in headless mode (default: True)
            enable_anti_detection: Enable anti-detection features (default: True)
            is_mobile: Use mobile user agent and viewport (default: False)
            output_file: Optional file path to save results (default: None)
            use_mongodb: Whether to save data to MongoDB (default: True)
            config: Custom scraping configuration (default: None)
            icp_identifier: ICP identifier for tracking which ICP this data belongs to (default: 'default')
        """
        self.headless = headless
        self.enable_anti_detection = enable_anti_detection
        self.is_mobile = is_mobile
        self.output_file = output_file
        self.use_mongodb = use_mongodb
        self.config = config or ScrapingConfig()
        self.icp_identifier = icp_identifier
        
        # Initialize components
        self.context_pool = None
        self.worker_semaphore = asyncio.Semaphore(self.config.max_workers)
        self.rate_limiter = asyncio.Semaphore(3)  # Global rate limiter
        
        # Scraper no longer initializes MongoDB. DB operations are orchestrated centrally.
    
    async def scrape(self, urls: List[str]) -> Dict[str, Any]:
        """
        Scrape data from a list of Instagram URLs with optimizations
        
        Args:
            urls: List of Instagram URLs to scrape
            
        Returns:
            Dictionary containing scraping results
        """
        if not urls:
            return {
                'success': False,
                'error': 'No URLs provided',
                'data': [],
                'summary': {},
                'errors': []
            }
        
        print(f"🚀 Starting optimized Instagram scraper...")
        print(f"   URLs to process: {len(urls)}")
        print(f"   Max workers: {self.config.max_workers}")
        print(f"   Batch size: {self.config.batch_size}")
        print(f"   Context pool size: {self.config.context_pool_size}")
        print(f"   Anti-detection: {'✅ Enabled' if self.enable_anti_detection else '❌ Disabled'}")
        print(f"   Mobile mode: {'✅ Enabled' if self.is_mobile else '❌ Disabled'}")
        print(f"   Headless mode: {'✅ Enabled' if self.headless else '❌ Disabled'}")
        
        start_time = time.time()
        all_extracted_data = []
        errors = []
        
        try:
            # Initialize context pool
            self.context_pool = BrowserContextPool(
                pool_size=self.config.context_pool_size,
                headless=self.headless,
                enable_anti_detection=self.enable_anti_detection,
                is_mobile=self.is_mobile
            )
            await self.context_pool.initialize()
            
            if not self.context_pool.contexts:
                raise RuntimeError("Failed to initialize any browser contexts")
            
            # Process URLs in batches
            print(f"\n📦 Processing URLs in batches of {self.config.batch_size}...")
            processed_usernames = set()
            
            for batch_num, batch_urls in enumerate(self._create_batches(urls, self.config.batch_size), 1):
                print(f"\n🔄 Processing batch {batch_num}/{(len(urls) + self.config.batch_size - 1) // self.config.batch_size}")
                print(f"   URLs in batch: {len(batch_urls)}")
                
                # Process batch concurrently
                batch_results = await self._process_batch(batch_urls, processed_usernames)
                
                # Collect results
                for result in batch_results:
                    if result['success']:
                        all_extracted_data.extend(result['data'])
                        processed_usernames.update(result['usernames'])
                    else:
                        errors.extend(result['errors'])
                
                # Cleanup contexts that have reached usage limit
                await self._cleanup_old_contexts()
                
                # Rate limiting between batches
                if batch_num < (len(urls) + self.config.batch_size - 1) // self.config.batch_size:
                    await asyncio.sleep(self.config.rate_limit_delay)
            
            # Build unified leads locally for ALL content types. Orchestrator will handle DB persistence.
            unified_leads = []
            if all_extracted_data:
                unified_leads = [
                    self._transform_instagram_to_unified(entry, self.icp_identifier)
                    for entry in all_extracted_data
                ]
                unified_leads = [u for u in unified_leads if u]
            
            # Save to file if specified (as backup)
            output_file_path = None
            # if self.output_file:
            #     try:
            #         with open(self.output_file, 'w', encoding='utf-8') as f:
            #             json.dump(all_extracted_data, f, indent=2, ensure_ascii=False, default=str)
            #         output_file_path = self.output_file
            #         print(f"\n💾 Results also saved to file: {self.output_file}")
            #     except Exception as e:
            #         print(f"❌ Error saving to file: {e}")
            
            # Calculate summary statistics
            total_time = time.time() - start_time
            content_types = {}
            original_urls_processed = len(urls)
            additional_profiles_extracted = len(processed_usernames)
            
            for entry in all_extracted_data:
                content_type = entry.get('content_type', 'unknown')
                content_types[content_type] = content_types.get(content_type, 0) + 1
            
            summary = {
                'total_original_urls': original_urls_processed,
                'additional_profiles_extracted': additional_profiles_extracted,
                'total_extractions': len(all_extracted_data),
                'successful_extractions': len(all_extracted_data),
                'failed_extractions': len(errors),
                'success_rate': len(all_extracted_data) / original_urls_processed * 100 if original_urls_processed else 0,
                'total_time_seconds': total_time,
                'average_time_per_url': total_time / original_urls_processed if original_urls_processed else 0,
                'content_type_breakdown': content_types,
                'performance_metrics': {
                    'contexts_used': len(self.context_pool.contexts),
                    'max_workers': self.config.max_workers,
                    'batch_size': self.config.batch_size,
                    'throughput_per_second': original_urls_processed / total_time if total_time > 0 else 0
                }
            }
            
            print(f"\n🎉 Optimized scraping completed!")
            print(f"   - Total time: {total_time:.2f} seconds")
            print(f"   - Original URLs processed: {original_urls_processed}")
            print(f"   - Additional profiles extracted: {additional_profiles_extracted}")
            print(f"   - Total extractions: {len(all_extracted_data)}")
            print(f"   - Success rate: {summary['success_rate']:.1f}%")
            print(f"   - Failed extractions: {len(errors)}")
            print(f"   - Throughput: {summary['performance_metrics']['throughput_per_second']:.2f} URLs/second")
            
            if content_types:
                print(f"   - Content types:")
                for content_type, count in content_types.items():
                    print(f"     • {content_type.title()}: {count}")
            
            return {
                'success': len(errors) == 0,
                'data': all_extracted_data,
                'summary': summary,
                'errors': errors,
                'output_file': output_file_path,
                'unified_leads': unified_leads
            }
            
        except Exception as e:
            error_msg = f"Critical error during optimized scraping: {str(e)}"
            print(f"❌ {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'data': all_extracted_data,
                'summary': {
                    'total_original_urls': len(urls),
                    'additional_profiles_extracted': len(processed_usernames) if 'processed_usernames' in locals() else 0,
                    'total_extractions': len(all_extracted_data),
                    'successful_extractions': len(all_extracted_data),
                    'failed_extractions': len(urls),
                    'success_rate': 0,
                    'total_time_seconds': time.time() - start_time,
                    'average_time_per_url': 0,
                    'content_type_breakdown': {}
                },
                'errors': errors + [{'url': 'ALL', 'error': error_msg, 'index': 0}],
                'output_file': None
            }
        
        finally:
            # Clean up
            if self.context_pool:
                try:
                    await self.context_pool.cleanup_all()
                    print(f"✅ Context pool cleanup completed")
                except Exception as e:
                    print(f"⚠️ Warning during context pool cleanup: {e}")
    
    def _transform_instagram_to_unified(self, instagram_data: Dict[str, Any], icp_identifier: str = 'default') -> Optional[Dict[str, Any]]:
        """Transform Instagram data (profile/article/video) to unified schema (local to scraper)"""
        try:
            content_type = (instagram_data.get('content_type') or '').lower()

            if content_type == 'unknown':
                return None

            base = {
                "url": instagram_data.get('url', ""),
                "platform": "instagram",
                "content_type": content_type or "unknown",
                "source": "instagram-scraper",
                "icp_identifier": icp_identifier,
                "metadata": {
                    "scraped_at": instagram_data.get('scraped_at') or time.time(),
                    "data_quality_score": ""
                },
                # Common optional fields kept for unified schema
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

            if content_type == 'profile':
                base.update({
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
                        "caption": "",
                        "upload_date": "",
                        "channel_name": "",
                        "author_name": ""
                    }
                })
                return base

            if content_type in ('article', 'video'):
                base.update({
                    "profile": {
                        "username": instagram_data.get('username', ""),
                        "full_name": instagram_data.get('full_name', ""),
                        "bio": "",
                        "location": "",
                        "job_title": "",
                        "employee_count": ""
                    },
                    "contact": {
                        "emails": [],
                        "phone_numbers": [],
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
                        "bio_links": ""
                    },
                    "content": {
                        "caption": instagram_data.get('caption', ""),
                        "upload_date": instagram_data.get('post_date', ""),
                        "channel_name": "",
                        "author_name": instagram_data.get('username', "")
                    }
                })
                return base

            # Unknown type - still return minimal entry so caller can decide
            return None
        except Exception:
            return None
    
    def _create_batches(self, urls: List[str], batch_size: int) -> List[List[str]]:
        """Create batches of URLs for processing"""
        batches = []
        for i in range(0, len(urls), batch_size):
            batches.append(urls[i:i + batch_size])
        return batches
    
    async def _process_batch(self, batch_urls: List[str], processed_usernames: set) -> List[Dict[str, Any]]:
        """Process a batch of URLs concurrently"""
        tasks = []
        
        for i, url in enumerate(batch_urls):
            task = asyncio.create_task(
                self._process_single_url(url, i, processed_usernames)
            )
            tasks.append(task)
        
        # Wait for all tasks in the batch to complete
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results and handle exceptions
        processed_results = []
        for i, result in enumerate(batch_results):
            if isinstance(result, Exception):
                processed_results.append({
                    'success': False,
                    'data': [],
                    'usernames': set(),
                    'errors': [{'url': batch_urls[i], 'error': str(result), 'index': i}]
                })
            else:
                processed_results.append(result)
        
        return processed_results
    
    async def _process_single_url(self, url: str, index: int, processed_usernames: set) -> Dict[str, Any]:
        """Process a single URL with worker pool, rate limiting, and enhanced error handling"""
        async with self.worker_semaphore:  # Limit concurrent workers
            async with self.rate_limiter:  # Global rate limiting
                try:
                    # Get context from pool
                    context = await self.context_pool.get_context()
                    if not context:
                        raise RuntimeError("No available browser contexts")
                    
                    # Apply rate limiting delay with jitter
                    delay = self.config.rate_limit_delay + random.uniform(0, 0.5)
                    await asyncio.sleep(delay)
                    
                    print(f"  🔍 Processing: {url}")
                    
                    # First check if URL type is known before extracting data
                    initial_content_type = self._determine_content_type_from_url(url)
                    
                    # Skip unknown URL types immediately
                    if initial_content_type == "unknown":
                        print(f"  ⚠️ Skipping unknown URL type: {url}")
                        return {
                            'success': True,
                            'data': [],
                            'usernames': set(),
                            'errors': []
                        }
                    
                    # Extract data from the URL with enhanced error handling
                    extracted_data = await context.extract_graphql_data(url)
                    
                    # Check for various error conditions
                    if extracted_data.get('error'):
                        error_msg = extracted_data['error']
                        error_type = extracted_data.get('error_type', 'unknown')
                        
                        # Log specific error types
                        if 'connection' in error_msg.lower() or 'network' in error_msg.lower():
                            print(f"  ⚠️ Network error for {url}: {error_msg}")
                        elif 'timeout' in error_msg.lower():
                            print(f"  ⚠️ Timeout error for {url}: {error_msg}")
                        else:
                            print(f"  ❌ Error for {url}: {error_msg}")
                        
                        return {
                            'success': False,
                            'data': [],
                            'usernames': set(),
                            'errors': [{
                                'url': url, 
                                'error': error_msg, 
                                'error_type': error_type,
                                'index': index
                            }]
                        }
                    
                    # Check if extraction was successful
                    if not extracted_data.get('success', True):
                        print(f"  ❌ Extraction failed for {url}: {extracted_data.get('error', 'Unknown error')}")
                        return {
                            'success': False,
                            'data': [],
                            'usernames': set(),
                            'errors': [{
                                'url': url, 
                                'error': extracted_data.get('error', 'Extraction failed'), 
                                'index': index
                            }]
                        }
                    
                    # Process extracted data
                    result = await self._process_extracted_data(extracted_data, url, processed_usernames)
                    
                    print(f"  ✅ Successfully processed: {url}")
                    return result
                    
                except Exception as e:
                    error_msg = f"Error processing {url}: {str(e)}"
                    print(f"  ❌ {error_msg}")
                    
                    # Classify error type for better handling
                    error_type = 'unknown'
                    if 'connection' in str(e).lower() or 'network' in str(e).lower():
                        error_type = 'network_error'
                    elif 'timeout' in str(e).lower():
                        error_type = 'timeout'
                    elif 'rate' in str(e).lower() or 'limit' in str(e).lower():
                        error_type = 'rate_limit'
                    
                    return {
                        'success': False,
                        'data': [],
                        'usernames': set(),
                        'errors': [{
                            'url': url, 
                            'error': str(e), 
                            'error_type': error_type,
                            'index': index
                        }]
                    }
    
    async def _process_extracted_data(self, extracted_data: Dict[str, Any], url: str, processed_usernames: set) -> Dict[str, Any]:
        """Process extracted data and create clean entries"""
        all_extracted_data = []
        usernames = set()
        
        # Determine content type and create clean entry
        content_type = self._determine_content_type_from_url(url, extracted_data)
        
        # Skip unknown URL types
        if content_type == "unknown":
            return {
                'success': True,
                'data': [],
                'usernames': set(),
                'errors': []
            }
        
        clean_entry = {
            "url": url,
            "content_type": content_type
        }
        
        # Add data based on content type
        if content_type == "profile":
            user_data = extracted_data.get('user_data', {})
            clean_entry.update({
                "full_name": user_data.get('full_name'),
                "username": user_data.get('username'),
                "followers_count": self._format_count(user_data.get('followers_count')),
                "following_count": self._format_count(user_data.get('following_count')),
                "biography": user_data.get('biography', ''),
                "bio_links": user_data.get('bio_links', []),
                "is_private": user_data.get('is_private', False),
                "is_verified": user_data.get('is_verified', False),
                "is_business_account": user_data.get('is_business_account', False),
                "is_professional_account": user_data.get('is_professional_account', True),
                "business_email": user_data.get('business_email'),
                "business_phone_number": user_data.get('business_phone_number'),
                "business_category_name": user_data.get('business_category_name')
            })
            
        elif content_type in ["article", "video"]:
            meta_data = extracted_data.get('meta_data', {})
            script_data = extracted_data.get('script_data', {})
            
            # Extract username from multiple sources
            username = (meta_data.get('username') or 
                      meta_data.get('username_from_title') or
                      script_data.get('username'))
            
            # Extract caption
            caption = meta_data.get('caption') or script_data.get('caption') or ''
            
            # Extract email and phone from caption
            extracted_email = None
            extracted_phone = None
            
            if caption:
                # Extract email - more specific pattern that stops at valid TLD
                email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,6}'
                email_match = re.search(email_pattern, caption)
                if email_match:
                    extracted_email = email_match.group(0)
                
                # Extract phone number - improved patterns with word boundaries
                phone_patterns = [
                    # Indian mobile numbers (10 digits starting with 6-9)
                    r'(?<!\d)[6-9]\d{9}(?!\d)',
                    # US format with country code
                    r'(?<!\d)\+?1[-.\s]?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})(?!\d)',
                    # International format with + and country code
                    r'(?<!\d)\+[1-9]\d{1,3}[-.\s]?\d{3,4}[-.\s]?\d{3,4}[-.\s]?\d{3,4}(?!\d)',
                    # General format 10-15 digits with optional separators
                    r'(?<!\d)(?:\+?[1-9]\d{0,3}[-.\s]?)?\d{3,4}[-.\s]?\d{3,4}[-.\s]?\d{3,4}(?!\d)'
                ]
                
                for pattern in phone_patterns:
                    phone_matches = re.findall(pattern, caption)
                    if phone_matches:
                        # Take the first valid phone number found
                        if isinstance(phone_matches[0], tuple):  # For patterns with groups
                            extracted_phone = ''.join(phone_matches[0])
                        else:
                            extracted_phone = phone_matches[0]
                        
                        # Clean up the phone number (remove extra spaces, dots, hyphens)
                        extracted_phone = re.sub(r'[-.\s]+', '', extracted_phone)
                        
                        # Validate phone length (should be 10-15 digits)
                        if 10 <= len(re.sub(r'\D', '', extracted_phone)) <= 15:
                            break
                        else:
                            extracted_phone = None
            
            clean_entry.update({
                "likes_count": self._format_count(meta_data.get('likes_count') or script_data.get('likes')),
                "comments_count": self._format_count(meta_data.get('comments_count') or script_data.get('comments')),
                "username": username,
                "post_date": meta_data.get('post_date'),
                "caption": caption,
                "business_email": extracted_email,
                "business_phone_number": extracted_phone
            })
            
            # If we found a username and haven't processed it yet, extract profile data
            if username and username not in processed_usernames:
                processed_usernames.add(username)
                usernames.add(username)
                print(f"  🔍 Found username '{username}' in {content_type}. Extracting profile data...")
                
                try:
                    # Create profile URL and extract profile data
                    profile_url = f"https://www.instagram.com/{username}/"
                    
                    # Get a context for profile extraction
                    context = await self.context_pool.get_context()
                    if context:
                        profile_extracted_data = await context.extract_graphql_data(profile_url)
                        
                        if not profile_extracted_data.get('error'):
                            user_data = profile_extracted_data.get('user_data', {})
                            
                            # Create profile entry
                            profile_entry = {
                                "url": profile_url,
                                "content_type": "profile",
                                "full_name": user_data.get('full_name'),
                                "username": user_data.get('username'),
                                "followers_count": self._format_count(user_data.get('followers_count')),
                                "following_count": self._format_count(user_data.get('following_count')),
                                "biography": user_data.get('biography', ''),
                                "bio_links": user_data.get('bio_links', []),
                                "is_private": user_data.get('is_private', False),
                                "is_verified": user_data.get('is_verified', False),
                                "is_business_account": user_data.get('is_business_account', False),
                                "is_professional_account": user_data.get('is_professional_account', True),
                                "business_email": user_data.get('business_email') or extracted_email,
                                "business_phone_number": user_data.get('business_phone_number') or extracted_phone,
                                "business_category_name": user_data.get('business_category_name')
                            }
                            
                            # Always include business fields, even if null
                            business_fields = ['business_email', 'business_phone_number', 'business_category_name']
                            for field in business_fields:
                                if field not in profile_entry:
                                    profile_entry[field] = None
                                elif profile_entry[field] == '':
                                    profile_entry[field] = None
                            
                            # Try to extract business email from biography if not found
                            if not profile_entry.get('business_email') and profile_entry.get('biography'):
                                email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', profile_entry['biography'])
                                if email_match:
                                    profile_entry['business_email'] = email_match.group(0)
                            
                            # Remove None values for non-business fields
                            profile_entry = {k: v for k, v in profile_entry.items() if v is not None or k in business_fields}
                            all_extracted_data.append(profile_entry)
                            
                            print(f"  ✅ Successfully extracted profile data for @{username}")
                        else:
                            print(f"  ❌ Failed to extract profile data for @{username}: {profile_extracted_data.get('error')}")
                            
                except Exception as e:
                    print(f"  ❌ Error extracting profile data for @{username}: {str(e)}")
        
        # Always include business fields, even if null
        business_fields = ['business_email', 'business_phone_number', 'business_category_name']
        for field in business_fields:
            if field not in clean_entry:
                clean_entry[field] = None
            elif clean_entry[field] == '':
                clean_entry[field] = None
        
        # Try to extract business email from biography if not found
        if not clean_entry.get('business_email') and clean_entry.get('biography'):
            email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', clean_entry['biography'])
            if email_match:
                clean_entry['business_email'] = email_match.group(0)
        
        # Remove None values for non-business fields
        clean_entry = {k: v for k, v in clean_entry.items() if v is not None or k in business_fields}
        all_extracted_data.append(clean_entry)
        
        return {
            'success': True,
            'data': all_extracted_data,
            'usernames': usernames,
            'errors': []
        }
    
    async def _cleanup_old_contexts(self):
        """Clean up contexts that have reached usage limit"""
        contexts_to_cleanup = []
        
        for context in self.context_pool.contexts:
            usage_count = self.context_pool.context_usage_count.get(id(context), 0)
            if usage_count >= self.config.context_reuse_limit:
                contexts_to_cleanup.append(context)
        
        for context in contexts_to_cleanup:
            await self.context_pool.cleanup_context(context)
    
    def _determine_content_type_from_url(self, url: str, data: Dict[str, Any] = None) -> str:
        """Determine content type from URL and data"""
        
        parsed_url = urlparse(url)
        
        # Remove leading/trailing slashes and split path
        path = parsed_url.path.strip('/')
        path_parts = [part for part in path.split('/') if part]

        # Only process instagram.com URLs
        if 'instagram.com' not in parsed_url.netloc:
            return "unknown"

        if '/reel/' in url:
            return "video"
        elif '/p/' in url:
            # If no data provided, default to article (will be refined later with data)
            if data is None:
                return "article"
            # Check if it's actually a video post
            if (data.get('meta_data', {}).get('content_type') == 'video' or
                data.get('script_data', {}).get('is_video') or
                data.get('script_data', {}).get('video_url')):
                return "video"
            else:
                return "article"
        # Check for profile URLs: /[username]/ (only one path component)
        elif len(path_parts) == 1 and path_parts[0]:
            # Additional validation: username should be alphanumeric, dots, underscores
            username = path_parts[0]
            if username and all(c.isalnum() or c in '._' for c in username):
                return "profile"
    
        # All other Instagram URLs are unknown
        return "unknown"
    
    def _format_count(self, count) -> str:
        """Format count numbers to readable format (e.g., 16000 -> 16K)"""
        if count is None:
            return None
        
        try:
            count = int(count)
            if count >= 1000000:
                return f"{count/1000000:.1f}M".replace('.0', '')
            elif count >= 1000:
                return f"{count/1000:.1f}K".replace('.0', '')
            else:
                return str(count)
        except (ValueError, TypeError):
            return str(count) if count else None


async def scrape_instagram_urls_optimized(urls: List[str], 
                                        headless: bool = True,
                                        enable_anti_detection: bool = True,
                                        is_mobile: bool = False,
                                        output_file: Optional[str] = None,
                                        config: Optional[ScrapingConfig] = None) -> Dict[str, Any]:
    """
    Convenience function to scrape Instagram URLs with optimizations
    
    Args:
        urls: List of Instagram URLs to scrape
        headless: Run browser in headless mode (default: True)
        enable_anti_detection: Enable anti-detection features (default: True)
        is_mobile: Use mobile user agent and viewport (default: False)
        output_file: Optional file path to save results (default: None)
        config: Custom scraping configuration (default: None)
        
    Returns:
        Dictionary containing scraping results
    """
    scraper = OptimizedInstagramScraper(
        headless=headless,
        enable_anti_detection=enable_anti_detection,
        is_mobile=is_mobile,
        output_file=output_file,
        config=config
    )
    return await scraper.scrape(urls)


async def main():
    """Main function for command-line usage"""
    print("Instagram Scraper - Optimized Main Entry Point")
    print("=" * 60)
    
    # Example URLs (you can modify these or get them from user input)
    # example_urls = [
    #     "https://www.instagram.com/90svogue.__",
    #     "https://www.instagram.com/codehype_",
    #     "https://www.instagram.com/p/DMsercXMVeZ/",
    #     "https://www.instagram.com/reel/CSb6-Rap2Ip/"
    # ]
    example_urls = [
        "https://www.instagram.com/prattprattpratt/?hl=en",
        "https://www.instagram.com/evazubeck/?hl=en",
        "https://www.instagram.com/p/DE36aN2vRcw/",
        "https://www.instagram.com/p/C7DrIZdSnGc/"
    ]
    
    print("Example URLs:")
    for i, url in enumerate(example_urls, 1):
        content_type = "profile" if "/p/" not in url and "/reel/" not in url else ("video" if "/reel/" in url else "article")
        print(f"  {i}. {url} ({content_type})")
    
    # Ask user if they want to use example URLs or input their own
    choice = input("\nUse example URLs? (y/n): ").strip().lower()
    
    if choice == 'y':
        urls = example_urls
    else:
        print("\nEnter Instagram URLs (one per line, press Enter twice when done):")
        print("Supported URL types:")
        print("  - Profile: https://www.instagram.com/username/")
        print("  - Article: https://www.instagram.com/p/post_id/")
        print("  - Video: https://www.instagram.com/reel/reel_id/")
        urls = []
        while True:
            url = input().strip()
            if not url:
                break
            if 'instagram.com' in url:
                urls.append(url)
            else:
                print("⚠️  Please enter a valid Instagram URL")
        
        if not urls:
            print("No valid URLs provided. Using example URLs.")
            urls = example_urls
    
    # Ask for scraper options
    print("\nScraper Options:")
    headless = input("Run in headless mode? (y/n, default: y): ").strip().lower() != 'n'
    anti_detection = input("Enable anti-detection? (y/n, default: y): ").strip().lower() != 'n'
    mobile = input("Use mobile mode? (y/n, default: n): ").strip().lower() == 'y'
    save_file = input("Save results to file? (y/n, default: y): ").strip().lower() != 'n'
    
    output_file = None
    if save_file:
        output_file = input("Enter output filename (default: instagram_scraper/instagram_scraped_data_optimized.json): ").strip()
        if not output_file:
            output_file = "instagram_scraper/instagram_scraped_data_optimized.json"
    
    # Ask for performance configuration
    print("\nPerformance Configuration:")
    max_workers = input("Max concurrent workers (default: 5): ").strip()
    max_workers = int(max_workers) if max_workers.isdigit() else 5
    
    batch_size = input("Batch size (default: 8): ").strip()
    batch_size = int(batch_size) if batch_size.isdigit() else 8
    
    config = ScrapingConfig(
        max_workers=max_workers,
        batch_size=batch_size,
        context_pool_size=min(max_workers, 5),
        rate_limit_delay=1.0
    )
    
    print(f"\n🚀 Starting optimized scraping with {len(urls)} URLs...")
    print(f"   Max workers: {config.max_workers}")
    print(f"   Batch size: {config.batch_size}")
    print(f"   Context pool size: {config.context_pool_size}")
    print("   Note: Additional profile data will be automatically extracted for article/video URLs!")
    
    # Run the optimized scraper
    result = await scrape_instagram_urls_optimized(
        urls=urls,
        headless=headless,
        enable_anti_detection=anti_detection,
        is_mobile=mobile,
        output_file=output_file,
        config=config
    )
    
    # Display results
    print(f"\n📊 FINAL RESULTS:")
    print(f"   Success: {'✅' if result['success'] else '❌'}")
    print(f"   Original URLs: {result['summary'].get('total_original_urls', len(urls))}")
    print(f"   Additional profiles: {result['summary'].get('additional_profiles_extracted', 0)}")
    print(f"   Total data entries: {len(result['data'])}")
    print(f"   Errors: {len(result['errors'])}")
    print(f"   Throughput: {result['summary'].get('performance_metrics', {}).get('throughput_per_second', 0):.2f} URLs/second")
    
    if result['data']:
        print(f"\n📋 Extracted Data Preview:")
        for i, entry in enumerate(result['data'][:5], 1):  # Show first 5 entries
            content_type = entry.get('content_type', 'unknown')
            url = entry.get('url', 'unknown')
            if content_type == 'profile':
                username = entry.get('username', 'unknown')
                followers = entry.get('followers_count', 'unknown')
                full_name = entry.get('full_name', '')
                print(f"   {i}. Profile: @{username} ({followers} followers) - {full_name}")
            elif content_type in ['article', 'video']:
                username = entry.get('username', 'unknown')
                likes = entry.get('likes_count', 'unknown')
                comments = entry.get('comments_count', 'unknown')
                print(f"   {i}. {content_type.title()}: @{username} ({likes} likes, {comments} comments)")
        
        if len(result['data']) > 5:
            print(f"   ... and {len(result['data']) - 5} more entries")
    
    if result['errors']:
        print(f"\n❌ Errors encountered:")
        for error in result['errors']:
            print(f"   - {error['url']}: {error['error']}")
    
    if result['output_file']:
        print(f"\n💾 Results saved to: {result['output_file']}")
        
        # Show content type breakdown
        if result['summary'].get('content_type_breakdown'):
            print(f"\n📈 Content Type Breakdown:")
            for content_type, count in result['summary']['content_type_breakdown'].items():
                print(f"   • {content_type.title()}: {count} entries")


if __name__ == "__main__":
    # Run the main function
    asyncio.run(main())
