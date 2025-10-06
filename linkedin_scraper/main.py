"""
LinkedIn Scraper - Optimized Main Interface with Concurrency
Simple one-line usage: linkedin_scraper(urls)

Usage:
    from main import linkedin_scraper
    
    urls = [
        "https://www.linkedin.com/in/williamhgates/",
        "https://www.linkedin.com/company/microsoft/",
        "https://www.linkedin.com/posts/aiqod_inside-aiqod-how-were-building-enterprise-ready-activity-7348224698146541568-N7oQ",
        "https://www.linkedin.com/newsletters/aiqod-insider-7325820451622940672"
    ]
    
    results = linkedin_scraper(urls)
"""

import asyncio
import json
import time
import re
import sys
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
import random
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum

# Add parent directory to path to import database module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from linkedin_scraper.linkedin_data_extractor import LinkedInDataExtractor
# Orchestrator handles MongoDB persistence; scraper avoids direct DB usage


class ScrapingStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class ScrapingTask:
    url: str
    status: ScrapingStatus = ScrapingStatus.PENDING
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 2


class BrowserContextPool:
    """Manages a pool of browser contexts for concurrent scraping"""
    
    def __init__(self, pool_size: int = 5, headless: bool = True, enable_anti_detection: bool = True):
        self.pool_size = pool_size
        self.headless = headless
        self.enable_anti_detection = enable_anti_detection
        self.contexts = []
        self.available_contexts = asyncio.Queue()
        self.context_usage_count = {}
        self.max_usage_per_context = 20  # Recycle after 20 operations
        
    async def initialize(self):
        """Initialize the browser context pool"""
        print(f"🚀 Initializing browser context pool with {self.pool_size} contexts...")
        
        for i in range(self.pool_size):
            try:
                extractor = LinkedInDataExtractor(
                    headless=self.headless,
                    enable_anti_detection=self.enable_anti_detection
                )
                await extractor.start()
                
                self.contexts.append(extractor)
                self.context_usage_count[id(extractor)] = 0
                await self.available_contexts.put(extractor)
                
                print(f"✅ Context {i+1}/{self.pool_size} initialized")
                
            except Exception as e:
                print(f"❌ Failed to initialize context {i+1}: {e}")
        
        print(f"✅ Browser context pool initialized with {len(self.contexts)} contexts")
    
    async def get_context(self) -> LinkedInDataExtractor:
        """Get an available browser context"""
        context = await self.available_contexts.get()
        
        # Check if context needs recycling
        usage_count = self.context_usage_count.get(id(context), 0)
        if usage_count >= self.max_usage_per_context:
            print(f"🔄 Recycling context after {usage_count} operations")
            await self._recycle_context(context)
            context = await self.available_contexts.get()
        
        return context
    
    async def return_context(self, context: LinkedInDataExtractor):
        """Return a context to the pool"""
        self.context_usage_count[id(context)] = self.context_usage_count.get(id(context), 0) + 1
        await self.available_contexts.put(context)
    
    async def _recycle_context(self, old_context: LinkedInDataExtractor):
        """Recycle an old context by creating a new one"""
        try:
            # Stop old context
            await old_context.stop()
            
            # Create new context
            new_context = LinkedInDataExtractor(
                headless=self.headless,
                enable_anti_detection=self.enable_anti_detection
            )
            await new_context.start()
            
            # Replace in contexts list
            for i, ctx in enumerate(self.contexts):
                if id(ctx) == id(old_context):
                    self.contexts[i] = new_context
                    break
            
            # Reset usage count
            self.context_usage_count[id(new_context)] = 0
            
            print("✅ Context recycled successfully")
            
        except Exception as e:
            print(f"❌ Error recycling context: {e}")
    
    async def cleanup(self):
        """Clean up all browser contexts"""
        print("🧹 Cleaning up browser context pool...")
        
        for context in self.contexts:
            try:
                await context.stop()
            except Exception as e:
                print(f"⚠️ Error stopping context: {e}")
        
        self.contexts.clear()
        self.context_usage_count.clear()
        print("✅ Browser context pool cleaned up")


class RateLimiter:
    """Global rate limiter for coordinated request timing"""
    
    def __init__(self, requests_per_minute: int = 30):
        self.requests_per_minute = requests_per_minute
        self.request_times = []
        self.lock = asyncio.Lock()
    
    async def wait_if_needed(self):
        """Wait if we're hitting rate limits"""
        async with self.lock:
            now = time.time()
            
            # Remove requests older than 1 minute
            self.request_times = [t for t in self.request_times if now - t < 60]
            
            # If we're at the limit, wait
            if len(self.request_times) >= self.requests_per_minute:
                sleep_time = 60 - (now - self.request_times[0]) + 1
                if sleep_time > 0:
                    print(f"⏳ Rate limit reached, waiting {sleep_time:.1f}s")
                    await asyncio.sleep(sleep_time)
                    # Clean up old requests after waiting
                    now = time.time()
                    self.request_times = [t for t in self.request_times if now - t < 60]
            
            # Record this request
            self.request_times.append(now)


class OptimizedLinkedInScraper:
    """Optimized LinkedIn scraper with concurrency and resource management"""
    
    def __init__(self, 
                 headless: bool = True, 
                 enable_anti_detection: bool = True, 
                 use_mongodb: bool = True,
                 max_workers: int = 5,
                 batch_size: int = 8,
                 requests_per_minute: int = 30,
                 rate_limit_delay: float = 1.0,
                 icp_identifier: str = 'default'):
        
        self.headless = headless
        self.enable_anti_detection = enable_anti_detection
        self.use_mongodb = use_mongodb
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.rate_limit_delay = rate_limit_delay
        self.icp_identifier = icp_identifier
        
        # Initialize components
        self.context_pool = BrowserContextPool(
            pool_size=max_workers,
            headless=headless,
            enable_anti_detection=enable_anti_detection
        )
        self.rate_limiter = RateLimiter(requests_per_minute)
        self.semaphore = asyncio.Semaphore(max_workers)
        
        # DB operations are centralized in the orchestrator


    async def scrape_async(self, urls: List[str], output_filename: str = "linkedin_scraped_data.json") -> Dict[str, Any]:
        """Optimized async scraping with concurrency and batch processing"""
        
        print("=" * 80)
        print("🚀 OPTIMIZED LINKEDIN SCRAPER - STARTING")
        print("=" * 80)
        print(f"📋 URLs to scrape: {len(urls)}")
        print(f"👥 Max workers: {self.max_workers}")
        print(f"📦 Batch size: {self.batch_size}")
        print(f"📁 Output file: {output_filename}")
        print("=" * 80)
        
        # Initialize context pool
        await self.context_pool.initialize()
        
        # Create tasks
        tasks = [ScrapingTask(url=url) for url in urls]
        
        results = {
            "scraping_metadata": {
                "timestamp": time.time(),
                "date": time.strftime("%Y-%m-%d %H:%M:%S"),
                "total_urls": len(urls),
                "successful_scrapes": 0,
                "failed_scrapes": 0,
                "signup_pages_detected": 0,
                "signup_pages_retried": 0,
                "signup_pages_skipped": 0,
                "scraper_version": "optimized_linkedin_scraper_v2.0",
                "max_workers": self.max_workers,
                "batch_size": self.batch_size
            },
            "scraped_data": [],
            "signup_urls_flagged": [],
            "signup_urls_skipped": [],
            "failed_urls": []
        }
        
        try:
            # Process URLs in batches
            for batch_num, batch_tasks in enumerate(self._create_batches(tasks, self.batch_size), 1):
                print(f"\n🔄 Processing batch {batch_num}: {len(batch_tasks)} URLs")
                
                # Process batch concurrently
                batch_results = await self._process_batch(batch_tasks)
                
                # Update results
                self._update_results_from_batch(results, batch_results)
                
                # Brief pause between batches
                if batch_num < len(list(self._create_batches(tasks, self.batch_size))):
                    print("⏳ Pausing between batches...")
                    await asyncio.sleep(self.rate_limit_delay)
            
            # Phase 2: Retry sign-up flagged URLs with enhanced anti-detection
            if results["signup_urls_flagged"]:
                print(f"\n🔄 PHASE 2: RETRYING {len(results['signup_urls_flagged'])} SIGN-UP FLAGGED URLs")
                await self._retry_signup_urls(results)
            
            # Phase 3: Filter and save results
            print(f"\n💾 PHASE 3: FILTERING AND SAVING RESULTS")
            self._finalize_results(results)
            # Always attach unified leads for orchestrator-level persistence
            try:
                if results.get("scraped_data"):
                    unified_leads = [
                        self._transform_linkedin_to_unified(item, self.icp_identifier)
                        for item in results["scraped_data"]
                    ]
                    results['unified_leads'] = [u for u in unified_leads if u]
                else:
                    results['unified_leads'] = []
            except Exception:
                results['unified_leads'] = []
            self._save_results_to_file(results, output_filename)
            self._print_summary(results)
            
            return results
            
        except Exception as e:
            print(f"❌ Critical error in optimized LinkedIn scraper: {e}")
            raise
        
        finally:
            await self.context_pool.cleanup()

    def _transform_linkedin_to_unified(self, linkedin_data: Dict[str, Any], icp_identifier: str = 'default') -> Optional[Dict[str, Any]]:
        """Transform LinkedIn data to unified schema (local to scraper)"""
        try:
            # Map URL type to content type
            url_type = linkedin_data.get('url_type', '')
            content_type = {
                'profile': 'profile',
                'company': 'profile',
                'post': 'article',
                'newsletter': 'article'
            }.get(url_type, 'profile')

            full_name = linkedin_data.get('author_name') or linkedin_data.get('full_name')
            if not full_name or str(full_name).strip().lower() in { 'sign up','signup','log in','login','register','join now','get started','create account','sign in','signin','continue','next','submit','loading','please wait','error','page not found','404','access denied','unauthorized','linkedin','connect','follow','view profile' }:
                return None

            unified = {
                "url": linkedin_data.get('url', ""),
                "platform": "linkedin",
                "content_type": content_type,
                "source": "linkedin-scraper",
                "icp_identifier": icp_identifier,
                "profile": {
                    "username": linkedin_data.get('username', ""),
                    "full_name": full_name or "",
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
                    "scraped_at": datetime.utcnow().isoformat(),
                    "data_quality_score": "0.45"
                },
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
            return unified
        except Exception:
            return None
    
    def _create_batches(self, tasks: List[ScrapingTask], batch_size: int):
        """Create batches of tasks for processing"""
        for i in range(0, len(tasks), batch_size):
            yield tasks[i:i + batch_size]
    
    async def _process_batch(self, batch_tasks: List[ScrapingTask]) -> List[ScrapingTask]:
        """Process a batch of tasks concurrently"""
        
        # Create coroutines for concurrent execution
        coroutines = [self._scrape_single_url(task) for task in batch_tasks]
        
        # Execute concurrently with semaphore limiting
        results = await asyncio.gather(*coroutines, return_exceptions=True)
        
        # Handle any exceptions
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                batch_tasks[i].status = ScrapingStatus.FAILED
                batch_tasks[i].error = str(result)
                print(f"❌ Task failed with exception: {result}")
        
        return batch_tasks
    
    async def _scrape_single_url(self, task: ScrapingTask) -> ScrapingTask:
        """Scrape a single URL with resource management"""
        
        async with self.semaphore:  # Limit concurrent operations
            try:
                # Apply rate limiting
                await self.rate_limiter.wait_if_needed()
                
                # Apply additional rate limit delay
                await asyncio.sleep(self.rate_limit_delay)
                
                # Get browser context from pool
                context = await self.context_pool.get_context()
                
                try:
                    task.status = ScrapingStatus.IN_PROGRESS
                    
                    # Detect URL type and skip unknown URLs
                    url_type = context.browser_manager.detect_url_type(task.url)
                    if url_type == 'unknown':
                        print(f"⚠️ SKIPPING unknown URL type: {task.url}")
                        task.status = ScrapingStatus.SKIPPED
                        return task
                    
                    # Extract data
                    raw_data = await context.extract_linkedin_data(task.url)
                    
                    if raw_data.get('error'):
                        print(f"❌ Failed to scrape {task.url}: {raw_data['error']}")
                        task.status = ScrapingStatus.FAILED
                        task.error = raw_data['error']
                        return task
                    
                    # Structure the data
                    structured_data = self._structure_linkedin_data(raw_data)
                    
                    if structured_data:
                        # Check if this is sign-up data
                        if self._is_signup_data(structured_data):
                            print(f"🚫 SIGN-UP PAGE DETECTED: {task.url}")
                            task.result = {
                                "url": task.url,
                                "detected_data": structured_data,
                                "is_signup": True
                            }
                            task.status = ScrapingStatus.COMPLETED
                        else:
                            task.result = structured_data
                            task.status = ScrapingStatus.COMPLETED
                            print(f"✅ Successfully scraped: {structured_data.get('full_name', 'Unknown')}")
                    else:
                        print(f"❌ Failed to structure data for {task.url}")
                        task.status = ScrapingStatus.FAILED
                        task.error = "Failed to structure data"
                
                finally:
                    # Always return context to pool
                    await self.context_pool.return_context(context)
                
            except Exception as e:
                print(f"❌ Error scraping {task.url}: {str(e)}")
                task.status = ScrapingStatus.FAILED
                task.error = str(e)
        
        return task
    
    async def _retry_signup_urls(self, results: Dict[str, Any]):
        """Retry sign-up flagged URLs with enhanced anti-detection"""
        
        retry_tasks = []
        for signup_item in results["signup_urls_flagged"]:
            task = ScrapingTask(url=signup_item["url"])
            task.retry_count = 1
            retry_tasks.append(task)
        
        print(f"🔄 Retrying {len(retry_tasks)} sign-up URLs with enhanced anti-detection...")
        
        # Process retry tasks in smaller batches
        retry_batch_size = min(3, len(retry_tasks))  # Smaller batches for retries
        for batch_num, batch_tasks in enumerate(self._create_batches(retry_tasks, retry_batch_size), 1):
            print(f"🔄 Retry batch {batch_num}: {len(batch_tasks)} URLs")
            
            # Process retry batch
            retry_results = await self._process_batch(batch_tasks)
            
            # Update results
            for task in retry_results:
                if task.status == ScrapingStatus.COMPLETED and task.result:
                    if task.result.get("is_signup"):
                        # Still sign-up data, skip it
                        results["signup_urls_skipped"].append({
                            "url": task.url,
                            "reason": "Still shows sign-up page after retry"
                        })
                        results["scraping_metadata"]["signup_pages_skipped"] += 1
                    else:
                        # Success! Got real data
                        results["scraped_data"].append(task.result)
                        results["scraping_metadata"]["successful_scrapes"] += 1
                        print(f"✅ RETRY SUCCESS: {task.result.get('full_name', 'Unknown')}")
                
                elif task.status == ScrapingStatus.FAILED:
                    results["signup_urls_skipped"].append({
                        "url": task.url,
                        "reason": f"Retry error: {task.error}"
                    })
                    results["scraping_metadata"]["signup_pages_skipped"] += 1
            
            results["scraping_metadata"]["signup_pages_retried"] += len(batch_tasks)
            
            # Longer pause between retry batches
            if batch_num < len(list(self._create_batches(retry_tasks, retry_batch_size))):
                await asyncio.sleep(self.rate_limit_delay * 3)  # 3x the normal delay for retries
    
    def _update_results_from_batch(self, results: Dict[str, Any], batch_tasks: List[ScrapingTask]):
        """Update results from a batch of completed tasks"""
        
        for task in batch_tasks:
            if task.status == ScrapingStatus.COMPLETED and task.result:
                if task.result.get("is_signup"):
                    results["signup_urls_flagged"].append({
                        "url": task.url,
                        "detected_data": task.result["detected_data"]
                    })
                    results["scraping_metadata"]["signup_pages_detected"] += 1
                else:
                    # Add ICP identifier to the scraped data
                    task.result['icp_identifier'] = self.icp_identifier
                    results["scraped_data"].append(task.result)
                    results["scraping_metadata"]["successful_scrapes"] += 1
            
            elif task.status == ScrapingStatus.FAILED:
                results["failed_urls"].append({
                    "url": task.url,
                    "error": task.error
                })
                results["scraping_metadata"]["failed_scrapes"] += 1
    
    def _finalize_results(self, results: Dict[str, Any]):
        """Final filter to ensure no sign-up data gets through"""
        
        # Final filter to ensure no sign-up data gets through
        filtered_data = []
        for item in results["scraped_data"]:
            if not self._is_signup_data(item):
                filtered_data.append(item)
            else:
                print(f"🚫 FINAL FILTER: Removing sign-up data for {item.get('url', 'Unknown URL')}")
        
        results["scraped_data"] = filtered_data
        
        # Update final counts
        results["scraping_metadata"]["successful_scrapes"] = len(filtered_data)
    
    def _is_signup_data(self, structured_data: Dict[str, Any]) -> bool:
        """Detect if scraped data is from a sign-up page"""
        if not structured_data:
            return False
        
        # Check for common sign-up indicators
        signup_indicators = [
            "sign up", "signup", "join linkedin", "create account",
            "register", "get started", "welcome to linkedin",
            "member login", "log in", "continue with", "create profile"
        ]
        # Normalize fields
        def normalize(value: Any) -> str:
            """Convert value to lowercase string, handle lists gracefully"""
            if isinstance(value, list):
                return " ".join([str(v).lower().strip() for v in value])
            elif isinstance(value, str):
                return value.lower().strip()
            return ""
            
        full_name = normalize(structured_data.get('full_name', ''))
        job_title = normalize(structured_data.get('job_title', ''))
        title = normalize(structured_data.get('title', ''))
        about = normalize(structured_data.get('about', ''))
        
        # Check if any field contains signup indicators
        fields_to_check = [full_name, job_title, title, about]
        
        for field in fields_to_check:
            if field:
                for indicator in signup_indicators:
                    if indicator in field:
                        return True
        
        # Additional checks for specific patterns
        if full_name == "sign up" or job_title == "linkedin":
            return True
        
        # Check if about contains LinkedIn's default signup description
        if "million+ members" in about and "manage your professional identity" in about:
            return True
        
        return False
    
    def _structure_linkedin_data(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Structure raw LinkedIn data according to requirements"""
        
        url = raw_data.get('url', '')
        url_type = raw_data.get('url_type', 'unknown')
        
        # Get combined data (primary source)
        combined_data = raw_data.get('extracted_data', {})
        
        # Get fallback data
        json_ld_data = raw_data.get('json_ld_data', {}).get('parsed_data', {})
        meta_data = raw_data.get('meta_data', {})
        
        # Base structure
        structured = {
            "url": url,
            "url_type": url_type,
            "scraping_timestamp": time.time(),
            "scraping_date": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Structure data based on URL type
        if url_type == "profile":
            structured.update(self._structure_profile_data(combined_data, json_ld_data, meta_data, url))
        
        elif url_type == "company":
            structured.update(self._structure_company_data(combined_data, json_ld_data, meta_data, url))
        
        elif url_type == "post":
            structured.update(self._structure_post_data(combined_data, json_ld_data, meta_data, url))
        
        elif url_type == "newsletter":
            structured.update(self._structure_newsletter_data(combined_data, json_ld_data, meta_data, url))
        
        else:
            # Generic structure
            structured.update(self._structure_generic_data(combined_data, json_ld_data, meta_data, url))
        
        return structured if self._has_meaningful_data(structured) else None
    
    def _structure_profile_data(self, combined: Dict, json_ld: Dict, meta: Dict, url: str) -> Dict[str, Any]:
        """Structure profile data"""
        
        # Extract username from URL
        username_match = re.search(r'linkedin\.com/in/([^/?]+)', url)
        username = username_match.group(1) if username_match else ""
        
        return {
            "username": username,
            "full_name": self._get_reliable_value([
                combined.get('name'),
                json_ld.get('name'),
                meta.get('open_graph', {}).get('og:title', '').split(' | ')[0] if ' | ' in meta.get('open_graph', {}).get('og:title', '') else meta.get('open_graph', {}).get('og:title'),
                meta.get('title', '').split(' | ')[0] if ' | ' in meta.get('title', '') else meta.get('title')
            ]),
            "job_title": self._get_reliable_value([
                combined.get('job_title'),
                json_ld.get('job_title'),
                self._extract_title_from_meta(meta)
            ]),
            "title": self._get_reliable_value([
                meta.get("title").split(" - ", 1)[-1].split(" | ", 1)[0]
            ]),
            "followers": self._get_reliable_value([
                combined.get('followers'),
                json_ld.get('followers'),
                combined.get('author_followers')
            ], convert_to_int=True),
            "connections": self._get_reliable_value([
                combined.get('connections')
            ], convert_to_int=True),
            "about": self._get_reliable_value([
                combined.get('description'),
                json_ld.get('description'),
                meta.get('open_graph', {}).get('og:description'),
                meta.get('description')
            ]),
            "location": self._get_reliable_value([
                combined.get('location'),
                json_ld.get('location'),
                combined.get('country')
            ]),
            "website": self._get_reliable_value([
                combined.get('same_as'),
                json_ld.get('same_as'),
                combined.get('url')
            ]),
            "contact_info": {}  # Not typically available in public data
        }
    
    def _structure_company_data(self, combined: Dict, json_ld: Dict, meta: Dict, url: str) -> Dict[str, Any]:
        """Structure company data"""
        
        # Extract username from URL
        username_match = re.search(r'linkedin\.com/company/([^/?]+)', url)
        username = username_match.group(1) if username_match else ""
        return {
            "username": username,
            "full_name": self._get_reliable_value([
                json_ld.get('name'),
                combined.get('name'),
                meta.get('title', '').split(' | ')[0] if ' | ' in meta.get('title', '') else meta.get('title')
            ]),
            "address": self._format_company_address(json_ld.get('address', {})),
            "website": self._get_reliable_value([
                combined.get('same_as'),
                json_ld.get('same_as')
            ]),
            "about_us": self._get_reliable_value([
                json_ld.get('description')
            ]),
            "employee_count": self._get_reliable_value([
                json_ld.get('employee_count')
            ], convert_to_int=True)
        }
    
    def _structure_post_data(self, combined: Dict, json_ld: Dict, meta: Dict, url: str) -> Dict[str, Any]:
        """Structure post data"""
        
        return {
            "url": url,
            "headline": self._get_reliable_value([
                combined.get('headline'),
                json_ld.get('headline'),
                meta.get('open_graph', {}).get('og:title'),
                meta.get('title')
            ]),
            "author_url": self._get_reliable_value([
                combined.get('author', {}).get('url') if isinstance(combined.get('author'), dict) else None,
                json_ld.get('author', {}).get('url') if isinstance(json_ld.get('author'), dict) else None
            ]),
            "author_name": self._get_reliable_value([
                combined.get('author', {}).get('name') if isinstance(combined.get('author'), dict) else None,
                json_ld.get('author', {}).get('name') if isinstance(json_ld.get('author'), dict) else None
            ]),
            "full_name": self._get_reliable_value([
                combined.get('author', {}).get('name') if isinstance(combined.get('author'), dict) else None,
                json_ld.get('author', {}).get('name') if isinstance(json_ld.get('author'), dict) else None
            ]),
            "comment_count": self._get_reliable_value([
                combined.get('comment_count'),
                combined.get('comments_count'),
                json_ld.get('comment_count')
            ], convert_to_int=True),
            "likes_count": self._get_reliable_value([
                combined.get('likes'),
                json_ld.get('likes')
            ], convert_to_int=True),
            "followers": self._get_reliable_value([
                combined.get('author_followers'),
                json_ld.get('author_followers')
            ], convert_to_int=True),
            "date_published": self._get_reliable_value([
                combined.get('date_published'),
                json_ld.get('date_published')
            ])
        }
    
    def _structure_newsletter_data(self, combined: Dict, json_ld: Dict, meta: Dict, url: str) -> Dict[str, Any]:
        """Structure newsletter data"""
        
        # Extract username from URL
        username_match = re.search(r'linkedin\.com/newsletters/([^/?]+)', url)
        username = username_match.group(1) if username_match else ""
        
        return {
            "username": username,
            "full_name": self._get_reliable_value([
                combined.get('name'),
                json_ld.get('name'),
                meta.get('open_graph', {}).get('og:title', '').split(' | ')[0] if ' | ' in meta.get('open_graph', {}).get('og:title', '') else meta.get('open_graph', {}).get('og:title'),
                meta.get('title', '').split(' | ')[0] if ' | ' in meta.get('title', '') else meta.get('title')
            ]),
            "description": self._get_reliable_value([
                combined.get('description'),
                json_ld.get('description'),
                meta.get('open_graph', {}).get('og:description'),
                meta.get('description')
            ]),
            "author_name": self._get_reliable_value([
                combined.get('name'),
                json_ld.get('name'),
                meta.get('open_graph', {}).get('og:title', '').split(' | ')[0] if ' | ' in meta.get('open_graph', {}).get('og:title', '') else meta.get('open_graph', {}).get('og:title'),
                meta.get('title', '').split(' | ')[0] if ' | ' in meta.get('title', '') else meta.get('title')
            ]),
            "date_published": self._get_reliable_value([
                combined.get('date_published'),
                json_ld.get('date_published')
            ])
        }
    
    def _structure_generic_data(self, combined: Dict, json_ld: Dict, meta: Dict, url: str) -> Dict[str, Any]:
        """Structure generic data for unknown URL types"""
        
        return {
            "full_name": self._get_reliable_value([
                combined.get('name'),
                json_ld.get('name'),
                meta.get('open_graph', {}).get('og:title'),
                meta.get('title')
            ]),
            "description": self._get_reliable_value([
                combined.get('description'),
                json_ld.get('description'),
                meta.get('open_graph', {}).get('og:description'),
                meta.get('description')
            ]),
            "url": url,
            "image_url": self._get_reliable_value([
                combined.get('image_url'),
                json_ld.get('image_url'),
                meta.get('open_graph', {}).get('og:image')
            ])
        }
    
    def _get_reliable_value(self, values: List[Any], convert_to_int: bool = False) -> Any:
        """Get the most reliable non-empty value from a list"""
        
        for value in values:
            if value is not None and value != '' and value != 'N/A':
                if convert_to_int:
                    try:
                        if isinstance(value, str):
                            # Remove commas and convert
                            clean_value = value.replace(',', '').replace(' ', '')
                            return int(clean_value)
                        elif isinstance(value, (int, float)):
                            return int(value)
                    except (ValueError, TypeError):
                        continue
                else:
                    return value
        
        return None if not convert_to_int else 0
    
    def _extract_title_from_meta(self, meta: Dict) -> Optional[str]:
        """Extract job title from meta data"""
        
        og_title = meta.get('open_graph', {}).get('og:title', '')
        if ' | ' in og_title:
            parts = og_title.split(' | ')
            if len(parts) > 1:
                return parts[1]  # Usually job title comes after name
        
        return None
    
    def _format_company_address(self, address_dict: Dict) -> str:
        """Format company address from dictionary"""
        
        if not address_dict:
            return ""
        
        address_parts = []
        
        if address_dict.get('street'):
            address_parts.append(address_dict['street'])
        if address_dict.get('city'):
            address_parts.append(address_dict['city'])
        if address_dict.get('region'):
            address_parts.append(address_dict['region'])
        if address_dict.get('postal_code'):
            address_parts.append(address_dict['postal_code'])
        if address_dict.get('country'):
            address_parts.append(address_dict['country'])
        
        return ', '.join(address_parts) if address_parts else ""
    
    def _has_meaningful_data(self, structured: Dict[str, Any]) -> bool:
        """Check if structured data has meaningful content"""
        
        # Must have at least a name or title
        key_fields = ['full_name', 'name', 'headline', 'title']
        
        for field in key_fields:
            if structured.get(field) and structured[field].strip():
                return True
        
        return False
    
    def _save_results_to_file(self, results: Dict[str, Any], filename: str) -> None:
        """Save results to JSON file and attach unified leads for orchestrator"""
        
        # Build unified leads for orchestrator-level persistence
        if results.get("scraped_data"):
            unified_leads = [
                self._transform_linkedin_to_unified(item, self.icp_identifier)
                for item in results["scraped_data"]
            ]
            unified_leads = [u for u in unified_leads if u]
            if unified_leads:
                results['unified_leads'] = unified_leads
        
        # Save to file as backup
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"\n💾 Results also saved to file: {filename}")
            print(f"   File size: {len(json.dumps(results, indent=2, ensure_ascii=False, default=str)):,} characters")
        
        except Exception as e:
            print(f"❌ Error saving results to {filename}: {e}")
    
    def _print_summary(self, results: Dict[str, Any]) -> None:
        """Print scraping summary"""
        
        metadata = results.get("scraping_metadata", {})
        successful = metadata.get("successful_scrapes", 0)
        failed = metadata.get("failed_scrapes", 0)
        signup_detected = metadata.get("signup_pages_detected", 0)
        signup_retried = metadata.get("signup_pages_retried", 0)
        signup_skipped = metadata.get("signup_pages_skipped", 0)
        total = metadata.get("total_urls", 0)
        
        print(f"\n{'='*80}")
        print("🎯 OPTIMIZED LINKEDIN SCRAPING SUMMARY")
        print(f"{'='*80}")
        print(f"✅ Successful: {successful}/{total} ({successful/total*100 if total > 0 else 0:.1f}%)")
        print(f"❌ Failed: {failed}/{total} ({failed/total*100 if total > 0 else 0:.1f}%)")
        print(f"🚫 Sign-up pages detected: {signup_detected}")
        print(f"🔄 Sign-up pages retried: {signup_retried}")
        print(f"⏭️ Sign-up pages skipped: {signup_skipped}")
        print(f"👥 Max workers used: {metadata.get('max_workers', 'N/A')}")
        print(f"📦 Batch size: {metadata.get('batch_size', 'N/A')}")
        
        if results.get("scraped_data"):
            print(f"\n📊 Successfully scraped:")
            for item in results["scraped_data"]:
                name = item.get('full_name', 'Unknown')
                url_type = item.get('url_type', 'unknown')
                print(f"  ✓ {name} ({url_type})")
        
        if results.get("failed_urls"):
            print(f"\n❌ Failed URLs:")
            for item in results["failed_urls"]:
                print(f"  ✗ {item['url']}: {item['error']}")
        
        if results.get("signup_urls_skipped"):
            print(f"\n🚫 Sign-up URLs skipped after retry:")
            for item in results["signup_urls_skipped"]:
                print(f"  ⏭️ {item['url']}: {item['reason']}")
        print(f"{'='*80}")


class LinkedInScraperMain:
    """Main LinkedIn Scraper class with simplified interface (backward compatibility)"""
    
    def __init__(self, headless: bool = True, enable_anti_detection: bool = True, use_mongodb: bool = True):
        self.headless = headless
        self.enable_anti_detection = enable_anti_detection
        self.use_mongodb = use_mongodb
        self.extractor = None
        
        # Initialize MongoDB manager if needed
        if self.use_mongodb:
            try:
                self.mongodb_manager = get_mongodb_manager()
                print("✅ MongoDB connection initialized")
            except Exception as e:
                print(f"⚠️ Failed to initialize MongoDB: {e}")
                self.use_mongodb = False
    
    def _is_signup_data(self, structured_data: Dict[str, Any]) -> bool:
        """Detect if scraped data is from a sign-up page"""
        if not structured_data:
            return False
        
        # Check for common sign-up indicators
        signup_indicators = [
            "sign up", "signup", "join linkedin", "create account",
            "register", "get started", "welcome to linkedin",
            "member login", "log in", "continue with", "create profile"
        ]
        # Normalize fields
        def normalize(value: Any) -> str:
            """Convert value to lowercase string, handle lists gracefully"""
            if isinstance(value, list):
                return " ".join([str(v).lower().strip() for v in value])
            elif isinstance(value, str):
                return value.lower().strip()
            return ""
            
        full_name = normalize(structured_data.get('full_name', ''))
        job_title = normalize(structured_data.get('job_title', ''))
        title = normalize(structured_data.get('title', ''))
        about = normalize(structured_data.get('about', ''))
        
        # Check if any field contains signup indicators
        fields_to_check = [full_name, job_title, title, about]
        
        for field in fields_to_check:
            if field:
                for indicator in signup_indicators:
                    if indicator in field:
                        return True
        
        # Additional checks for specific patterns
        if full_name == "sign up" or job_title == "linkedin":
            return True
        
        # Check if about contains LinkedIn's default signup description
        if "million+ members" in about and "manage your professional identity" in about:
            return True
        
        return False
    
    async def _retry_with_enhanced_anti_detection(self, url: str) -> Optional[Dict[str, Any]]:
        """Retry scraping with enhanced anti-detection measures"""
        print(f"🔄 Retrying with enhanced anti-detection: {url}")
        
        try:
            # Create new extractor with enhanced settings for retry
            enhanced_extractor = LinkedInDataExtractor(
                headless=self.headless, 
                enable_anti_detection=True,
                # Enhanced anti-detection settings
                is_mobile=True,  # Try mobile user agent
            )
            
            await enhanced_extractor.start()
            
            # Add random delay before retry
            await asyncio.sleep(random.uniform(2.0, 4.0))
            
            # Detect URL type and prepare Google referer only for profiles
            url_type = enhanced_extractor.browser_manager.detect_url_type(url)
            google_referer: Optional[str] = None
            if url_type == 'profile':
                # Simulate coming from Google search results for this profile
                username_match = re.search(r'linkedin\.com/in/([^/?]+)', url)
                search_query = username_match.group(1) if username_match else ''
                if search_query:
                    google_referer = f"https://www.google.com/search?q=site%3Alinkedin.com%2Fin%2F+{search_query}"
                else:
                    google_referer = 'https://www.google.com/'
                print("🔎 Using Google referer for profile retry")
            
            # Extract data with enhanced settings and optional referer
            raw_data = await enhanced_extractor.extract_linkedin_data(url, referer=google_referer)
            
            if raw_data.get('error'):
                print(f"❌ Enhanced retry failed: {raw_data['error']}")
                return None
            
            # Structure the data
            structured_data = self._structure_linkedin_data(raw_data)
            
            await enhanced_extractor.stop()
            return structured_data
        
        except Exception as e:
            print(f"❌ Enhanced retry error for {url}: {str(e)}")
            return None

    async def scrape_async(self, urls: List[str], output_filename: str = "linkedin_scraped_data.json") -> Dict[str, Any]:
        """Legacy async method - now uses optimized scraper"""
        
        # Use optimized scraper for better performance
        optimized_scraper = OptimizedLinkedInScraper(
            headless=self.headless,
            enable_anti_detection=self.enable_anti_detection,
            use_mongodb=self.use_mongodb,
            max_workers=5,
            batch_size=8,
            rate_limit_delay=1.0
        )
        
        return await optimized_scraper.scrape_async(urls, output_filename)
    
    def _structure_linkedin_data(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Structure raw LinkedIn data according to requirements"""
        
        url = raw_data.get('url', '')
        url_type = raw_data.get('url_type', 'unknown')
        
        # Get combined data (primary source)
        combined_data = raw_data.get('extracted_data', {})
        
        # Get fallback data
        json_ld_data = raw_data.get('json_ld_data', {}).get('parsed_data', {})
        meta_data = raw_data.get('meta_data', {})
        
        # Base structure
        structured = {
            "url": url,
            "url_type": url_type,
            "scraping_timestamp": time.time(),
            "scraping_date": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        # print("="*100)
        # print(f"Combined Data: {combined_data}")
        # print("="*100)
        # print(f"JSON-LD Data: {json_ld_data}")
        # print("="*100)
        # print(f"Meta Data: {meta_data}")
        # print("="*100)
        # print(f"URL: {url}")
        # print("="*100)
        # Structure data based on URL type
        if url_type == "profile":
            structured.update(self._structure_profile_data(combined_data, json_ld_data, meta_data, url))
        
        elif url_type == "company":
            structured.update(self._structure_company_data(combined_data, json_ld_data, meta_data, url))
        
        elif url_type == "post":
            structured.update(self._structure_post_data(combined_data, json_ld_data, meta_data, url))
        
        elif url_type == "newsletter":
            structured.update(self._structure_newsletter_data(combined_data, json_ld_data, meta_data, url))
        
        else:
            # Generic structure
            structured.update(self._structure_generic_data(combined_data, json_ld_data, meta_data, url))
        
        return structured if self._has_meaningful_data(structured) else None

    def _structure_profile_data(self, combined: Dict, json_ld: Dict, meta: Dict, url: str) -> Dict[str, Any]:
        """Structure profile data"""
        
        # Extract username from URL
        username_match = re.search(r'linkedin\.com/in/([^/?]+)', url)
        username = username_match.group(1) if username_match else ""

        return {
            "username": username,
            "full_name": self._get_reliable_value([
                combined.get('name'),
                json_ld.get('name'),
                meta.get('open_graph', {}).get('og:title', '').split(' | ')[0] if ' | ' in meta.get('open_graph', {}).get('og:title', '') else meta.get('open_graph', {}).get('og:title'),
                meta.get('title', '').split(' | ')[0] if ' | ' in meta.get('title', '') else meta.get('title')
            ]),
            "job_title": None,
            "title": self._get_reliable_value([
                meta.get("title").split(" - ", 1)[-1].split(" | ", 1)[0]
            ]),
            "followers": self._get_reliable_value([
                combined.get('followers'),
                json_ld.get('followers'),
                combined.get('author_followers')
            ], convert_to_int=True),
            "connections": self._get_reliable_value([
                combined.get('connections')
            ], convert_to_int=True),
            "about": self._get_reliable_value([
                combined.get('description'),
                json_ld.get('description'),
                meta.get('open_graph', {}).get('og:description'),
                meta.get('description')
            ]),
            "location": self._get_reliable_value([
                combined.get('location'),
                json_ld.get('location'),
                combined.get('country')
            ]),
            "website": self._get_reliable_value([
                combined.get('same_as'),
                json_ld.get('same_as'),
                combined.get('url')
            ]),
            "contact_info": {}  # Not typically available in public data
        }
    
    def _structure_company_data(self, combined: Dict, json_ld: Dict, meta: Dict, url: str) -> Dict[str, Any]:
        """Structure company data"""
        
        # Extract username from URL
        username_match = re.search(r'linkedin\.com/company/([^/?]+)', url)
        username = username_match.group(1) if username_match else ""
        return {
            "username": username,
            "full_name": self._get_reliable_value([
                json_ld.get('name'),
                combined.get('name'),
                meta.get('title', '').split(' | ')[0] if ' | ' in meta.get('title', '') else meta.get('title')
            ]),
            "address": self._format_company_address(json_ld.get('address', {})),
            "website": self._get_reliable_value([
                combined.get('same_as'),
                json_ld.get('same_as')
            ]),
            "about_us": self._get_reliable_value([
                json_ld.get('description')
            ]),
            "employee_count": self._get_reliable_value([
                json_ld.get('employee_count')
            ], convert_to_int=True)
        }
    
    def _structure_post_data(self, combined: Dict, json_ld: Dict, meta: Dict, url: str) -> Dict[str, Any]:
        """Structure post data"""
        
        return {
            "url": url,
            "headline": self._get_reliable_value([
                combined.get('headline'),
                json_ld.get('headline'),
                meta.get('open_graph', {}).get('og:title'),
                meta.get('title')
            ]),
            "author_url": self._get_reliable_value([
                combined.get('author', {}).get('url') if isinstance(combined.get('author'), dict) else None,
                json_ld.get('author', {}).get('url') if isinstance(json_ld.get('author'), dict) else None
            ]),
            "author_name": self._get_reliable_value([
                combined.get('author', {}).get('name') if isinstance(combined.get('author'), dict) else None,
                json_ld.get('author', {}).get('name') if isinstance(json_ld.get('author'), dict) else None
            ]),
            "full_name": self._get_reliable_value([
                combined.get('author', {}).get('name') if isinstance(combined.get('author'), dict) else None,
                json_ld.get('author', {}).get('name') if isinstance(json_ld.get('author'), dict) else None
            ]),
            "comment_count": self._get_reliable_value([
                combined.get('comment_count'),
                combined.get('comments_count'),
                json_ld.get('comment_count')
            ], convert_to_int=True),
            "likes_count": self._get_reliable_value([
                combined.get('likes'),
                json_ld.get('likes')
            ], convert_to_int=True),
            "followers": self._get_reliable_value([
                combined.get('author_followers'),
                json_ld.get('author_followers')
            ], convert_to_int=True),
            "date_published": self._get_reliable_value([
                combined.get('date_published'),
                json_ld.get('date_published')
            ])
        }
    
    def _structure_newsletter_data(self, combined: Dict, json_ld: Dict, meta: Dict, url: str) -> Dict[str, Any]:
        """Structure newsletter data"""
        
        # Extract username from URL
        username_match = re.search(r'linkedin\.com/newsletters/([^/?]+)', url)
        username = username_match.group(1) if username_match else ""
        
        return {
            "username": username,
            "full_name": self._get_reliable_value([
                combined.get('name'),
                json_ld.get('name'),
                meta.get('open_graph', {}).get('og:title', '').split(' | ')[0] if ' | ' in meta.get('open_graph', {}).get('og:title', '') else meta.get('open_graph', {}).get('og:title'),
                meta.get('title', '').split(' | ')[0] if ' | ' in meta.get('title', '') else meta.get('title')
            ]),
            "description": self._get_reliable_value([
                combined.get('description'),
                json_ld.get('description'),
                meta.get('open_graph', {}).get('og:description'),
                meta.get('description')
            ]),
            "author_name": self._get_reliable_value([
                combined.get('name'),
                json_ld.get('name'),
                meta.get('open_graph', {}).get('og:title', '').split(' | ')[0] if ' | ' in meta.get('open_graph', {}).get('og:title', '') else meta.get('open_graph', {}).get('og:title'),
                meta.get('title', '').split(' | ')[0] if ' | ' in meta.get('title', '') else meta.get('title')
            ]),
            "date_published": self._get_reliable_value([
                combined.get('date_published'),
                json_ld.get('date_published')
            ])
        }
    
    def _structure_generic_data(self, combined: Dict, json_ld: Dict, meta: Dict, url: str) -> Dict[str, Any]:
        """Structure generic data for unknown URL types"""
        
        return {
            "full_name": self._get_reliable_value([
                combined.get('name'),
                json_ld.get('name'),
                meta.get('open_graph', {}).get('og:title'),
                meta.get('title')
            ]),
            "description": self._get_reliable_value([
                combined.get('description'),
                json_ld.get('description'),
                meta.get('open_graph', {}).get('og:description'),
                meta.get('description')
            ]),
            "url": url,
            "image_url": self._get_reliable_value([
                combined.get('image_url'),
                json_ld.get('image_url'),
                meta.get('open_graph', {}).get('og:image')
            ])
        }
    
    def _get_reliable_value(self, values: List[Any], convert_to_int: bool = False) -> Any:
        """Get the most reliable non-empty value from a list"""
        
        for value in values:
            if value is not None and value != '' and value != 'N/A':
                if convert_to_int:
                    try:
                        if isinstance(value, str):
                            # Remove commas and convert
                            clean_value = value.replace(',', '').replace(' ', '')
                            return int(clean_value)
                        elif isinstance(value, (int, float)):
                            return int(value)
                    except (ValueError, TypeError):
                        continue
                else:
                    return value
        
        return None if not convert_to_int else 0
    
    def _extract_title_from_meta(self, meta: Dict) -> Optional[str]:
        """Extract job title from meta data"""
        
        og_title = meta.get('open_graph', {}).get('og:title', '')
        if ' | ' in og_title:
            parts = og_title.split(' | ')
            if len(parts) > 1:
                return parts[1]  # Usually job title comes after name
        
        return None
    
    def _format_company_address(self, address_dict: Dict) -> str:
        """Format company address from dictionary"""
        
        if not address_dict:
            return ""
        
        address_parts = []
        
        if address_dict.get('street'):
            address_parts.append(address_dict['street'])
        if address_dict.get('city'):
            address_parts.append(address_dict['city'])
        if address_dict.get('region'):
            address_parts.append(address_dict['region'])
        if address_dict.get('postal_code'):
            address_parts.append(address_dict['postal_code'])
        if address_dict.get('country'):
            address_parts.append(address_dict['country'])
        
        return ', '.join(address_parts) if address_parts else ""
    
    def _has_meaningful_data(self, structured: Dict[str, Any]) -> bool:
        """Check if structured data has meaningful content"""
        
        # Must have at least a name or title
        key_fields = ['full_name', 'name', 'headline', 'title']
        
        for field in key_fields:
            if structured.get(field) and structured[field].strip():
                return True
        
        return False
    
    # Removed legacy _save_results_to_file method with MongoDB writes to avoid duplication.
    
    def _print_summary(self, results: Dict[str, Any]) -> None:
        """Print scraping summary"""
        
        metadata = results.get("scraping_metadata", {})
        successful = metadata.get("successful_scrapes", 0)
        failed = metadata.get("failed_scrapes", 0)
        signup_detected = metadata.get("signup_pages_detected", 0)
        signup_retried = metadata.get("signup_pages_retried", 0)
        signup_skipped = metadata.get("signup_pages_skipped", 0)
        total = metadata.get("total_urls", 0)
        
        print(f"\n{'='*80}")
        print("🎯 LINKEDIN SCRAPING SUMMARY")
        print(f"{'='*80}")
        print(f"✅ Successful: {successful}/{total} ({successful/total*100 if total > 0 else 0:.1f}%)")
        print(f"❌ Failed: {failed}/{total} ({failed/total*100 if total > 0 else 0:.1f}%)")
        print(f"🚫 Sign-up pages detected: {signup_detected}")
        print(f"🔄 Sign-up pages retried: {signup_retried}")
        print(f"⏭️ Sign-up pages skipped: {signup_skipped}")
        
        if results.get("scraped_data"):
            print(f"\n📊 Successfully scraped:")
            for item in results["scraped_data"]:
                name = item.get('full_name', 'Unknown')
                url_type = item.get('url_type', 'unknown')
                print(f"  ✓ {name} ({url_type})")
        
        if results.get("failed_urls"):
            print(f"\n❌ Failed URLs:")
            for item in results["failed_urls"]:
                print(f"  ✗ {item['url']}: {item['error']}")
        
        if results.get("signup_urls_skipped"):
            print(f"\n🚫 Sign-up URLs skipped after retry:")
            for item in results["signup_urls_skipped"]:
                print(f"  ⏭️ {item['url']}: {item['reason']}")
        print(f"{'='*80}")


# Global instance
_scraper_instance = None


def linkedin_scraper(urls: List[str], output_filename: str = "linkedin_scraped_data.json", headless: bool = True, 
                    max_workers: int = 5, batch_size: int = 8, rate_limit_delay: float = 1.0) -> Dict[str, Any]:
    """
    Optimized LinkedIn scraper function with concurrency
    
    Args:
        urls: List of LinkedIn URLs to scrape
        output_filename: Name of output JSON file (default: "linkedin_scraped_data.json")
        headless: Run browser in headless mode (default: True)
        max_workers: Maximum number of concurrent workers (default: 5)
        batch_size: Number of URLs to process in each batch (default: 8)
        rate_limit_delay: Delay between requests in seconds (default: 1.0)
    
    Returns:
        Dict containing scraped data and metadata
    
    Usage:
        from main import linkedin_scraper
        
        urls = [
            "https://www.linkedin.com/in/williamhgates/",
            "https://www.linkedin.com/company/microsoft/"
        ]
        
        results = linkedin_scraper(urls, max_workers=5, batch_size=8, rate_limit_delay=1.0)
    """
    
    global _scraper_instance
    
    if not urls:
        print("❌ No URLs provided")
        return {"error": "No URLs provided"}
    
    try:
        # Create optimized scraper instance
        _scraper_instance = OptimizedLinkedInScraper(
            headless=headless, 
            enable_anti_detection=True,
            use_mongodb=True,
            max_workers=max_workers,
            batch_size=batch_size,
            rate_limit_delay=rate_limit_delay
        )
        
        # Run async scraping
        results = asyncio.run(_scraper_instance.scrape_async(urls, output_filename))
        
        return results
    
    except Exception as e:
        print(f"❌ LinkedIn scraper failed: {e}")
        return {"error": str(e)}


# Alternative class-based approach (if preferred)
class LinkedInScraper:
    """Alternative class-based interface"""
    
    def __init__(self, headless: bool = True):
        self.scraper = LinkedInScraperMain(headless=headless, enable_anti_detection=True)
    
    def scrape(self, urls: List[str], output_filename: str = "linkedin_scraper/linkedin_scraped_data.json") -> Dict[str, Any]:
        """
        Scrape LinkedIn URLs
        
        Usage:
            from main import LinkedInScraper
            
            scraper = LinkedInScraper()
            results = scraper.scrape(urls)
        """
        
        if not urls:
            print("❌ No URLs provided")
            return {"error": "No URLs provided"}
        
        try:
            return asyncio.run(self.scraper.scrape_async(urls, output_filename))
        except Exception as e:
            print(f"❌ LinkedIn scraper failed: {e}")
            return {"error": str(e)}


if __name__ == "__main__":
    # Example usage
    # test_urls = [
    #     "https://www.linkedin.com/in/williamhgates/", #Profile URL Type
        #     "https://www.linkedin.com/legal/user-agreement",
        # "https://in.linkedin.com/in/rishabhmariwala",
        # "https://in.linkedin.com/in/ruchi-aggarwal",
        # "https://www.linkedin.com/posts/mohesh-mohan_a-hackers-travel-diaries-episode-1-hotels-activity-7139268391843921920-DW8u",
        #         "https://www.linkedin.com/posts/aazam-ali-mir-26aab2135_i-recently-travelled-to-belgrade-serbia-activity-7350519219886653440-n0IY",
        # "https://in.linkedin.com/company/odysseytravels",
    #     "https://www.linkedin.com/company/microsoft/", #Company URL Type
    #     "https://www.linkedin.com/newsletters/aiqod-insider-7325820451622940672", #Newsletter URL type
    #     "https://www.linkedin.com/pulse/10-offbeat-destinations-india-corporate…", #Unknown URL type
    #     "https://careers.linkedin.com/", #Unknown URL type
    #     "https://www.linkedin.com/legal/user-agreement", #Unknown URL type
    #     "https://economicgraph.linkedin.com/workforce-data", #Unknown URL type
        #     "https://www.linkedin.com/posts/mehar-labana_the-empowered-coach-retreat-2024-is-here-activity-7255548691091005440-Zcoi", #Post URL type '@type': 'VideoObject'
        # "https://www.linkedin.com/posts/manojsatishkumar_below-is-my-experience-booking-a-trip-to-activity-7090924640289632256-jgSc", #Post URL type @type': 'DiscussionForumPosting'
        # "https://www.linkedin.com/posts/harishbali_ep-5-nusa-penida-island-bali-everything-activity-7200356196912963584-V8mV" #Post URL type '@type': 'DiscussionForumPosting'
    # ]
    # test_urls = [
    #     "https://www.linkedin.com/pulse/just-finished-travelling-around-world-one-year-now-what-guimar%C3%A3es",
    #     "https://si.linkedin.com/posts/mayankgarg01_recently-i-travelled-to-trieste-italy-to-activity-7070635215194349568-3oRg",
    #     "https://careers.linkedin.com/",
    #     "https://www.linkedin.com/company/world-travel-inc",
    #     "https://www.linkedin.com/company/travel-the-world",
    #     "https://www.linkedin.com/in/emma-cleary",
    #     "https://in.linkedin.com/in/mohansundaram",

    # ]
    test_urls = [
    "https://in.linkedin.com/in/ruchi-aggarwal",
    "https://www.linkedin.com/in/emma-cleary",
    "https://in.linkedin.com/in/mohansundaram",
    "https://in.linkedin.com/in/rishabhmariwala"
    ]
    
    print("🚀 Testing Optimized LinkedIn Scraper...")
    print("=" * 80)
    print("Method 1: Optimized function approach")
    print(f"URLs: {len(test_urls)}")
    print(f"Max Workers: 5")
    print(f"Batch Size: 8")
    print("=" * 80)
    
    # Test with optimized parameters
    results = linkedin_scraper(
        test_urls, 
        "linkedin_scraper/test_results_optimized.json", 
        headless=False,
        max_workers=2,
        batch_size=4,
        rate_limit_delay=1.0
    )
    
    # print("\nMethod 2: Class approach")
    # scraper = LinkedInScraper(headless=False)
    # results2 = scraper.scrape(test_urls, "test_results_class.json")
    print("\n" + "=" * 80)
    print("✅ Optimized scraper test completed!")
    print("=" * 80)