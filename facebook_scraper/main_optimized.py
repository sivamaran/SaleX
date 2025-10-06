"""
Facebook Scraper - Optimized Main Entry Point
High-performance Facebook scraper with concurrency, browser pooling, and batch processing

Key Optimizations:
1. Async concurrency with worker pool pattern
2. Browser context pooling (3-5 contexts)
3. Batch processing (5-10 concurrent URLs)
4. Resource optimization and cleanup
5. Fail-fast strategy with graceful degradation
6. Coordinated rate limiting

Usage:
    from facebook_scraper.main_optimized import OptimizedFacebookScraper

    scraper = OptimizedFacebookScraper()
    result = await scraper.scrape(urls)

    # Or use the convenience function
    result = await scrape_facebook_urls_optimized(urls)
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

from facebook_scraper.facebook_data_extractor import FacebookDataExtractor
from facebook_scraper.browser_manager import BrowserManager
from web_scraper.utils.anti_detection import AntiDetectionManager
# Removed direct MongoDB dependency from scraper. Orchestrator will handle DB.


@dataclass
class FacebookScrapingConfig:
    """Configuration for optimized Facebook scraping with enhanced network resilience"""
    max_workers: int = 3  # Reduced for better stability
    batch_size: int = 5   # Reduced for better stability
    context_pool_size: int = 3  # Reduced for better stability
    context_reuse_limit: int = 15  # Reduced for better stability
    rate_limit_delay: float = 2.0  # Facebook is more restrictive


class OptimizedFacebookScraper:
    """
    Optimized Facebook scraper with concurrency, pooling, and batch processing
    """

    def __init__(self,
                 headless: bool = True,
                 enable_anti_detection: bool = True,
                 output_file: Optional[str] = None,
                 use_mongodb: bool = True,
                 config: Optional[FacebookScrapingConfig] = None,
                 icp_identifier: str = 'default'):
        """
        Initialize the optimized Facebook scraper

        Args:
            headless: Run browser in headless mode (default: True)
            enable_anti_detection: Enable anti-detection features (default: True)
            output_file: Optional file path to save results (default: None)
            use_mongodb: Whether to save data to MongoDB (default: True)
            config: Custom scraping configuration (default: None)
            icp_identifier: ICP identifier for tracking which ICP this data belongs to (default: 'default')
        """
        self.headless = headless
        self.enable_anti_detection = enable_anti_detection
        self.output_file = output_file
        self.use_mongodb = use_mongodb
        self.config = config or FacebookScrapingConfig()
        self.icp_identifier = icp_identifier

        # Initialize components
        self.context_pool = None
        self.worker_semaphore = asyncio.Semaphore(self.config.max_workers)
        self.rate_limiter = asyncio.Semaphore(2)  # Facebook is more restrictive

        # Scraper no longer initializes MongoDB. DB operations are orchestrated centrally.

    async def scrape(self, urls: List[str]) -> Dict[str, Any]:
        """
        Scrape data from a list of Facebook URLs with optimizations

        Args:
            urls: List of Facebook URLs to scrape

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

        print(f"🚀 Starting optimized Facebook scraper...")
        print(f"   URLs to process: {len(urls)}")
        print(f"   Workers: {self.config.max_workers}, Batch size: {self.config.batch_size}")

        start_time = time.time()

        try:
            # Initialize browser manager
            self.browser_manager = BrowserManager()

            # Process URLs in batches
            all_results = []
            all_errors = []

            for i in range(0, len(urls), self.config.batch_size):
                batch_urls = urls[i:i + self.config.batch_size]
                print(f"📦 Processing batch {i//self.config.batch_size + 1}/{(len(urls) + self.config.batch_size - 1)//self.config.batch_size} ({len(batch_urls)} URLs)")

                # Process batch with concurrency
                batch_results, batch_errors = await self._process_batch(batch_urls)
                all_results.extend(batch_results)
                all_errors.extend(batch_errors)

                # Rate limiting between batches
                if i + self.config.batch_size < len(urls):
                    await asyncio.sleep(self.config.rate_limit_delay)

            # Transform results to unified format
            unified_leads = []
            for result in all_results:
                unified = self._transform_facebook_to_unified(result, self.icp_identifier)
                if unified:
                    unified_leads.append(unified)

            # Calculate performance metrics
            total_time = time.time() - start_time
            successful_scrapes = len(all_results)
            total_urls = len(urls)
            success_rate = (successful_scrapes / total_urls * 100) if total_urls > 0 else 0
            throughput = successful_scrapes / total_time if total_time > 0 else 0

            summary = {
                'total_urls': total_urls,
                'successful_scrapes': successful_scrapes,
                'failed_scrapes': len(all_errors),
                'success_rate': success_rate,
                'total_time_seconds': total_time,
                'performance_metrics': {
                    'throughput_per_second': throughput,
                    'max_workers': self.config.max_workers,
                    'batch_size': self.config.batch_size,
                    'contexts_used': self.config.context_pool_size,
                    'rate_limit_delay': self.config.rate_limit_delay
                }
            }

            result = {
                'success': True,
                'data': all_results,
                'unified_leads': unified_leads,
                'summary': summary,
                'errors': all_errors
            }

            # Save to file if specified
            if self.output_file:
                with open(self.output_file, 'w', encoding='utf-8') as f:
                    json.dump(result, f, indent=2, ensure_ascii=False)
                print(f"💾 Results saved to {self.output_file}")

            print(f"✅ Facebook scraping completed: {successful_scrapes}/{total_urls} URLs ({success_rate:.1f}%)")
            print(f"   - Total time: {total_time:.2f} seconds")
            print(f"   - Throughput: {throughput:.2f} URLs/second")

            return result

        except Exception as e:
            error_msg = f"Facebook scraping failed: {str(e)}"
            print(f"❌ {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'data': [],
                'summary': {},
                'errors': [error_msg]
            }
        finally:
            # Cleanup
            if self.context_pool:
                await self.context_pool.cleanup()
                self.context_pool = None

            # Force garbage collection
            gc.collect()

    async def _process_batch(self, urls: List[str]) -> Tuple[List[Dict[str, Any]], List[str]]:
        """
        Process a batch of URLs concurrently

        Args:
            urls: List of URLs to process

        Returns:
            Tuple of (successful results, errors)
        """
        tasks = []
        for url in urls:
            task = asyncio.create_task(self._scrape_single_url(url))
            tasks.append(task)

        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)

        successful_results = []
        errors = []

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                error_msg = f"URL {urls[i]} failed: {str(result)}"
                errors.append(error_msg)
                print(f"❌ {error_msg}")
            elif result:
                successful_results.append(result)
            else:
                error_msg = f"URL {urls[i]} returned no data"
                errors.append(error_msg)

        return successful_results, errors

    async def _scrape_single_url(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Scrape a single Facebook URL

        Args:
            url: Facebook URL to scrape

        Returns:
            Scraped data or None if failed
        """
        async with self.worker_semaphore:
            async with self.rate_limiter:
                try:
                    # Get new context from browser manager
                    context, adm = await self.browser_manager.get_new_context()

                    try:
                        # Create data extractor
                        extractor = FacebookDataExtractor()

                        # Get page from context
                        page = await context.new_page()

                        try:
                            # Extract data
                            result = await extractor.extract_facebook_data(page, url, adm)

                            if result and result.get('success', True) and result.get('extracted_data'):
                                # Add metadata
                                result['scraped_at'] = time.time()
                                result['url'] = url
                                return result
                            else:
                                print(f"⚠️ No data extracted from {url}")
                                return None

                        finally:
                            await page.close()

                    finally:
                        # Close context
                        await context.close()

                except Exception as e:
                    print(f"❌ Error scraping {url}: {str(e)}")
                    return None

    def _transform_facebook_to_unified(self, facebook_data: Dict[str, Any], icp_identifier: str = 'default') -> Optional[Dict[str, Any]]:
        """Transform Facebook data to unified schema (local to scraper)"""
        try:
            url_type = facebook_data.get('url_type', 'unknown')
            extracted_data = facebook_data.get('extracted_data', {})

            if url_type == 'unknown':
                return None

            base = {
                "url": facebook_data.get('url', ""),
                "platform": "facebook",
                "content_type": url_type,
                "source": "facebook-scraper",
                "icp_identifier": icp_identifier,
                "metadata": {
                    "scraped_at": facebook_data.get('scraped_at') or time.time(),
                    "data_quality_score": ""
                },
                # Common optional fields kept for unified schema
                "industry": None,
                "revenue": None,
                "lead_category": None,
                "lead_sub_category": None,
                "company_name": extracted_data.get('name', ''),
                "company_type": None,
                "decision_makers": None,
                "bdr": "AKG",
                "product_interests": None,
                "timeline": None,
                "interest_level": None
            }

            if url_type == 'post':
                # Handle Facebook posts (often from pages/businesses)
                base.update({
                    "profile": {
                        "username": extracted_data.get('username', ''),
                        "full_name": extracted_data.get('og_title', '').replace(' | Facebook', ''),
                        "bio": extracted_data.get('og_description', ''),
                        "location": "",
                        "job_title": "Post",
                        "employee_count": ""
                    },
                    "contact": {
                        "emails": extracted_data.get('emails', []),
                        "phone_numbers": "",
                        "address": "",
                        "websites": [extracted_data.get('og_url')] if extracted_data.get('og_url') else [],
                        "social_media_handles": {
                            "instagram": "",
                            "twitter": "",
                            "facebook": extracted_data.get('username', ''),
                            "linkedin": "",
                            "youtube": "",
                            "tiktok": "",
                            "other": []
                        },
                        "bio_links": []
                    },
                    "content": {
                        "caption": extracted_data.get('og_description', ''),
                        "upload_date": "",
                        "channel_name": extracted_data.get('og_title', '').replace(' | Facebook', ''),
                        "author_name": extracted_data.get('og_title', '').replace(' | Facebook', '')
                    }
                })
                return base

            elif url_type == 'page':
                base.update({
                    "profile": {
                        "username": extracted_data.get('username', ''),
                        "full_name": extracted_data.get('name', ''),
                        "bio": extracted_data.get('description', ''),
                        "location": extracted_data.get('location', ''),
                        "job_title": extracted_data.get('category', ''),
                        "employee_count": ""
                    },
                    "contact": {
                        "emails": extracted_data.get('emails', []),
                        "phone_numbers": "",
                        "address": extracted_data.get('address', ''),
                        "websites": [extracted_data.get('website')] if extracted_data.get('website') else [],
                        "social_media_handles": {
                            "instagram": "",
                            "twitter": "",
                            "facebook": extracted_data.get('username', ''),
                            "linkedin": "",
                            "youtube": "",
                            "tiktok": "",
                            "other": []
                        },
                        "bio_links": []
                    },
                    "content": {
                        "caption": "",
                        "upload_date": "",
                        "channel_name": extracted_data.get('name', ''),
                        "author_name": extracted_data.get('name', '')
                    }
                })
                return base

            elif url_type == 'profile':
                base.update({
                    "profile": {
                        "username": extracted_data.get('username', ''),
                        "full_name": extracted_data.get('name', ''),
                        "bio": extracted_data.get('bio', ''),
                        "location": extracted_data.get('location', ''),
                        "job_title": extracted_data.get('job_title', ''),
                        "employee_count": ""
                    },
                    "contact": {
                        "emails": extracted_data.get('emails', []),
                        "phone_numbers": "",
                        "address": extracted_data.get('address', ''),
                        "websites": [extracted_data.get('website')] if extracted_data.get('website') else [],
                        "social_media_handles": {
                            "instagram": "",
                            "twitter": "",
                            "facebook": extracted_data.get('username', ''),
                            "linkedin": "",
                            "youtube": "",
                            "tiktok": "",
                            "other": []
                        },
                        "bio_links": []
                    },
                    "content": {
                        "caption": "",
                        "upload_date": "",
                        "channel_name": extracted_data.get('name', ''),
                        "author_name": extracted_data.get('name', '')
                    }
                })
                return base

            elif url_type == 'group':
                base.update({
                    "profile": {
                        "username": extracted_data.get('username', ''),
                        "full_name": extracted_data.get('name', ''),
                        "bio": extracted_data.get('description', ''),
                        "location": extracted_data.get('location', ''),
                        "job_title": "Group",
                        "employee_count": extracted_data.get('member_count', '')
                    },
                    "contact": {
                        "emails": extracted_data.get('emails', []),
                        "phone_numbers": "",
                        "address": "",
                        "websites": [],
                        "social_media_handles": {
                            "instagram": "",
                            "twitter": "",
                            "facebook": extracted_data.get('username', ''),
                            "linkedin": "",
                            "youtube": "",
                            "tiktok": "",
                            "other": []
                        },
                        "bio_links": []
                    },
                    "content": {
                        "caption": extracted_data.get('description', ''),
                        "upload_date": "",
                        "channel_name": extracted_data.get('name', ''),
                        "author_name": extracted_data.get('name', '')
                    }
                })
                return base

            elif url_type == 'event':
                base.update({
                    "profile": {
                        "username": extracted_data.get('username', ''),
                        "full_name": extracted_data.get('name', ''),
                        "bio": extracted_data.get('description', ''),
                        "location": extracted_data.get('location', ''),
                        "job_title": "Event",
                        "employee_count": ""
                    },
                    "contact": {
                        "emails": extracted_data.get('emails', []),
                        "phone_numbers": "",
                        "address": extracted_data.get('address', ''),
                        "websites": [],
                        "social_media_handles": {
                            "instagram": "",
                            "twitter": "",
                            "facebook": extracted_data.get('username', ''),
                            "linkedin": "",
                            "youtube": "",
                            "tiktok": "",
                            "other": []
                        },
                        "bio_links": []
                    },
                    "content": {
                        "caption": extracted_data.get('description', ''),
                        "upload_date": extracted_data.get('start_time', ''),
                        "channel_name": extracted_data.get('name', ''),
                        "author_name": extracted_data.get('organizer', '')
                    }
                })
                return base

            return None

        except Exception as e:
            print(f"❌ Error transforming Facebook data: {str(e)}")
            return None


# Convenience function for easy usage
async def scrape_facebook_urls_optimized(urls: List[str],
                                        headless: bool = True,
                                        enable_anti_detection: bool = True,
                                        output_file: Optional[str] = None,
                                        config: Optional[FacebookScrapingConfig] = None,
                                        icp_identifier: str = 'default') -> Dict[str, Any]:
    """
    Convenience function to scrape Facebook URLs with optimizations

    Args:
        urls: List of Facebook URLs to scrape
        headless: Run browser in headless mode
        enable_anti_detection: Enable anti-detection features
        output_file: Optional output file path
        config: Custom scraping configuration
        icp_identifier: ICP identifier

    Returns:
        Dictionary containing scraping results
    """
    scraper = OptimizedFacebookScraper(
        headless=headless,
        enable_anti_detection=enable_anti_detection,
        output_file=output_file,
        config=config,
        icp_identifier=icp_identifier
    )

    return await scraper.scrape(urls)