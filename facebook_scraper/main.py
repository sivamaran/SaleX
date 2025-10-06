import asyncio
import time
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse

from loguru import logger
from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

# Ensure sys.path is set for relative imports
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from facebook_scraper.browser_manager import BrowserManager
from facebook_scraper.facebook_data_extractor import FacebookDataExtractor
from database.mongodb_manager import get_mongodb_manager


class FacebookScraper:
    """
    Orchestrates the Facebook scraping process, managing browser instances,
    data extraction, and storage.
    """

    def __init__(self, max_concurrent_pages: int = 5, retry_attempts: int = 3, proxy: Optional[Dict[str, str]] = None):
        self.browser_manager = BrowserManager(proxy=proxy)
        self.data_extractor = FacebookDataExtractor()
        self.mongodb_manager = get_mongodb_manager()
        self.max_concurrent_pages = max_concurrent_pages
        self.retry_attempts = retry_attempts
        self.semaphore = asyncio.Semaphore(self.max_concurrent_pages)
        self.scraped_urls_stats = {
            'total_urls': 0,
            'successful_scrapes': 0,
            'failed_scrapes': 0,
            'skipped_urls': 0,
            'profile_count': 0,
            'page_count': 0,
            'group_count': 0,
            'event_count': 0,
            'post_count': 0,
            'other_count': 0,
            'emails_found': 0,
            'phone_numbers_found': 0,
            'websites_found': 0,
            'top_categories': {}, # For pages
            'sample_leads': []
        }
        self.all_processed_data = [] # To store all successfully scraped data for analysis

    async def _process_url(self, url: str):
        """Processes a single URL, including retries and error handling."""
        self.scraped_urls_stats['total_urls'] += 1
        for attempt in range(self.retry_attempts):
            try:
                async with self.semaphore:
                    logger.info(f"Processing URL: {url} (Attempt {attempt + 1}/{self.retry_attempts})")
                    context, adm = await self.browser_manager.get_new_context()
                    page = await context.new_page()
                    
                    extracted_data = await self.data_extractor.extract_facebook_data(page, url, adm)
                    
                    if extracted_data.get('success', True) is False and extracted_data.get('error'):
                        logger.error(f"Failed to extract data for {url}: {extracted_data['error']}")
                        raise Exception(extracted_data['error']) # Raise to trigger retry
                    
                    self.scraped_urls_stats['successful_scrapes'] += 1
                    self.all_processed_data.append(extracted_data) # Store for analysis

                    # Update content type counts
                    url_type = extracted_data.get('url_type', 'unknown')
                    if url_type == 'profile':
                        self.scraped_urls_stats['profile_count'] += 1
                    elif url_type == 'page':
                        self.scraped_urls_stats['page_count'] += 1
                        # Update top categories for pages
                        category = extracted_data.get('extracted_data', {}).get('category') or \
                                   extracted_data.get('meta_data', {}).get('open_graph', {}).get('og:type')
                        if category:
                            self.scraped_urls_stats['top_categories'][category] = self.scraped_urls_stats['top_categories'].get(category, 0) + 1
                    elif url_type == 'group':
                        self.scraped_urls_stats['group_count'] += 1
                    elif url_type == 'event':
                        self.scraped_urls_stats['event_count'] += 1
                    elif url_type == 'post':
                        self.scraped_urls_stats['post_count'] += 1
                    else:
                        self.scraped_urls_stats['other_count'] += 1

                    # Update email, phone, website counts
                    if extracted_data.get('extracted_data', {}).get('emails'):
                        self.scraped_urls_stats['emails_found'] += len(extracted_data['extracted_data']['emails'])
                    if extracted_data.get('extracted_data', {}).get('phone_numbers'):
                        self.scraped_urls_stats['phone_numbers_found'] += len(extracted_data['extracted_data']['phone_numbers'])
                    if extracted_data.get('extracted_data', {}).get('website') or \
                       extracted_data.get('extracted_data', {}).get('og_url'):
                        self.scraped_urls_stats['websites_found'] += 1
                    

                    # Save raw extracted data to facebook_leads collection
                    logger.info(f"💾 Attempting to insert raw Facebook data into facebook_leads collection for {url}...")
                    raw_data_insert_success = self.mongodb_manager.insert_facebook_lead(extracted_data)
                    if raw_data_insert_success:
                        logger.info(f"✅ Raw Facebook data inserted into facebook_leads collection for {url}.")
                    else:
                        logger.warning(f"❌ Failed to insert raw Facebook data into facebook_leads collection for {url} (may already exist).")

                    # Transform and insert into unified_leads collection
                    logger.info(f"💾 Attempting to insert into unified_leads collection for {url}...")
                    
                    unified_stats = self.mongodb_manager.insert_and_transform_to_unified(
                        [extracted_data], 'facebook'
                    )
                    
                    logger.info(f"✅ Unified insertion result for {url}: Success: {unified_stats['success_count']}, Updated: {unified_stats['updated_count']}, Failed: {unified_stats['failure_count']}")

                    # Add to sample leads if less than 5
                    if len(self.scraped_urls_stats['sample_leads']) < 5:
                        self.scraped_urls_stats['sample_leads'].append(extracted_data)

                await page.close()
                await context.close()
                return # Success, exit retry loop
            
            except PlaywrightTimeoutError:
                logger.warning(f"Timeout while processing {url} (Attempt {attempt + 1}/{self.retry_attempts}). Retrying...")
            except Exception as e:
                logger.error(f"Error processing {url} (Attempt {attempt + 1}/{self.retry_attempts}): {e}")
            
            await asyncio.sleep(2 ** attempt) # Exponential backoff
        
        self.scraped_urls_stats['failed_scrapes'] += 1
        logger.error(f"Failed to process {url} after {self.retry_attempts} attempts. Skipping.")

    async def scrape_urls(self, urls: List[str]):
        """Initiates the scraping of a list of URLs."""
        logger.info(f"Starting Facebook scraping for {len(urls)} URLs...")
        
        # Create tasks for each URL
        tasks = [self._process_url(url) for url in urls]
        await asyncio.gather(*tasks)
        
        await self.browser_manager.close_browser()
        logger.info("Facebook scraping completed.")
        self._generate_analysis_report()

    def _generate_analysis_report(self):
        """Generates and prints the analysis report."""
        total_scraped = self.scraped_urls_stats['successful_scrapes']
        
        logger.info("\n" + "=" * 50)
        logger.info("Facebook Scraper Analysis Report")
        logger.info("=" * 50)
        logger.info(f"Total URLs processed: {self.scraped_urls_stats['total_urls']}")
        logger.info(f"Successful scrapes: {total_scraped}")
        logger.info(f"Failed scrapes: {self.scraped_urls_stats['failed_scrapes']}")
        logger.info(f"Skipped URLs: {self.scraped_urls_stats['skipped_urls']}")
        logger.info("-" * 50)
        
        if total_scraped > 0:
            logger.info("Content Type Breakdown:")
            logger.info(f"  Profiles: {self.scraped_urls_stats['profile_count']}")
            logger.info(f"  Pages: {self.scraped_urls_stats['page_count']}")
            logger.info(f"  Groups: {self.scraped_urls_stats['group_count']}")
            logger.info(f"  Events: {self.scraped_urls_stats['event_count']}")
            logger.info(f"  Posts: {self.scraped_urls_stats['post_count']}")
            logger.info(f"  Others: {self.scraped_urls_stats['other_count']}")
            logger.info("-" * 50)

            total_leads_with_contact = self.scraped_urls_stats['emails_found'] + self.scraped_urls_stats['phone_numbers_found'] + self.scraped_urls_stats['websites_found']
            
            logger.info(f"Leads with Emails: {self.scraped_urls_stats['emails_found']} ({ (self.scraped_urls_stats['emails_found'] / total_scraped * 100):.2f}%)")
            logger.info(f"Leads with Phone Numbers: {self.scraped_urls_stats['phone_numbers_found']} ({ (self.scraped_urls_stats['phone_numbers_found'] / total_scraped * 100):.2f}%)")
            logger.info(f"Leads with Websites: {self.scraped_urls_stats['websites_found']} ({ (self.scraped_urls_stats['websites_found'] / total_scraped * 100):.2f}%)")
            logger.info("-" * 50)

            if self.scraped_urls_stats['top_categories']:
                logger.info("Top Categories (Pages):")
                sorted_categories = sorted(self.scraped_urls_stats['top_categories'].items(), key=lambda item: item[1], reverse=True)
                for category, count in sorted_categories:
                    logger.info(f"  - {category}: {count}")
                logger.info("-" * 50)

            if self.scraped_urls_stats['sample_leads']:
                logger.info("Sample Preview of First 5 Leads:")
                for i, lead in enumerate(self.scraped_urls_stats['sample_leads']):
                    logger.info(f"  Lead {i+1}:")
                    logger.info(f"    URL: {lead.get('url')}")
                    logger.info(f"    Type: {lead.get('url_type')}")
                    logger.info(f"    Name: {lead.get('extracted_data', {}).get('name') or lead.get('extracted_data', {}).get('og_title')}")
                    if lead.get('extracted_data', {}).get('emails'):
                        logger.info(f"    Emails: {lead.get('extracted_data', {}).get('emails')}")
                    if lead.get('extracted_data', {}).get('phone_numbers'):
                        logger.info(f"    Phone: {lead.get('extracted_data', {}).get('phone_numbers')}")
                    logger.info("-" * 10)
        else:
            logger.info("No successful scrapes to report analysis.")
        logger.info("=" * 50)


async def main():
    urls = [
        "https://www.facebook.com/AirtelIndia/",
        "https://www.facebook.com/zuck",
        "https://www.facebook.com/groups/facebookdevelopers/",
        "https://www.facebook.com/events/1234567890/", # Example event, might not exist
        "https://www.facebook.com/facebook",
        "https://www.facebook.com/nasa",
        "https://www.facebook.com/CocaCola/",
        "https://www.facebook.com/Meta/"
    ]
    scraper = FacebookScraper(max_concurrent_pages=3, retry_attempts=3)
    await scraper.scrape_urls(urls)

if __name__ == "__main__":
    asyncio.run(main())
