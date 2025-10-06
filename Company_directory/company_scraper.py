from __future__ import annotations

import asyncio
import time
import random
import re
import json
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any, Tuple
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from loguru import logger
from playwright.async_api import Page, BrowserContext, Browser, async_playwright

from web_scraper.data_models.models import PageContent
from web_scraper.utils.anti_detection import AntiDetectionManager, create_stealth_browser_context, execute_human_behavior
from web_scraper.ai_integration.ai import extract_client_info_from_sections, _ai_generate_json # Import the AI integration


def extract_emails(lead_data: Dict[str, Any]) -> List[str]:
    """Extract email addresses from lead data."""
    emails = []

    # Check direct email field
    if lead_data.get('email'):
        emails.append(lead_data['email'])

    # Extract from notes or other text fields
    text_fields = ['notes', 'bio', 'description']
    for field in text_fields:
        if lead_data.get(field):
            # Simple email regex
            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            found_emails = re.findall(email_pattern, lead_data[field])
            emails.extend(found_emails)

    return list(set(emails))  # Remove duplicates

def extract_phones(lead_data: Dict[str, Any]) -> List[str]:
    """Extract phone numbers from lead data."""
    phones = []

    # Check direct phone field
    if lead_data.get('phone'):
        phones.append(lead_data['phone'])

    # Extract from notes or other text fields
    text_fields = ['notes', 'bio', 'description', 'address']
    for field in text_fields:
        if lead_data.get(field):
            # Simple phone regex (basic patterns)
            phone_patterns = [
                r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',  # US format
                r'\+\d{1,3}[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,4}',  # International
                r'\(\d{3}\)\s*\d{3}[-.]?\d{4}',  # (123) 456-7890
            ]
            for pattern in phone_patterns:
                found_phones = re.findall(pattern, lead_data[field])
                phones.extend(found_phones)

    return list(set(phones))  # Remove duplicates

def get_websites(lead_data: Dict[str, Any]) -> List[str]:
    """Extract websites from lead data."""
    websites = []

    # Check for website fields
    website_fields = ['website', 'url', 'homepage']
    for field in website_fields:
        if lead_data.get(field):
            websites.append(lead_data[field])

    # Extract from notes or other text fields
    text_fields = ['notes', 'bio', 'description']
    for field in text_fields:
        if lead_data.get(field):
            # URL regex
            url_pattern = r'https?://(?:[-\w.])+(?:[:\d]+)?(?:/(?:[\w/_.])*(?:\?(?:[\w&=%.])*)?(?:#(?:\w*))*)?'
            found_urls = re.findall(url_pattern, lead_data[field])
            websites.extend(found_urls)

    return list(set(websites))  # Remove duplicates

def extract_social_media(lead_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract social media handles from lead data."""
    social_media = {
        'instagram': None,
        'twitter': None,
        'facebook': None,
        'linkedin': None,
        'youtube': None,
        'tiktok': None
    }

    text_fields = ['notes', 'bio', 'description', 'profile_url']
    for field in text_fields:
        if lead_data.get(field):
            text = lead_data[field].lower()

            # Check for social media patterns
            if 'instagram.com/' in text:
                handle = re.search(r'instagram\.com/([^\s/]+)', text)
                if handle:
                    social_media['instagram'] = handle.group(1)
            if 'twitter.com/' in text or 'x.com/' in text:
                handle = re.search(r'(?:twitter|x)\.com/([^\s/]+)', text)
                if handle:
                    social_media['twitter'] = handle.group(1)
            if 'facebook.com/' in text:
                handle = re.search(r'facebook\.com/([^\s/]+)', text)
                if handle:
                    social_media['facebook'] = handle.group(1)
            if 'linkedin.com/' in text:
                handle = re.search(r'linkedin\.com/(?:in|company)/([^\s/]+)', text)
                if handle:
                    social_media['linkedin'] = handle.group(1)
            if 'youtube.com/' in text:
                handle = re.search(r'youtube\.com/(?:user|c|channel)/([^\s/]+)', text)
                if handle:
                    social_media['youtube'] = handle.group(1)
            if 'tiktok.com/' in text:
                handle = re.search(r'tiktok\.com/@([^\s/]+)', text)
                if handle:
                    social_media['tiktok'] = handle.group(1)

    return social_media

def transform_to_unified_format(lead_data: Dict[str, Any], source_url: str = "", page_type: str = "") -> Dict[str, Any]:
    """Transform extracted lead data into unified format."""

    # Extract social media handles
    social_media = extract_social_media(lead_data)

    # Get other social links (anything not in the main platforms)
    other_social_links = []
    websites = get_websites(lead_data)
    for website in websites:
        domain = urlparse(website).netloc.lower()
        if not any(platform in domain for platform in ['instagram', 'twitter', 'facebook', 'linkedin', 'youtube', 'tiktok']):
            other_social_links.append(website)

    # Determine lead category and sub-category
    lead_category = lead_data.get('lead_category', 'B2B')
    lead_sub_category = lead_data.get('lead_sub_category', 'Company')

    # Extract contact information with enhanced data priority
    emails_data = lead_data.get('email', [])
    phones_data = lead_data.get('phone', [])
    websites_data = lead_data.get('websites', [])
    
    logger.info(f"DEBUG: Enhanced contact data - emails: {len(emails_data) if isinstance(emails_data, list) else 'not_list'}, phones: {len(phones_data) if isinstance(phones_data, list) else 'not_list'}, websites: {len(websites_data) if isinstance(websites_data, list) else 'not_list'}")
    
    # Create unified data structure
    unified_data = {
        "url": source_url or lead_data.get('source_url', ''),
        "platform": "web",
        "content_type": page_type or "",
        "source": "web-scraper",
        "profile": {
            "username": "",
            "full_name": lead_data.get('name', '') or lead_data.get('company_name', ''),
            "bio": lead_data.get('bio', ''),
            "location": lead_data.get('location', ''),
            "job_title": "",
            "employee_count": lead_data.get('employee_count', '')
        },
        "contact": {
            "emails": emails_data if isinstance(emails_data, list) else extract_emails(lead_data),
            "phone_numbers": phones_data if isinstance(phones_data, list) else extract_phones(lead_data),
            "address": lead_data.get('address', ''),
            "websites": websites_data if isinstance(websites_data, list) and websites_data else get_websites(lead_data),
            "social_media_handles": lead_data.get('social_media', {}) or {
                "instagram": social_media.get('instagram'),
                "twitter": social_media.get('twitter'),
                "facebook": social_media.get('facebook'),
                "linkedin": social_media.get('linkedin'),
                "youtube": social_media.get('youtube'),
                "tiktok": social_media.get('tiktok'),
                "other": other_social_links
            },
            "bio_links": []
        },
        "content": {
            "caption": "",
            "upload_date": "",
            "channel_name": "",
            "author_name": ""
        },
        "metadata": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "data_quality_score": "0.45"
        },
        "industry": lead_data.get('industry', ''),
        "revenue": lead_data.get('revenue', ''),
        "lead_category": lead_category,
        "lead_sub_category": lead_sub_category,
        "company_name": lead_data.get('company_name', ''),
        "company_type": lead_data.get('company_type', ''),
        "decision_makers": lead_data.get('name', ''),
        "bdr": "AKG",
        "product_interests": None,
        "timeline": None,
        "interest_level": None
    }

    return unified_data


def _filter_invalid_contacts(contact_data: Dict[str, Any]) -> Dict[str, Any]:
    """Filter out invalid emails and phone numbers from contact data."""
    import re

    def _is_valid_email(email: str) -> bool:
        """Validate email format and filter out false positives."""
        if not email or len(email) < 5 or len(email) > 100:
            return False

        # Basic email pattern
        pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$'
        if not re.match(pattern, email):
            return False

        # Filter out obvious false positives
        email_lower = email.lower()

        # Exclude file extensions (images, CSS, etc.)
        if any(ext in email_lower for ext in ['.png', '.jpg', '.jpeg', '.gif', '.css', '.js', '.ico', '.svg', '.webp']):
            return False

        # Exclude CSS-like patterns (numbers at start, @2x, etc.)
        if re.search(r'^\d|@2x|@3x|logo|icon|image|img|photo|pic|asset', email_lower):
            return False

        # Exclude emails with spaces or unusual characters
        if ' ' in email or any(char in email for char in ['<', '>', '{', '}', '[', ']', '|', '\\', '^']):
            return False

        # Exclude emails that look like timestamps or IDs
        local_part = email.split('@')[0]
        if re.match(r'^\d{10,}$', local_part):  # Long numeric local parts
            return False

        # Exclude emails with suspicious patterns
        if re.search(r'\d{8,}', email):  # 8+ consecutive digits
            return False

        # Exclude emails that are clearly not business emails
        suspicious_domains = ['example.com', 'test.com', 'sample.com', 'domain.com', 'email.com']
        domain = email.split('@')[1].lower()
        if domain in suspicious_domains:
            return False

        return True

    def _is_valid_phone(phone: str) -> bool:
        """Validate phone number and filter out false positives."""
        if not phone:
            return False

        # Remove common formatting but keep some structure
        cleaned = re.sub(r'[^\d+\-\(\)\.\s]', '', phone)

        # Filter out CSS measurements (contain dots and spaces)
        if '.' in cleaned and ' ' in cleaned:
            return False

        # Filter out CSS-like patterns with dots and dashes
        if re.search(r'\d+\.\d+-\d+', phone):  # patterns like "7.09-10.36"
            return False

        # Filter out patterns that look like CSS dimensions
        if re.search(r'\d+\s+\d+\s+\d+', phone):  # multiple numbers separated by spaces
            return False

        # Remove all non-digits for length check
        digits_only = re.sub(r'[^\d]', '', phone)

        # Should have 7-15 digits (reasonable phone number range)
        if not (7 <= len(digits_only) <= 15):
            return False

        # Filter out timestamps (13-digit numbers starting with 1, like Unix timestamps)
        if len(digits_only) == 13 and digits_only.startswith('1'):
            return False

        # Filter out obvious timestamps (10-digit numbers that look like Unix timestamps)
        if len(digits_only) == 10 and digits_only.startswith(('1', '2')):
            # Additional check: if it looks like a timestamp (reasonable date range)
            try:
                timestamp = int(digits_only)
                # Unix timestamps from 2000-2050 are roughly 946684800 to 2524608000
                if 946684800 <= timestamp <= 2524608000:
                    return False
            except:
                pass

        # Should not be all the same digit (like CSS measurements)
        if len(set(digits_only)) == 1 and len(digits_only) > 3:
            return False

        # Should not be simple sequential patterns
        if len(digits_only) >= 6:
            # Check if it's a simple pattern like 123456, 987654, etc.
            for i in range(len(digits_only) - 2):
                if (int(digits_only[i+1]) == int(digits_only[i]) + 1 and
                    int(digits_only[i+2]) == int(digits_only[i+1]) + 1):
                    # Allow some sequential but not long sequences
                    if len(digits_only) >= 8:
                        return False

        # Should have some formatting (not just digits)
        if len(phone) == len(digits_only) and len(digits_only) > 10:
            # Long numbers without formatting are suspicious
            return False

        # Filter out numbers that start with 0 (unless international)
        if not phone.startswith('+') and digits_only.startswith('0') and len(digits_only) > 7:
            return False

        # Filter out numbers that contain dots in suspicious ways
        if re.search(r'\d+\.\d+', phone) and not phone.startswith('+'):
            # Allow international numbers like +1.123.456.7890 but not CSS-like
            return False

        return True

    filtered_data = contact_data.copy()

    # Filter emails
    if 'email' in filtered_data:
        if isinstance(filtered_data['email'], list):
            filtered_data['email'] = [email for email in filtered_data['email'] if _is_valid_email(email)]
        elif isinstance(filtered_data['email'], str):
            if not _is_valid_email(filtered_data['email']):
                filtered_data['email'] = []

    # Filter phones
    if 'phone' in filtered_data:
        if isinstance(filtered_data['phone'], list):
            filtered_data['phone'] = [phone for phone in filtered_data['phone'] if _is_valid_phone(phone)]
        elif isinstance(filtered_data['phone'], str):
            if not _is_valid_phone(filtered_data['phone']):
                filtered_data['phone'] = []

    return filtered_data


async def enhance_contact_details(page, contact_data: Dict[str, Any]) -> Dict[str, Any]:
    """Enhanced contact detail extraction using advanced techniques."""
    try:
        # Import the advanced contact extraction strategy
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.dirname(__file__)))
        from web_scraper.utils.contact_extraction import ContactExtractionStrategy
        
        # Create extraction strategy instance
        extractor = ContactExtractionStrategy(page)
        
        # Apply comprehensive contact extraction
        enhanced_data = await extractor.extract_all_contact_methods(contact_data)
        
        # Also try the original approach for additional coverage
        contact_selectors = [
            'a[href*="contact"]', 'button:has-text("contact")', 
            'a:has-text("Contact")', 'a:has-text("Get Quote")',
            '.contact-btn', '.contact-link', '.get-quote',
            'a[href^="mailto:"]', 'a[href^="tel:"]',
            'button:has-text("Call")', 'button:has-text("Email")',
            '.phone-number', '.email-address', '.contact-info',
            '[data-phone]', '[data-email]', '[data-contact]'
        ]
        
        # Try clicking contact buttons to reveal hidden contact info
        for selector in contact_selectors:
            try:
                contact_elements = await page.query_selector_all(selector)
                for element in contact_elements[:3]:  # Limit to first 3 to avoid too many clicks
                    try:
                        # Get any direct contact info from the element
                        text_content = await element.text_content()
                        if text_content:
                            # Extract emails from text
                            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
                            found_emails = re.findall(email_pattern, text_content)
                            if found_emails:
                                if 'email' not in enhanced_data:
                                    enhanced_data['email'] = []
                                elif isinstance(enhanced_data['email'], str):
                                    enhanced_data['email'] = [enhanced_data['email']]
                                enhanced_data['email'].extend(found_emails)
                            
                            # Extract phones from text
                            phone_patterns = [
                                r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',  # US format
                                r'\+\d{1,3}[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,4}',  # International
                                r'\(\d{3}\)\s*\d{3}[-.]?\d{4}',  # (123) 456-7890
                            ]
                            for pattern in phone_patterns:
                                found_phones = re.findall(pattern, text_content)
                                if found_phones:
                                    if 'phone' not in enhanced_data:
                                        enhanced_data['phone'] = []
                                    elif isinstance(enhanced_data['phone'], str):
                                        enhanced_data['phone'] = [enhanced_data['phone']]
                                    enhanced_data['phone'].extend(found_phones)

                        # Try clicking for modal/popup content
                        href = await element.get_attribute('href')
                        if href and ('mailto:' in href or 'tel:' in href):
                            if 'mailto:' in href:
                                email = href.replace('mailto:', '').split('?')[0]
                                if 'email' not in enhanced_data:
                                    enhanced_data['email'] = []
                                elif isinstance(enhanced_data['email'], str):
                                    enhanced_data['email'] = [enhanced_data['email']]
                                enhanced_data['email'].append(email)
                            elif 'tel:' in href:
                                phone = href.replace('tel:', '').replace('+', '').replace('-', '').replace(' ', '')
                                if 'phone' not in enhanced_data:
                                    enhanced_data['phone'] = []
                                elif isinstance(enhanced_data['phone'], str):
                                    enhanced_data['phone'] = [enhanced_data['phone']]
                                enhanced_data['phone'].append(phone)
                                
                    except Exception:
                        continue
                        
            except Exception:
                continue
        
        # Deduplicate contact info
        if 'email' in enhanced_data and isinstance(enhanced_data['email'], list):
            enhanced_data['email'] = list(set(enhanced_data['email']))
        if 'phone' in enhanced_data and isinstance(enhanced_data['phone'], list):
            enhanced_data['phone'] = list(set(enhanced_data['phone']))
            
        # Filter out invalid contacts
        enhanced_data = _filter_invalid_contacts(enhanced_data)
            
        logger.info(f"Enhanced contact extraction found: {len(enhanced_data.get('email', []))} emails, {len(enhanced_data.get('phone', []))} phones")
        return enhanced_data
        
    except Exception as e:
        logger.warning(f"Error in enhanced contact extraction: {e}")
        return contact_data


class UniversalScraper:
    def __init__(self, url: Optional[str] = None, search_query: Optional[str] = None, max_pages: int = 5,
                 base_url: Optional[str] = None, company_name: Optional[str] = None,
                 search_input_selector: Optional[str] = None, search_button_selector: Optional[str] = None,
                 next_page_selector: Optional[str] = None, max_pages_to_scrape: Optional[int] = None):
        # Handle both old and new API parameters
        if url is not None:
            # Old API
            self.url = url
            self.search_query = search_query
            self.max_pages = max_pages
            self.base_url = None
            self.company_name = search_query
            self.search_input_selector = None
            self.search_button_selector = None
            self.next_page_selector = None
            self.max_pages_to_scrape = max_pages
        else:
            # New API
            self.url = base_url
            self.search_query = company_name
            self.max_pages = max_pages_to_scrape or 5
            self.base_url = base_url
            self.company_name = company_name
            self.search_input_selector = search_input_selector
            self.search_button_selector = search_button_selector
            self.next_page_selector = next_page_selector
            self.max_pages_to_scrape = max_pages_to_scrape

        # Comprehensive selector arrays for automatic detection
        self.search_input_selectors = [
            'input[type="search"]',
            'input[name="q"]',
            'input[name="query"]',
            'input[name="search"]',
            'input[name="s"]',
            'input[placeholder*="search" i]',
            'input[placeholder*="find" i]',
            'input[placeholder*="look" i]',
            '.search-input',
            '#search-input',
            '.search-box input',
            '.search-form input',
            'input.search-field',
            'input.query',
            'input.search-query',
            'input[name*="search"]',
            'input[id*="search"]',
            'input[class*="search"]'
        ]

        self.search_button_selectors = [
            'button[type="submit"]',
            'input[type="submit"]',
            'button.search-btn',
            '.search-button',
            '#search-btn',
            'button:has-text("Search")',
            'button:has-text("search")',
            'button:has-text("Find")',
            'button:has-text("find")',
            'button:has-text("Submit")',
            'button:has-text("Go")',
            'button:has-text("Submit Query")',
            '.search-submit',
            'form button[type="submit"]',
            'button[name="search"]',
            'input[value="Search"]',
            'input[value="search"]'
        ]

        self.next_page_selectors = [
            'a.next',
            '.next',
            '.pagination-next',
            'a:has-text("Next")',
            'a:has-text("next")',
            'a:has-text(">")',
            'a:has-text("»")',
            'button:has-text("Next")',
            'button:has-text("next")',
            '.nav-next',
            '.page-next'
        ]

        self.anti_detection_manager = AntiDetectionManager(
            enable_fingerprint_evasion=True,
            enable_behavioral_mimicking=True,
            enable_network_obfuscation=True,
        )
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None

    async def _initialize_browser(self):
        """Initializes Playwright browser and context with anti-detection."""
        if self.browser and self.context:
            await self._close_browser() # Close existing if any
        
        try:
            pw = await async_playwright().start()
            self.browser, self.context = await create_stealth_browser_context(pw, self.anti_detection_manager)
            self.page = await self.context.new_page()
        except Exception as e:
            logger.error(f"Failed to initialize Playwright browser: {e}")
            raise

    async def _close_browser(self):
        """Closes the Playwright browser and context."""
        if self.context:
            await self.context.close()
            self.context = None
        if self.browser:
            await self.browser.close()
            self.browser = None
        self.page = None

    async def _navigate_to_url(self, url: str, wait_for_selector: Optional[str] = None, timeout_ms: int = 30000) -> PageContent:
        """Navigates to a given URL and returns PageContent."""
        if not self.page:
            await self._initialize_browser()

        logger.info(f"Navigating to URL: {url}")
        
        # Apply network delay before navigating
        delay = await self.anti_detection_manager.calculate_request_delay()
        if delay > 0:
            await asyncio.sleep(delay)
        
        resp = await self.page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        
        try:
            await self.page.wait_for_load_state("networkidle", timeout=timeout_ms // 2)
        except Exception:
            pass
        
        if wait_for_selector:
            try:
                await self.page.wait_for_selector(wait_for_selector, timeout=timeout_ms // 2)
            except Exception:
                pass
        
        # Dismiss popups and simulate human behavior
        try:
            await self.page.evaluate("window.scrollBy(0, 200)") # Initial scroll
            await execute_human_behavior(self.page, self.anti_detection_manager, behavior_type='scroll', position=random.randint(500, 1000))
            await execute_human_behavior(self.page, self.anti_detection_manager, behavior_type='mousemove', position=(random.randint(100, 800), random.randint(100, 500)))
        except Exception as e:
            logger.warning(f"Error during human behavior simulation: {e}")

        html = await self.page.content()
        status = (resp.status if resp else 200)
        
        soup = BeautifulSoup(html, "lxml")
        for tag in soup.find_all(["script", "style"]):
            tag.decompose()
        text_only = soup.get_text(separator="\n", strip=True)

        return PageContent(
            url=url,
            status_code=status,
            elapsed_seconds=0, # This will be calculated in the main run function
            encoding="utf-8",
            content_type="text/html",
            html=html,
            text=text_only,
            metadata={},
        )

    async def search_and_extract(self) -> List[Dict[str, Any]]:
        """Main function to perform search, paginate, and extract data."""
        all_extracted_leads = []
        current_page_num = 1

        await self._initialize_browser()
        
        try:
            # Navigate to the base URL
            page_content = await self._navigate_to_url(self.base_url)
            logger.info(f"Initial page content fetched: {page_content.url}")

            # Check for redirection to login page
            if "login" in self.page.url.lower():
                logger.error(f"Redirected to login page: {self.page.url}. Please ensure the base_url points to a search-enabled page or handle login separately.")
                return []

            # Find the search input field and type the company name
            try:
                search_input_element = self.page.locator(self.search_input_selector)
                if not await search_input_element.is_visible():
                    logger.error(f"Search input field with selector '{self.search_input_selector}' not visible on {self.page.url}. Check selector or page state.")
                    return []
                await search_input_element.fill(self.company_name)
                
                # Simulate a human-like click on the search button
                search_button_element = self.page.locator(self.search_button_selector)
                if await search_button_element.is_visible():
                    bbox = await search_button_element.bounding_box()
                    if bbox:
                        x, y = bbox['x'] + bbox['width'] / 2, bbox['y'] + bbox['height'] / 2
                        await execute_human_behavior(self.page, self.anti_detection_manager, behavior_type='click', position=(x, y))
                    else:
                        await search_button_element.click() # Fallback click
                else:
                    logger.warning(f"Search button with selector '{self.search_button_selector}' not visible, attempting direct click.")
                    await self.page.click(self.search_button_selector)

                await self.page.wait_for_load_state("networkidle")
                logger.info(f"Searched for company: {self.company_name}")
            except Exception as e:
                logger.error(f"Failed to perform search on {self.page.url}: {e}")
                return []

            while current_page_num <= self.max_pages_to_scrape:
                logger.info(f"Scraping page {current_page_num} for company '{self.company_name}'")
                
                # Extract HTML for current page results
                current_page_html = await self.page.content()
                
                # Prepare input for AI extraction
                ai_input_data = {
                    "sections": [{"section": {"text": current_page_html, "tag": "body"}, "priority_score": 1.0}],
                    "structured_data": [] # Assuming no structured data initially, can be enhanced
                }
                
                extracted_data = extract_client_info_from_sections(ai_input_data, self.page.url)
                if extracted_data and extracted_data.get("leads"):
                    logger.info(f"Before transformation: {len(extracted_data['leads'])} leads")
                    # Transform each lead to unified format
                    unified_contacts = []
                    for contact in extracted_data["leads"]:
                        unified_contact = transform_to_unified_format(contact, self.page.url, "company_directory")
                        unified_contacts.append(unified_contact)
                    
                    logger.info(f"After transformation: {len(unified_contacts)} unified contacts")
                    all_extracted_leads.extend(unified_contacts)
                    logger.info(f"Extracted {len(extracted_data['leads'])} leads from page {current_page_num}")

                # Implement pagination
                if self.next_page_selector and current_page_num < self.max_pages_to_scrape:
                    try:
                        next_button = self.page.locator(self.next_page_selector)
                        if await next_button.is_visible():
                            bbox = await next_button.bounding_box()
                            if bbox:
                                x, y = bbox['x'] + bbox['width'] / 2, bbox['y'] + bbox['height'] / 2
                                await execute_human_behavior(self.page, self.anti_detection_manager, behavior_type='click', position=(x, y))
                            else:
                                await next_button.click() # Fallback click
                            await self.page.wait_for_load_state("networkidle")
                            current_page_num += 1
                        else:
                            logger.info("No next page button found, ending pagination.")
                            break
                    except Exception as e:
                        logger.warning(f"Error navigating to next page: {e}. Ending pagination.")
                        break
                else:
                    logger.info(f"Reached maximum pages ({self.max_pages_to_scrape}) to scrape or no next page selector provided.")
                    break

        finally:
            await self._close_browser()

        return all_extracted_leads

    async def scrape_any_website(self) -> Dict[str, Any]:
        """Universal scraper that can handle any website automatically."""
        results = {
            "url": self.url,
            "page_type": "unknown",
            "content_type": "unknown",
            "extracted_data": [],
            "raw_content": "",
            "metadata": {},
            "errors": []
        }

        await self._initialize_browser()

        try:
            # Navigate to the URL
            logger.info(f"🌐 Navigating to: {self.url}")
            page_content = await self._navigate_to_url(self.url)

            # Detect page type
            results["page_type"] = self._detect_page_type(page_content.html, self.url)
            logger.info(f"📋 Detected page type: {results['page_type']}")

            # Get page content
            html_content = await self.page.content()
            results["raw_content"] = html_content

            # Extract data based on page type
            if results["page_type"] == "company_directory":
                extracted_data = await self._scrape_company_directory()
            elif results["page_type"] == "social_media":
                extracted_data = await self._scrape_social_media()
            elif results["page_type"] == "news_article":
                extracted_data = await self._scrape_news_article()
            elif results["page_type"] == "business_listing":
                extracted_data = await self._scrape_business_listing()
            else:
                # Generic extraction for unknown page types
                extracted_data = await self._scrape_generic_page()

            results["extracted_data"] = extracted_data
            results["content_type"] = "structured_data"

        except Exception as e:
            error_msg = f"Failed to scrape website: {e}"
            logger.error(error_msg)
            results["errors"].append(error_msg)
        finally:
            await self._close_browser()

        return results

    def _detect_page_type(self, html: str, url: str) -> str:
        """Detect the type of webpage based on content and URL patterns."""
        url_lower = url.lower()
        html_lower = html.lower()

        # Company directory patterns - expanded
        company_keywords = [
            'kompass', 'thomasnet', 'yellowpages', 'yelp', 'company directory', 'business directory',
            'opencorporates', 'corporate registry', 'company search', 'business search',
            'company database', 'corporate database', 'company information', 'business information',
            'company register', 'corporate register', 'companies house', 'sec filings'
        ]
        if any(keyword in html_lower for keyword in company_keywords):
            return "company_directory"

        url_patterns = ['/search', '/directory', '/companies', '/businesses', '/corporations', '/opencorporates']
        if any(pattern in url_lower for pattern in url_patterns):
            return "company_directory"

        # Domain-based detection
        company_domains = ['opencorporates.com', 'kompass.com', 'thomasnet.com', 'yellowpages.com', 'yelp.com', 'crunchbase.com']
        if any(domain in url_lower for domain in company_domains):
            return "company_directory"

        # Social media patterns
        if any(domain in url_lower for domain in ['facebook.com', 'linkedin.com', 'twitter.com', 'instagram.com']):
            return "social_media"

        # News/article patterns - more specific
        news_keywords = ['published', 'author', 'byline', 'article-content', 'news-article']
        if any(keyword in html_lower for keyword in news_keywords):
            return "news_article"

        # Business listing patterns
        if any(keyword in html_lower for keyword in ['listing', 'business', 'contact', 'about us']):
            return "business_listing"

        # Default to generic
        return "generic"

    async def _scrape_company_directory(self) -> List[Dict[str, Any]]:
        """Scrape company directory pages with universal support for any business directory."""
        logger.info("🏢 Scraping company directory")

        # Check for known directories that need special handling
        url_lower = self.url.lower()

        # Special handling for ThomasNet - has unique structure
        if "thomasnet.com" in url_lower:
            return await self._scrape_thomasnet_directory()

        # Special handling for YellowPages - local business directory
        if "yellowpages.com" in url_lower:
            return await self._scrape_yellowpages_directory()

        # Special handling for Crunchbase - startup database (though blocked by bot protection)
        if "crunchbase.com" in url_lower:
            return await self._scrape_crunchbase_directory()

        # For ALL other directories, use universal extraction that works with any business directory
        logger.info("🌍 Using universal directory scraper - works with any business directory website")
        return await self._scrape_universal_directory()

    async def _scrape_universal_directory(self) -> List[Dict[str, Any]]:
        """Universal directory scraper that works with any business directory website."""
        logger.info("🌍 Scraping universal directory - works with any business directory")

        # Check if this looks like a search results page
        is_search_page = await self._is_search_results_page()

        if is_search_page and self.search_query:
            # This is a search results page - extract company URLs and visit them
            logger.info("📋 Detected search results page, extracting company URLs")
            return await self._scrape_universal_search_results()
        else:
            # This is a single company/business page - extract contact info directly
            logger.info("🏢 Detected single business page, extracting contact info")
            return await self._scrape_universal_business_page()

    async def _is_search_results_page(self) -> bool:
        """Determine if current page is a search results page."""
        try:
            # Check for common search result indicators
            search_indicators = [
                # Multiple business listings
                '.result', '.listing', '.business-card', '.company-item',
                '.search-result', '.directory-listing', '.business-listing',
                # Pagination elements
                '.pagination', '.page-numbers', 'nav[aria-label*="pagination" i]',
                # Result counts
                'results', 'found', 'matches',
                # Multiple similar items
                'h2', 'h3', '.title', '.name'  # Multiple headings/titles
            ]

            # Count potential business listings
            listing_count = 0
            for indicator in search_indicators[:5]:  # Check first 5 indicators
                try:
                    elements = await self.page.query_selector_all(indicator)
                    if len(elements) > 3:  # If more than 3 similar elements, likely a results page
                        listing_count += len(elements)
                except:
                    continue

            # Check URL for search parameters
            url = self.page.url.lower()
            search_params = ['q=', 'query=', 'search=', 'keyword=', 'term=']
            has_search_param = any(param in url for param in search_params)

            # If we have multiple listings OR search parameters, it's likely a search page
            return listing_count > 5 or has_search_param

        except Exception as e:
            logger.warning(f"Error detecting search page: {e}")
            return False

    async def _scrape_universal_search_results(self) -> List[Dict[str, Any]]:
        """Extract company URLs from search results and visit each for contact info."""
        try:
            # Extract company/business URLs from search results
            company_urls = await self._extract_universal_company_urls()

            if not company_urls:
                logger.warning("No company URLs found in search results")
                # Fall back to extracting from current page
                return await self._scrape_universal_business_page()

            logger.info(f"Found {len(company_urls)} company URLs to visit")

            all_contacts = []
            visited_count = 0
            max_companies = min(20, len(company_urls))  # Limit to 20 companies

            for company_url in company_urls[:max_companies]:
                try:
                    visited_count += 1
                    logger.info(f"Visiting company page {visited_count}/{max_companies}: {company_url}")

                    # Navigate to company page
                    await self.page.goto(company_url, wait_until="domcontentloaded")
                    await asyncio.sleep(random.uniform(2, 4))  # Wait for page to load

                    # Extract contact information from company page
                    contact_data = await self._extract_universal_business_contact()
                    if contact_data:
                        all_contacts.append(contact_data)
                        logger.info("✅ Extracted contact data from company page")
                    else:
                        logger.debug(f"❌ No contact data found on company page: {company_url}")

                    # Add delay between requests to be respectful
                    if visited_count < max_companies:
                        await asyncio.sleep(random.uniform(3, 6))

                except Exception as e:
                    logger.warning(f"Failed to extract from company page {company_url}: {e}")
                    continue

            logger.info(f"✅ Completed universal directory extraction: {len(all_contacts)} contacts from {visited_count} company pages")
            return all_contacts

        except Exception as e:
            logger.error(f"Failed to scrape universal search results: {e}")
            return []

    async def _extract_universal_company_urls(self) -> List[str]:
        """Extract company/business profile URLs from any directory search results."""
        company_urls = []

        try:
            # Universal selectors for company/business links
            company_selectors = [
                # Direct company links
                'a[href*="/company/"]', 'a[href*="/business/"]', 'a[href*="/profile/"]',
                'a[href*="/listing/"]', 'a[href*="/details/"]', 'a[href*="/info/"]',

                # Common directory patterns
                '.result a', '.listing a', '.business-card a', '.company-item a',
                '.search-result a', '.directory-listing a', '.business-listing a',

                # Title/name links (often lead to detail pages)
                'h1 a', 'h2 a', 'h3 a', 'h4 a',
                '.title a', '.name a', '.business-name a', '.company-name a',

                # Generic links that might be company pages
                'a[href*="?"]', 'a[href*="/"]'  # Links with query params or subpaths
            ]

            for selector in company_selectors:
                try:
                    links = await self.page.query_selector_all(selector)
                    for link in links:
                        href = await link.get_attribute('href')
                        if href:
                            # Convert relative URLs to absolute
                            if href.startswith('/'):
                                parsed_url = urlparse(self.page.url)
                                base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                                href = base_url + href
                            elif not href.startswith('http'):
                                continue  # Skip invalid URLs

                            # Skip obvious non-company links
                            skip_patterns = [
                                'javascript:', 'mailto:', 'tel:', '#',
                                '/search', '/category', '/tag', '/page/',
                                'facebook.com', 'twitter.com', 'linkedin.com',
                                'youtube.com', 'instagram.com'
                            ]

                            if any(pattern in href.lower() for pattern in skip_patterns):
                                continue

                            # Only include if it's on the same domain and looks like a company page
                            parsed_href = urlparse(href)
                            parsed_base = urlparse(self.page.url)

                            if (parsed_href.netloc == parsed_base.netloc and
                                len(parsed_href.path.strip('/').split('/')) >= 2 and  # At least 2 path segments
                                href not in company_urls):
                                company_urls.append(href)

                    if len(company_urls) >= 10:  # Stop if we have enough URLs
                        break

                except Exception as e:
                    logger.debug(f"Selector {selector} failed: {e}")
                    continue

            # Remove duplicates and limit
            company_urls = list(set(company_urls))[:50]  # Max 50 URLs

        except Exception as e:
            logger.error(f"Failed to extract universal company URLs: {e}")

        logger.info(f"Extracted {len(company_urls)} company URLs from universal search results")
        return company_urls

    async def _scrape_universal_business_page(self) -> List[Dict[str, Any]]:
        """Extract contact information from a single business/company page."""
        try:
            contact_data = await self._extract_universal_business_contact()
            if contact_data:
                logger.info("✅ Extracted contact data from universal business page")
                return [contact_data]
            else:
                logger.warning("❌ No contact data found on universal business page")
                return []
        except Exception as e:
            logger.error(f"Failed to scrape universal business page: {e}")
            return []

    async def _extract_universal_business_contact(self) -> Optional[Dict[str, Any]]:
        """Extract contact information from any business/company page using AI and enhanced extraction."""
        try:
            # Extract from page content using AI
            html_content = await self.page.content()

            # Create a universal prompt that works for any business directory
            prompt = f"""Extract comprehensive contact information from this business/company page.

Page Content (first 15000 characters):
{html_content[:15000]}

This is a business or company profile page. Extract ALL available contact details:

CRITICAL: Look for business/company information sections, contact details, and company profiles.
- Company/business name (exact legal/registered name)
- Contact person name and title/role (owner, manager, executive)
- ALL phone numbers (main, office, cell, fax, multiple locations, toll-free)
- ALL email addresses (primary, contact, info, sales, support, admin emails)
- Company website URL (if different from current page)
- Complete business address (headquarters, main office, street address)
- Social media profiles (LinkedIn, Twitter, Facebook, Instagram, YouTube)
- Industry/sector information
- Company size, employee count, revenue range

Search for these specific patterns in ANY language:
- Emails: contact@, info@, sales@, support@, admin@, hello@, business@, company@
- Phones: +1-, 1-, area codes, local numbers, cell phones, fax numbers, toll-free
- Websites: https://www., http://, business domains, .com, .net, .org
- Social: linkedin.com/company, facebook.com/business, twitter.com/, instagram.com/
- Addresses: street names, city/state/zip, postal codes, countries

IMPORTANT: Extract REAL contact details visible on the page. Do not invent information.
Return comprehensive JSON with ALL found information:
{{
  "name": "Exact company/business name",
  "email": ["email1@domain.com", "email2@domain.com"],
  "phone": ["+1-555-123-4567", "(555) 123-4567"],
  "organization": "Company name",
  "role": "Contact person title/role",
  "confidence": 0.8,
  "source": "Universal Business Directory",
  "notes": "Additional contact details or business info",
  "lead_category": "Business/Company",
  "lead_sub_category": "Industry/Sector",
  "bio": "Business description",
  "industry": "Primary industry",
  "company_type": "Business type",
  "location": "City, State/Country",
  "employee_count": "Approximate employee count",
  "revenue": "Revenue range if available",
  "websites": ["https://www.company.com"],
  "social_media": {{
    "linkedin": "https://linkedin.com/company/companyname",
    "twitter": "https://twitter.com/companyhandle",
    "facebook": "https://www.facebook.com/companyname",
    "instagram": "https://www.instagram.com/companyhandle",
    "youtube": null
  }},
  "address": "Complete street address, City, State ZIP, Country",
  "status": "active",
  "incorporation_date": null,
  "jurisdiction": null
}}

If no contact information is found, return null."""

            ai_result = _ai_generate_json(prompt)
            if ai_result and ai_result.get('name'):
                # Enhance with direct contact extraction
                enhanced_result = await enhance_contact_details(self.page, ai_result)
                return enhanced_result

        except Exception as e:
            logger.warning(f"Failed to extract contact data from universal business page: {e}")

        return None

    async def _scrape_thomasnet_directory(self) -> List[Dict[str, Any]]:
        """ThomasNet has company listings on search results. Visit individual company pages for contact details."""
        logger.info("🔍 Scraping ThomasNet directory - visiting company detail pages for contact info")

        # Check if we're on a search URL that needs to be executed
        current_url = self.page.url
        if '/search.html' in current_url and ('what=' in current_url or 'q=' in current_url):
            logger.info("On ThomasNet search page, need to perform search first")
            # Extract search query from URL
            search_query = ""
            if 'what=' in current_url:
                search_query = current_url.split('what=')[1].split('&')[0].replace('+', ' ')
            elif 'q=' in current_url:
                search_query = current_url.split('q=')[1].split('&')[0].replace('+', ' ')

            if search_query:
                logger.info(f"Performing search for: {search_query}")
                # Perform the search
                search_success = await self._perform_thomasnet_search(search_query)
                if not search_success:
                    logger.warning("Failed to perform ThomasNet search")
                    return await self._scrape_generic_page()
            else:
                logger.warning("Could not extract search query from URL")
                return await self._scrape_generic_page()

        # Now extract company URLs from search results
        company_urls = await self._extract_thomasnet_company_urls()
        logger.info(f"Found {len(company_urls)} company URLs to visit")

        if not company_urls:
            logger.info("No company URLs found, falling back to generic extraction")
            return await self._scrape_generic_page()

        # Visit each company page and extract contact information
        all_contacts = []
        visited_count = 0
        max_companies = min(20, len(company_urls))  # Limit to 20 companies to avoid being too aggressive

        for company_url in company_urls[:max_companies]:
            try:
                logger.info(f"Visiting company page {visited_count + 1}/{max_companies}: {company_url}")

                # Navigate to company page
                await self.page.goto(company_url, wait_until="domcontentloaded")
                await asyncio.sleep(random.uniform(2, 4))  # Wait for page to load

                # Extract contact information from company page
                contact_data = await self._extract_thomasnet_company_contact()
                if contact_data:
                    all_contacts.append(contact_data)
                    logger.info(f"✅ Extracted contact data from company page")
                else:
                    logger.debug(f"❌ No contact data found on company page: {company_url}")

                visited_count += 1

                # Add delay between requests to be respectful
                if visited_count < max_companies:
                    await asyncio.sleep(random.uniform(3, 6))

            except Exception as e:
                logger.warning(f"Failed to extract from company page {company_url}: {e}")
                continue

        logger.info(f"✅ Completed ThomasNet extraction: {len(all_contacts)} contacts from {visited_count} company pages")
        return all_contacts

    async def _extract_thomasnet_category_links(self) -> List[str]:
        """Extract category/supplier links from ThomasNet search results."""
        category_links = []

        try:
            # Look for category links that match the pattern /suppliers/...
            links = await self.page.query_selector_all('a[href*="/suppliers/"]')

            for link in links:
                href = await link.get_attribute('href')
                if href and href.startswith('/suppliers/') and not href.count('/') > 3:  # Avoid deep nested links
                    if href not in category_links:
                        category_links.append(href)

        except Exception as e:
            logger.warning(f"Failed to extract category links: {e}")

        return category_links[:10]  # Limit to first 10 categories

    async def _extract_thomasnet_company_links(self) -> List[str]:
        """Extract individual company profile links from ThomasNet category page."""
        company_links = []

        try:
            # First, let's see what we have on the page
            page_title = await self.page.title()
            logger.info(f"Category page title: {page_title}")
            
            # Get some sample HTML to understand the structure
            body_html = await self.page.inner_html('body')
            logger.info(f"Page body length: {len(body_html)}")
            
            # Look for various patterns that might indicate company listings
            selectors = [
                'a[href*="/profile/"]',
                'a[href*="/company/"]', 
                'a[href*="/products/"]',
                '.supplier-result a',
                '.company-link',
                '.profile-link',
                '.supplier-name a',
                '.company-name a',
                'h3 a',  # Company names in headings
                '.result-item a',
                '.listing a',
                '.supplier-item a',
                '.company-item a',
                'a[href*="supplier"]',
                'a[href*="manufacturer"]'
            ]

            all_found_links = []
            for selector in selectors:
                try:
                    links = await self.page.query_selector_all(selector)
                    if links:
                        logger.info(f"Selector '{selector}' found {len(links)} links")
                        # Log all links found by this selector
                        for i, link in enumerate(links):
                            href = await link.get_attribute('href')
                            text = await link.inner_text()
                            if href:
                                logger.info(f"  [{i+1}] Link: {text.strip()[:30] if text else 'NO TEXT'}... -> {href}")
                    
                    # Add to our collection - be more permissive for ThomasNet
                    for link in links:
                        href = await link.get_attribute('href')
                        text = await link.inner_text()
                        if href and text and len(text.strip()) > 3:
                            # Accept links that look like company profiles
                            is_company_link = ('/profile/' in href or 
                                             '/company/' in href or 
                                             ('/products/' in href and not href.endswith('/products/')))
                            
                            if is_company_link:
                                if href not in company_links:
                                    company_links.append(href)
                                    all_found_links.append(href)
                                    logger.info(f"✅ Added company link: {text.strip()[:30]}... -> {href}")
                            else:
                                logger.debug(f"❌ Rejected link: {text.strip()[:30]}... -> {href} (not a company profile)")
                        elif href and not text:
                            logger.debug(f"❌ Link has no text: {href}")
                        elif href and text and len(text.strip()) <= 3:
                            logger.debug(f"❌ Link text too short: '{text.strip()}' -> {href}")
                                
                except Exception as e:
                    logger.debug(f"Selector '{selector}' failed: {e}")
                    continue
            
            logger.info(f"Total unique company links found: {len(company_links)}")
            
            # If no specific selectors worked, try a broader approach
            if not company_links:
                logger.info("No company links found with specific selectors, trying broader search...")
                all_links = await self.page.query_selector_all('a[href]')
                for link in all_links:
                    href = await link.get_attribute('href')
                    text = await link.inner_text()
                    if href and text and len(text.strip()) > 3:
                        # Look for links that might be company names
                        if (not href.startswith('javascript:') and 
                            not href.startswith('#') and
                            not href.startswith('mailto:') and
                            not href.startswith('tel:') and
                            not any(skip in href.lower() for skip in ['/suppliers/', '/browse/', '/search', '/about', '/contact', '/privacy', '/terms'])):
                            if href not in company_links:
                                company_links.append(href)
                                logger.info(f"Broad search found: {text.strip()[:30]}... -> {href}")

        except Exception as e:
            logger.warning(f"Failed to extract company links: {e}")

        return company_links[:20]  # Limit companies per category

    async def _perform_thomasnet_search(self, search_query: str) -> bool:
        """Perform search on ThomasNet search page."""
        try:
            # ThomasNet might work by direct URL navigation, try that first
            search_url = f"https://www.thomasnet.com/search.html?what={search_query.replace(' ', '+')}"
            logger.info(f"Trying direct navigation to ThomasNet search URL: {search_url}")

            await self.page.goto(search_url, wait_until="domcontentloaded")
            await asyncio.sleep(2)

            # Check if we're on a search results page
            current_url = self.page.url
            if 'search.html' in current_url and ('what=' in current_url or 'q=' in current_url):
                logger.info("Successfully navigated to ThomasNet search results")
                return True

            # Fallback to trying search input
            search_input_selectors = [
                'input[name="what"]',
                'input#search-what',
                'input[placeholder*="search" i]',
                'input[type="text"]',
                '.search-input input',
                '.search-box input'
            ]

            search_input = None
            for selector in search_input_selectors:
                try:
                    search_input = await self.page.query_selector(selector)
                    if search_input:
                        logger.info(f"Found search input with selector: {selector}")
                        break
                except Exception:
                    continue

            if not search_input:
                logger.error("Could not find ThomasNet search input")
                return False

            # Clear and fill search query
            await search_input.fill("")
            await search_input.fill(search_query)

            # Find and click search button
            search_button_selectors = [
                'input[type="submit"]',
                'button[type="submit"]',
                '.search-btn',
                'button[aria-label*="search" i]',
                '.search-button',
                'form button'
            ]

            search_button = None
            for selector in search_button_selectors:
                try:
                    search_button = await self.page.query_selector(selector)
                    if search_button:
                        logger.info(f"Found search button with selector: {selector}")
                        break
                except Exception:
                    continue

            if search_button:
                await search_button.click()
                await self.page.wait_for_load_state("networkidle")
                await asyncio.sleep(2)
                logger.info(f"Successfully performed ThomasNet search for: {search_query}")
                return True
            else:
                # Try pressing Enter in the search input
                await search_input.press("Enter")
                await self.page.wait_for_load_state("networkidle")
                await asyncio.sleep(2)
                logger.info(f"Successfully performed ThomasNet search with Enter key for: {search_query}")
                return True

        except Exception as e:
            logger.error(f"Failed to perform ThomasNet search: {e}")
            return False

    async def _extract_thomasnet_company_urls(self) -> List[str]:
        """Extract company profile URLs from ThomasNet search results."""
        company_urls = []

        try:
            # ThomasNet company links are typically in search result listings
            company_selectors = [
                'a[href*="/profile/"]',
                'a[href*="/company/"]',
                '.supplier-result a',
                '.company-link',
                '.profile-link',
                '.supplier-name a',
                '.company-name a',
                'h3 a',
                '.result-item a',
                '.listing a'
            ]

            for selector in company_selectors:
                try:
                    links = await self.page.query_selector_all(selector)
                    for link in links:
                        href = await link.get_attribute('href')
                        if href:
                            # Convert relative URLs to absolute
                            if href.startswith('/'):
                                href = f"https://www.thomasnet.com{href}"
                            elif not href.startswith('http'):
                                continue

                            # Avoid duplicate URLs and non-company pages
                            if href not in company_urls and any(pattern in href for pattern in ['/profile/', '/company/']):
                                company_urls.append(href)
                except Exception:
                    continue

            # Limit to reasonable number
            company_urls = company_urls[:50]

        except Exception as e:
            logger.warning(f"Failed to extract company URLs: {e}")

        return company_urls

    async def _extract_thomasnet_company_contact(self) -> Optional[Dict[str, Any]]:
        """Extract contact information from ThomasNet company page."""
        try:

            # Extract from page content using AI
            html_content = await self.page.content()

            prompt = f"""Extract comprehensive contact information from this ThomasNet company page.

Page Content (first 10000 characters):
{html_content[:10000]}

This is a ThomasNet company profile page with supplier/manufacturer information. Extract ALL available contact details:

CRITICAL: Look for contact sections, company information panels, and supplier details.
- Company name (exact legal name)
- Contact person name and title
- ALL phone numbers (main, toll-free, fax, multiple locations)
- ALL email addresses (primary, sales, support, info emails)
- Company website URL
- Complete business address (street, city, state, zip)
- Social media profiles (LinkedIn, Twitter, Facebook, YouTube)
- Business type and industry categories

Search for these specific patterns:
- Emails: @company.com, contact@, sales@, info@, support@, inquiry@
- Phones: +1-, 1-, area codes, toll-free 800/888/877 numbers, fax numbers
- Websites: https://www., http://, company domains
- Social: linkedin.com/company, twitter.com/, facebook.com/, youtube.com/
- Addresses: street addresses, PO boxes, city/state/zip codes

Return comprehensive JSON in the exact format specified below:

{{
  "leads": [
    {{
      "name": "Contact person name or primary contact",
      "contact_info": {{
        "email": "primary email address",
        "phone": "primary phone number",
        "linkedin": "LinkedIn profile URL",
        "twitter": "Twitter handle or URL",
        "website": "company website URL",
        "others": "other contact methods",
        "socialmedialinks": ["array of all social media URLs"]
      }},
      "company_name": "Full company/organization name",
      "time": "current timestamp in ISO format",
      "link_details": "short description of the source link/page content",
      "type": "lead or competitor",
      "what_we_can_offer": "suggested services/products we can provide",
      "source_url": "{self.page.url}",
      "source_platform": "ThomasNet",
      "location": "city, state, country or geographic location",
      "industry": "industry sector or business category",
      "company_type": "company type (Private, Public, LLC, Corporation, etc.)",
      "bio": "company description or business summary",
      "address": "complete business address"
    }}
  ],
  "summary": "Summary of extracted business information",
  "total_leads": 1
}}

IMPORTANT: Extract REAL contact details visible on the page. Return data in the "leads" array format as specified above."""

            ai_result = _ai_generate_json(prompt)
            if ai_result and ai_result.get('leads') and ai_result['leads']:
                # Take the first lead from the array
                lead_data = ai_result['leads'][0]
                # Enhance with direct contact extraction
                enhanced_result = await enhance_contact_details(self.page, lead_data)
                return enhanced_result

        except Exception as e:
            logger.warning(f"Failed to extract contact data from ThomasNet company page: {e}")

        return None

    async def _scrape_crunchbase_directory(self) -> List[Dict[str, Any]]:
        """Crunchbase has company listings on search results. Visit individual company pages for contact details."""
        logger.info("🏢 Scraping Crunchbase directory - visiting company detail pages for contact info")

        # Check if we're on a search URL - Crunchbase loads results directly from URL
        current_url = self.page.url
        if '/search/organizations' in current_url and ('q=' in current_url):
            logger.info("On Crunchbase search page, waiting for results to load")
            # Wait for search results to load
            await asyncio.sleep(5)  # Give time for dynamic content
        else:
            logger.info("Not on expected Crunchbase search URL")

        # Extract company URLs from search results
        company_urls = await self._extract_crunchbase_company_urls()
        if not company_urls:
            logger.warning("No company URLs found on Crunchbase search results")
            return []

        logger.info(f"Found {len(company_urls)} company URLs to visit")

        all_contacts = []
        visited_count = 0
        max_companies = min(20, len(company_urls))  # Limit to 20 companies

        for company_url in company_urls[:max_companies]:
            try:
                visited_count += 1
                logger.info(f"Visiting company page {visited_count}/{max_companies}: {company_url}")

                # Navigate to company page
                await self.page.goto(company_url, wait_until="domcontentloaded")
                await asyncio.sleep(random.uniform(2, 4))  # Wait for page to load

                # Extract contact information from company page
                contact_data = await self._extract_crunchbase_company_contact()
                if contact_data:
                    all_contacts.append(contact_data)
                    logger.info(f"✅ Extracted contact data from company page")
                else:
                    logger.debug(f"❌ No contact data found on company page: {company_url}")

                # Add delay between requests to be respectful
                if visited_count < max_companies:
                    await asyncio.sleep(random.uniform(3, 6))

            except Exception as e:
                logger.warning(f"Failed to extract from company page {company_url}: {e}")
                continue

        logger.info(f"✅ Completed Crunchbase extraction: {len(all_contacts)} contacts from {visited_count} company pages")
        return all_contacts

    async def _perform_crunchbase_search(self, search_query: str) -> bool:
        """Perform search on Crunchbase search page."""
        try:
            # Find search input - Crunchbase has various possible selectors
            search_input_selectors = [
                'input[name="query"]',
                'input[name="q"]',
                'input[placeholder*="search" i]',
                'input[placeholder*="company" i]',
                'input[type="search"]',
                'input[data-test*="search"]',
                '.search-input input',
                '.search-box input'
            ]
            search_input = None
            for selector in search_input_selectors:
                search_input = await self.page.query_selector(selector)
                if search_input:
                    logger.info(f"Found search input with selector: {selector}")
                    break

            if not search_input:
                logger.error("Could not find Crunchbase search input")
                return False

            # Clear and type search query
            await search_input.clear()
            await search_input.type(search_query, delay=random.uniform(100, 200))
            await asyncio.sleep(1)

            # Find and click search button
            search_button_selectors = [
                'button[type="submit"]',
                'button.search-submit',
                'input[type="submit"]',
                'button[data-test*="search"]',
                'button[aria-label*="search" i]',
                '.search-button',
                '.search-submit'
            ]
            search_button = None
            for selector in search_button_selectors:
                search_button = await self.page.query_selector(selector)
                if search_button:
                    logger.info(f"Found search button with selector: {selector}")
                    break

            if not search_button:
                # Try pressing Enter in the search input
                logger.info("No search button found, trying to submit with Enter key")
                await search_input.press("Enter")
            else:
                # Click search button
                await search_button.click()

            await self.page.wait_for_load_state("networkidle")
            await asyncio.sleep(3)  # Extra wait for Crunchbase

            logger.info(f"Successfully performed Crunchbase search for: {search_query}")
            return True

        except Exception as e:
            logger.error(f"Failed to perform Crunchbase search: {e}")
            return False

    async def _extract_crunchbase_company_urls(self) -> List[str]:
        """Extract company profile URLs from Crunchbase search results."""
        company_urls = []

        try:
            # Debug: Log page content to understand structure
            html_content = await self.page.content()
            logger.info(f"Crunchbase page content length: {len(html_content)}")
            logger.info(f"First 1000 chars: {html_content[:1000]}")

            # Crunchbase company links - try multiple selectors for different layouts
            company_selectors = [
                'a[href*="/organization/"]',
                '.result a[href*="/organization/"]',
                '.search-result a[href*="/organization/"]',
                '.entity-result a[href*="/organization/"]',
                '[data-test*="result"] a[href*="/organization/"]',
                '.organization-result a[href*="/organization/"]',
                '.company-result a[href*="/organization/"]'
            ]

            for selector in company_selectors:
                try:
                    links = await self.page.query_selector_all(selector)
                    for link in links:
                        href = await link.get_attribute('href')
                        if href:
                            # Convert relative URLs to absolute
                            if href.startswith('/'):
                                href = f"https://crunchbase.com{href}"
                            # Only include organization URLs and avoid duplicates
                            if '/organization/' in href and href not in company_urls:
                                company_urls.append(href)
                    if company_urls:
                        logger.info(f"Found {len(company_urls)} companies with selector: {selector}")
                        break
                except Exception as e:
                    logger.debug(f"Selector {selector} failed: {e}")
                    continue

            # Remove duplicates
            company_urls = list(set(company_urls))

        except Exception as e:
            logger.error(f"Failed to extract Crunchbase company URLs: {e}")

        logger.info(f"Extracted {len(company_urls)} company URLs from Crunchbase search results")
        return company_urls

    async def _extract_crunchbase_company_contact(self) -> Optional[Dict[str, Any]]:
        """Extract contact information from Crunchbase company page."""
        try:
            # Extract from page content using AI
            html_content = await self.page.content()

            prompt = f"""Extract comprehensive contact information from this Crunchbase company page.

Page Content (first 10000 characters):
{html_content[:10000]}

This is a Crunchbase company profile page with startup/company information. Extract ALL available contact details:

CRITICAL: Look for company information sections, contact details, and business profiles.
- Company name (exact legal name)
- Contact person name and title (founders, executives)
- ALL phone numbers (main, office, multiple locations)
- ALL email addresses (primary, contact, info, press emails)
- Company website URL
- Complete business address (headquarters, offices)
- Social media profiles (LinkedIn, Twitter, Facebook, Instagram, YouTube)
- Industry categories and business focus

Search for these specific patterns:
- Emails: @company.com, contact@, info@, press@, hello@, support@
- Phones: +1-, 1-, area codes, office numbers, international numbers
- Websites: https://www., http://, company domains
- Social: linkedin.com/company, twitter.com/, facebook.com/, instagram.com/, youtube.com/
- Addresses: street addresses, city/state/zip codes, country locations

Return comprehensive JSON with ALL found information:
{{
  "name": "Exact Company Legal Name",
  "email": ["contact@company.com", "info@company.com", "press@company.com"],
  "phone": ["+1-555-123-4567", "+1-555-123-4568"],
  "organization": "Exact Company Legal Name",
  "role": "Founder/CEO/CTO - Executive Name",
  "confidence": 0.9,
  "source": "Crunchbase Company Page",
  "notes": "Complete company profile including description, funding, products, team size",
  "lead_category": "B2B",
  "lead_sub_category": "Technology/Startup/SaaS",
  "bio": "Detailed company description, mission, products, funding rounds, key metrics",
  "industry": "Primary industry sector (Technology/Fintech/Healthtech/E-commerce)",
  "company_type": "Startup/Private/Public Company",
  "location": "Headquarters City, State, Country",
  "employee_count": "number of employees (from Crunchbase data)",
  "revenue": "funding amount or revenue if mentioned",
  "websites": ["https://www.companywebsite.com"],
  "social_media": {{
    "linkedin": "https://www.linkedin.com/company/companyname",
    "twitter": "https://twitter.com/companyhandle",
    "facebook": "https://www.facebook.com/companyname",
    "instagram": "https://www.instagram.com/companyhandle",
    "youtube": "https://www.youtube.com/channel/companychannel",
    "other": []
  }},
  "address": "Headquarters street address, City, State ZIP, Country",
  "status": "active",
  "incorporation_date": "founding year if available",
  "jurisdiction": "incorporation state/country"
}}

IMPORTANT: Extract REAL contact details visible on the page. Do not return empty email/phone arrays unless NO contact information exists on the page."""

            ai_result = _ai_generate_json(prompt)
            if ai_result and ai_result.get('name'):
                # Enhance with direct contact extraction
                enhanced_result = await enhance_contact_details(self.page, ai_result)
                return enhanced_result

        except Exception as e:
            logger.warning(f"Failed to extract contact data from Crunchbase company page: {e}")

        return None

    async def _scrape_yellowpages_directory(self) -> List[Dict[str, Any]]:
        """YellowPages has business listings on search results. Extract contact details from search results."""
        logger.info("📒 Scraping YellowPages directory - extracting contacts from search results")

        # Check if we're on a search URL that needs to be executed
        current_url = self.page.url
        if '/search' in current_url and ('search_terms=' in current_url or 'query=' in current_url):
            logger.info("On YellowPages search page, need to perform search first")
            # Extract search query from URL
            search_query = ""
            if 'search_terms=' in current_url:
                search_query = current_url.split('search_terms=')[1].split('&')[0].replace('+', ' ')
            elif 'query=' in current_url:
                search_query = current_url.split('query=')[1].split('&')[0].replace('+', ' ')

            if search_query:
                logger.info(f"Performing search for: {search_query}")
                # Perform the search
                search_success = await self._perform_yellowpages_search(search_query)
                if not search_success:
                    logger.warning("Failed to perform YellowPages search")
                    return await self._scrape_generic_page()
            else:
                logger.warning("Could not extract search query from URL")
                return await self._scrape_generic_page()

        # Extract contact information directly from search results page
        logger.info("Extracting contacts directly from YellowPages search results")
        contacts = await self._extract_contacts_from_yellowpages_search_results()

        if contacts:
            logger.info(f"✅ Extracted {len(contacts)} contacts from YellowPages search results")
            return contacts
        else:
            logger.warning("No contacts found in search results, trying individual pages")
            # Fallback to visiting individual pages (though they may be blocked)
            return await self._scrape_yellowpages_individual_pages()

    async def _extract_contacts_from_yellowpages_search_results(self) -> List[Dict[str, Any]]:
        """Extract contact information directly from YellowPages search results page."""
        try:
            # Get the full page content
            html_content = await self.page.content()
            
            contacts = []
            
            # Use regex to find business result blocks
            import re
            
            # Find business result containers (they often have class="result" or similar)
            result_pattern = r'<div[^>]*class="[^"]*result[^"]*"[^>]*>(.*?)</div>'
            result_blocks = re.findall(result_pattern, html_content, re.DOTALL | re.IGNORECASE)
            
            if not result_blocks:
                # Try alternative patterns
                result_pattern = r'<div[^>]*class="[^"]*listing[^"]*"[^>]*>(.*?)</div>'
                result_blocks = re.findall(result_pattern, html_content, re.DOTALL | re.IGNORECASE)
            
            logger.info(f"Found {len(result_blocks)} potential business result blocks")
            
            for i, block in enumerate(result_blocks[:20]):  # Limit to first 20 results
                try:
                    contact = {
                        "name": "",
                        "email": [],
                        "phone": [],
                        "organization": "",
                        "role": None,
                        "confidence": 0.7,
                        "source": "YellowPages Search Results",
                        "notes": "",
                        "lead_category": "Local Business",
                        "lead_sub_category": "",
                        "bio": None,
                        "industry": "",
                        "company_type": "Local Business",
                        "location": "Los Angeles, CA, USA",
                        "employee_count": None,
                        "revenue": None,
                        "websites": [],
                        "social_media": {
                            "linkedin": None,
                            "twitter": None,
                            "facebook": None,
                            "instagram": None,
                            "youtube": None
                        },
                        "address": "",
                        "status": "active",
                        "incorporation_date": None,
                        "jurisdiction": None
                    }
                    
                    # Extract business name
                    name_patterns = [
                        r'<a[^>]*class="[^"]*business-name[^"]*"[^>]*>(.*?)</a>',
                        r'<h2[^>]*class="[^"]*business-name[^"]*"[^>]*>(.*?)</h2>',
                        r'<span[^>]*class="[^"]*business-name[^"]*"[^>]*>(.*?)</span>',
                        r'<a[^>]*href="[^"]*business-details[^"]*"[^>]*>(.*?)</a>'
                    ]
                    
                    business_name = ""
                    for pattern in name_patterns:
                        matches = re.findall(pattern, block, re.IGNORECASE)
                        if matches:
                            business_name = re.sub(r'<[^>]+>', '', matches[0]).strip()
                            break
                    
                    if business_name:
                        contact["name"] = business_name
                        contact["organization"] = business_name
                    
                    # Extract phone numbers
                    phone_patterns = [
                        r'\(\d{3}\)\s*\d{3}-\d{4}',
                        r'\d{3}-\d{3}-\d{4}',
                        r'\+\d{1,3}\s*\(\d{3}\)\s*\d{3}-\d{4}'
                    ]
                    
                    phones_found = []
                    for pattern in phone_patterns:
                        matches = re.findall(pattern, block)
                        phones_found.extend(matches)
                    
                    if phones_found:
                        contact["phone"] = list(set(phones_found))  # Remove duplicates
                    
                    # Extract addresses
                    address_patterns = [
                        r'<span[^>]*class="[^"]*address[^"]*"[^>]*>(.*?)</span>',
                        r'<p[^>]*class="[^"]*address[^"]*"[^>]*>(.*?)</p>',
                        r'<div[^>]*class="[^"]*address[^"]*"[^>]*>(.*?)</div>'
                    ]
                    
                    address = ""
                    for pattern in address_patterns:
                        matches = re.findall(pattern, block, re.IGNORECASE | re.DOTALL)
                        if matches:
                            address = re.sub(r'<[^>]+>', '', matches[0]).strip()
                            break
                    
                    if address:
                        contact["address"] = address
                    
                    # Extract categories/services
                    category_patterns = [
                        r'<span[^>]*class="[^"]*categories[^"]*"[^>]*>(.*?)</span>',
                        r'<div[^>]*class="[^"]*categories[^"]*"[^>]*>(.*?)</div>'
                    ]
                    
                    category = ""
                    for pattern in category_patterns:
                        matches = re.findall(pattern, block, re.IGNORECASE | re.DOTALL)
                        if matches:
                            category = re.sub(r'<[^>]+>', '', matches[0]).strip()
                            break
                    
                    if category:
                        contact["industry"] = category
                        contact["notes"] = f"Category: {category}"
                    
                    # Only include contacts that have at least a name and some contact info
                    if contact["name"] and (contact["phone"] or contact["email"] or contact["websites"]):
                        # Enhance with additional contact extraction
                        enhanced_contact = await enhance_contact_details(self.page, contact)
                        contacts.append(enhanced_contact)
                        
                except Exception as e:
                    logger.warning(f"Error extracting contact from block {i}: {e}")
                    continue
            
            logger.info(f"Successfully extracted {len(contacts)} contacts from YellowPages search results")
            return contacts
            
        except Exception as e:
            logger.warning(f"Error extracting contacts from YellowPages search results: {e}")
            return []

    async def _scrape_yellowpages_individual_pages(self) -> List[Dict[str, Any]]:
        """Fallback method to visit individual YellowPages business pages."""
        try:
            # Extract business URLs from search results
            business_urls = await self._extract_yellowpages_business_urls()
            if not business_urls:
                logger.warning("No business URLs found on YellowPages search results")
                return []

            logger.info(f"Found {len(business_urls)} business URLs to visit")

            all_contacts = []
            visited_count = 0
            max_businesses = min(20, len(business_urls))  # Limit to 20 businesses

            for business_url in business_urls[:max_businesses]:
                try:
                    visited_count += 1
                    logger.info(f"Visiting business page {visited_count}/{max_businesses}: {business_url}")

                    # Navigate to business page
                    await self.page.goto(business_url, wait_until="domcontentloaded")
                    await asyncio.sleep(random.uniform(2, 4))  # Wait for page to load

                    # Extract contact information from business page
                    contact_data = await self._extract_yellowpages_business_contact()
                    if contact_data:
                        all_contacts.append(contact_data)
                        logger.info("✅ Extracted contact data from business page")
                    else:
                        logger.debug(f"❌ No contact data found on business page: {business_url}")

                    # Add delay between requests to be respectful
                    if visited_count < max_businesses:
                        await asyncio.sleep(random.uniform(3, 6))

                except Exception as e:
                    logger.warning(f"Failed to extract from business page {business_url}: {e}")
                    continue

            logger.info(f"✅ Completed YellowPages extraction: {len(all_contacts)} contacts from {visited_count} business pages")
            return all_contacts

        except Exception as e:
            logger.error(f"Failed to scrape YellowPages individual pages: {e}")
            return []

        if not business_urls:
            logger.info("No business URLs found, falling back to generic extraction")
            return await self._scrape_generic_page()

        # Visit each business page and extract contact information
        all_contacts = []
        visited_count = 0
        max_businesses = min(20, len(business_urls))  # Limit to 20 businesses to avoid being too aggressive

        for business_url in business_urls[:max_businesses]:
            try:
                logger.info(f"Visiting business page {visited_count + 1}/{max_businesses}: {business_url}")

                # Navigate to business page
                await self.page.goto(business_url, wait_until="domcontentloaded")
                await asyncio.sleep(random.uniform(2, 4))  # Wait for page to load

                # Extract contact information from business page
                contact_data = await self._extract_yellowpages_business_contact()
                if contact_data:
                    all_contacts.append(contact_data)
                    logger.info(f"✅ Extracted contact data from business page")
                else:
                    logger.debug(f"❌ No contact data found on business page: {business_url}")

                visited_count += 1

                # Add delay between requests to be respectful
                if visited_count < max_businesses:
                    await asyncio.sleep(random.uniform(3, 6))

            except Exception as e:
                logger.warning(f"Failed to extract from business page {business_url}: {e}")
                continue

        logger.info(f"✅ Completed YellowPages extraction: {len(all_contacts)} contacts from {visited_count} business pages")
        return all_contacts

    async def _perform_yellowpages_search(self, search_query: str) -> bool:
        """Perform search on YellowPages search page."""
        try:
            # Find search input
            search_input_selectors = ['input[name="search_terms"]', 'input#query', 'input[placeholder*="search" i]']
            search_input = None
            for selector in search_input_selectors:
                search_input = await self.page.query_selector(selector)
                if search_input:
                    break

            if not search_input:
                logger.error("Could not find YellowPages search input")
                return False

            # Fill search query
            await search_input.fill(search_query)

            # Find and click search button
            search_button_selectors = ['button[type="submit"]', 'input[type="submit"]', '.search-btn']
            search_button = None
            for selector in search_button_selectors:
                search_button = await self.page.query_selector(selector)
                if search_button:
                    break

            if not search_button:
                logger.error("Could not find YellowPages search button")
                return False

            # Click search button
            await search_button.click()
            await self.page.wait_for_load_state("networkidle")
            await asyncio.sleep(2)

            logger.info(f"Successfully performed YellowPages search for: {search_query}")
            return True

        except Exception as e:
            logger.error(f"Failed to perform YellowPages search: {e}")
            return False

    async def _extract_yellowpages_business_urls(self) -> List[str]:
        """Extract business profile URLs from YellowPages search results."""
        business_urls = []

        try:
            # YellowPages business links are typically in result listings
            business_selectors = [
                '.result a.business-name',
                '.listing a.business-name',
                '.search-results a[href*="/"]',
                '.result-item a[href*="/"]',
                'h2 a[href*="/"]',
                '.business-card a[href*="/"]'
            ]

            for selector in business_selectors:
                try:
                    links = await self.page.query_selector_all(selector)
                    for link in links:
                        href = await link.get_attribute('href')
                        if href:
                            # Convert relative URLs to absolute
                            if href.startswith('/'):
                                href = f"https://www.yellowpages.com{href}"
                            elif not href.startswith('http'):
                                continue

                            # Avoid duplicate URLs and non-business pages
                            if href not in business_urls and '/search' not in href and '/advertise' not in href:
                                business_urls.append(href)
                except Exception:
                    continue

            # Limit to reasonable number
            business_urls = business_urls[:50]

        except Exception as e:
            logger.warning(f"Failed to extract business URLs: {e}")

        return business_urls

    async def _extract_yellowpages_business_contact(self) -> Optional[Dict[str, Any]]:
        """Extract contact information from YellowPages business page."""
        try:
            # Extract from page content using AI
            html_content = await self.page.content()

            prompt = f"""Extract comprehensive contact information from this YellowPages business page.

Page Content (first 10000 characters):
{html_content[:10000]}

This is a YellowPages business listing page with local business information. Extract ALL available contact details:

CRITICAL: Look for business information sections, contact details, and company profiles.
- Business name (exact registered name)
- Contact person name and title
- ALL phone numbers (main, cell, fax, multiple locations)
- ALL email addresses (primary, contact, info emails)
- Business website URL
- Complete business address (street, city, state, zip)
- Social media profiles (Facebook, Twitter, Instagram, LinkedIn)
- Business categories and services

Search for these specific patterns:
- Emails: @business.com, contact@, info@, sales@, support@
- Phones: +1-, 1-, area codes, local numbers, cell phones, fax numbers
- Websites: https://www., http://, business domains
- Social: facebook.com/business, twitter.com/, instagram.com/, linkedin.com/
- Addresses: street addresses, suite numbers, city/state/zip codes

Return comprehensive JSON with ALL found information:
{{
  "name": "Exact Business Name",
  "email": ["contact@business.com", "info@business.com"],
  "phone": ["+1-555-123-4567", "+1-555-123-4568", "+1-555-123-4569"],
  "organization": "Exact Business Name",
  "role": "Owner/Manager/Contact Person Name",
  "confidence": 0.9,
  "source": "YellowPages Business Page",
  "notes": "Complete business description including services, hours, specialties",
  "lead_category": "Local Business",
  "lead_sub_category": "Retail/Service/Professional",
  "bio": "Detailed business description, years in business, specialties, certifications",
  "industry": "Primary business category (Retail/Restaurant/Service/Professional)",
  "company_type": "Local Business/Franchise/Independent",
  "location": "City, State, Country",
  "employee_count": null,
  "revenue": null,
  "websites": ["https://www.businesswebsite.com"],
  "social_media": {{
    "linkedin": null,
    "twitter": "https://twitter.com/businesshandle",
    "facebook": "https://www.facebook.com/businessname",
    "instagram": "https://www.instagram.com/businesshandle",
    "youtube": null,
    "other": []
  }},
  "address": "Complete street address, City, State ZIP, Country",
  "status": "active",
  "incorporation_date": null,
  "jurisdiction": null
}}

IMPORTANT: Extract REAL contact details visible on the page. Do not return empty email/phone arrays unless NO contact information exists on the page."""

            ai_result = _ai_generate_json(prompt)
            if ai_result and ai_result.get('name'):
                # Enhance with direct contact extraction
                enhanced_result = await enhance_contact_details(self.page, ai_result)
                return enhanced_result

        except Exception as e:
            logger.warning(f"Failed to extract contact data from YellowPages business page: {e}")

        return None

    async def _extract_crunchbase_company_contact(self) -> Optional[Dict[str, Any]]:
        """Extract contact information from Crunchbase company page."""
        try:
            # Extract from page content using AI
            html_content = await self.page.content()

            prompt = f"""Extract comprehensive contact information from this Crunchbase company page.

Page Content (first 10000 characters):
{html_content[:10000]}

This is a Crunchbase company profile page with startup/company information. Extract ALL available contact details:

CRITICAL: Look for company information sections, contact details, and business profiles.
- Company name (exact legal name)
- Contact person name and title (founders, executives)
- ALL phone numbers (main, office, multiple locations)
- ALL email addresses (primary, contact, info, press emails)
- Company website URL
- Complete business address (headquarters, offices)
- Social media profiles (LinkedIn, Twitter, Facebook, Instagram, YouTube)
- Industry categories and business focus

Search for these specific patterns:
- Emails: @company.com, contact@, info@, press@, hello@, support@
- Phones: +1-, 1-, area codes, office numbers, international numbers
- Websites: https://www., http://, company domains
- Social: linkedin.com/company, twitter.com/, facebook.com/, instagram.com/, youtube.com/
- Addresses: street addresses, city/state/zip codes, country locations

Return comprehensive JSON with ALL found information:
{{
  "name": "Exact Company Legal Name",
  "email": ["contact@company.com", "info@company.com", "press@company.com"],
  "phone": ["+1-555-123-4567", "+1-555-123-4568"],
  "organization": "Exact Company Legal Name",
  "role": "Founder/CEO/CTO - Executive Name",
  "confidence": 0.9,
  "source": "Crunchbase Company Page",
  "notes": "Complete company profile including description, funding, products, team size",
  "lead_category": "B2B",
  "lead_sub_category": "Technology/Startup/SaaS",
  "bio": "Detailed company description, mission, products, funding rounds, key metrics",
  "industry": "Primary industry sector (Technology/Fintech/Healthtech/E-commerce)",
  "company_type": "Startup/Private/Public Company",
  "location": "Headquarters City, State, Country",
  "employee_count": "number of employees (from Crunchbase data)",
  "revenue": "funding amount or revenue if mentioned",
  "websites": ["https://www.companywebsite.com"],
  "social_media": {{
    "linkedin": "https://www.linkedin.com/company/companyname",
    "twitter": "https://twitter.com/companyhandle",
    "facebook": "https://www.facebook.com/companyname",
    "instagram": "https://www.instagram.com/companyhandle",
    "youtube": "https://www.youtube.com/channel/companychannel",
    "other": []
  }},
  "address": "Headquarters street address, City, State ZIP, Country",
  "status": "active",
  "incorporation_date": "founding year if available",
  "jurisdiction": "incorporation state/country"
}}

IMPORTANT: Extract REAL contact details visible on the page. Do not return empty email/phone arrays unless NO contact information exists on the page."""

            ai_result = _ai_generate_json(prompt)
            if ai_result and ai_result.get('name'):
                # Enhance with direct contact extraction
                enhanced_result = await enhance_contact_details(self.page, ai_result)
                return enhanced_result

        except Exception as e:
            logger.warning(f"Failed to extract contact data from Crunchbase company page: {e}")

        return None
