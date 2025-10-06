import asyncio
from crawl4ai import AsyncWebCrawler
import json
import google.generativeai as genai
from json_repair import repair_json
from typing import List, Dict, Any, Optional
import re
import sys
import time
from datetime import datetime, timezone
import pandas as pd
import os
import urllib.parse
import asyncio
from concurrent.futures import ThreadPoolExecutor

# Add browser automation for Cloudflare handling
try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("⚠️  Playwright not available - Cloudflare handling will be limited")
    print("💡 To enable full Cloudflare bypass, install Playwright:")
    print("   pip install playwright")
    print("   playwright install chromium")

def setup_cloudflare_bypass():
    """
    Setup instructions for Cloudflare bypass functionality
    """
    if not PLAYWRIGHT_AVAILABLE:
        print("\n🔧 Cloudflare Bypass Setup Required:")
        print("1. Install Playwright: pip install playwright")
        print("2. Install browser: playwright install chromium")
        print("3. Restart the application")
        print("\nThis will enable automatic handling of Cloudflare challenges!")
        return False
    return True

def install_cloudflare_bypass():
    """
    Install Playwright for full Cloudflare bypass functionality
    """
    import subprocess
    import sys

    print("� Installing Playwright for Cloudflare bypass...")

    try:
        # Install Playwright
        subprocess.check_call([sys.executable, "-m", "pip", "install", "playwright"])
        print("✅ Playwright installed successfully")

        # Install Chromium browser
        subprocess.check_call([sys.executable, "-m", "playwright", "install", "chromium"])
        print("✅ Chromium browser installed successfully")

        print("🎉 Cloudflare bypass is now fully configured!")
        print("💡 Restart the application to use the enhanced Cloudflare handling")

    except subprocess.CalledProcessError as e:
        print(f"❌ Installation failed: {e}")
        print("💡 Try manual installation:")
        print("   pip install playwright")
        print("   playwright install chromium")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

# Add current directory to path for imports (prioritize local paths)
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, current_dir)  # Insert at beginning to prioritize local
sys.path.insert(0, parent_dir)   # Insert parent directory too
# Removed problematic path append that was causing MongoDB manager conflicts

from database.mongodb_manager import get_mongodb_manager
GOOGLE_API_KEY='AIzaSyD-Gsdh5u9JamamQdzH-pIi-5q78GOxWV4'

# Google Custom Search API Configuration (Optional)
# To set up Google Custom Search API:
# 1. Go to https://console.developers.google.com/
# 2. Create a new project or select existing one
# 3. Enable the "Custom Search JSON API"
# 4. Create credentials (API Key)
# 5. Go to https://cse.google.com/ and create a Custom Search Engine
# 6. Get the Search Engine ID (cx) from the CSE control panel
# 7. Set the variables below:
GOOGLE_CUSTOM_SEARCH_API_KEY = 'AIzaSyBm_W6jg0vlgFf00pyyYZbzdokQneCwvMw'  # Your API key
GOOGLE_CUSTOM_SEARCH_CX = '329ec506fbb334a8f'  # Your Search Engine ID

# Example:
# GOOGLE_CUSTOM_SEARCH_API_KEY = 'AIzaSyD-Your-API-Key-Here'
# GOOGLE_CUSTOM_SEARCH_CX = '017576662512468239146:omuauf_lfve'

# Configure Google GenAI clients
genai.configure(api_key=GOOGLE_API_KEY)

# Try to import alternative GenAI client
try:
    from google import genai as genai_alt
    genai_alt_client = genai_alt.Client(api_key=GOOGLE_API_KEY)
    USE_ALT_GENAI = True
    print("Alternative GenAI client available")
except ImportError:
    USE_ALT_GENAI = False
    print("Using standard GenAI client")

def generate_content(model: str, contents: str):
    """Generate content using the configured GenAI client"""
    if USE_ALT_GENAI:
        return genai_alt_client.models.generate_content(model=model, contents=contents)
    else:
        return genai.GenerativeModel(model).generate_content(contents=contents)

async def crawl_with_cloudflare_handling(url: str, max_retries: int = 3) -> Dict[str, Any]:
    """
    Enhanced crawler with Cloudflare handling capabilities
    Uses multiple strategies to bypass Cloudflare protection
    """
    print(f"🔒 Attempting to crawl Cloudflare-protected URL: {url}")

    # Strategy 1: Enhanced Crawl4AI with browser-like configuration
    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1}/{max_retries} - Using enhanced Crawl4AI configuration")

            async with AsyncWebCrawler() as crawler:
                # Enhanced configuration for Cloudflare bypass
                config = {
                    "headers": {
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                        "Accept-Language": "en-US,en;q=0.9",
                        "Accept-Encoding": "gzip, deflate, br",
                        "DNT": "1",
                        "Connection": "keep-alive",
                        "Upgrade-Insecure-Requests": "1",
                        "Sec-Fetch-Dest": "document",
                        "Sec-Fetch-Mode": "navigate",
                        "Sec-Fetch-Site": "none",
                        "Sec-Fetch-User": "?1",
                        "Cache-Control": "max-age=0",
                        "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                        "Sec-Ch-Ua-Mobile": "?0",
                        "Sec-Ch-Ua-Platform": '"macOS"'
                    },
                    "timeout": 30,  # Increased timeout for Cloudflare challenges
                    "follow_redirects": True,
                    "wait_for": "body",  # Wait for body to load
                    "js_wait": 3000,  # Wait for JavaScript execution
                    "css_selector": None,
                    "only_text": False,
                    "remove_overlay_elements": True,
                    "simulate_user": True,  # Enable user simulation
                    "override_navigator": True,  # Override navigator properties
                    "magic": True  # Enable magic mode for better JS handling
                }

                result = await crawler.arun(url=url, **config)

                # Check if we got a Cloudflare challenge page
                if result and result.markdown:
                    content_lower = result.markdown.lower()
                    if any(indicator in content_lower for indicator in [
                        'cloudflare', 'cf-browser-verification', 'cf-challenge',
                        'checking your browser', 'ddos protection', 'cf-ray'
                    ]):
                        print(f"⚠️  Cloudflare challenge detected on attempt {attempt + 1}")
                        if attempt < max_retries - 1:
                            wait_time = (attempt + 1) * 10  # Progressive backoff
                            print(f"⏳ Waiting {wait_time} seconds before retry...")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            print("❌ All attempts failed - Cloudflare protection too strong")
                            return {"error": "Cloudflare protection", "content": "", "links": []}

                    # Success! We got past Cloudflare
                    print(f"✅ Successfully bypassed Cloudflare on attempt {attempt + 1}")
                    return {
                        "content": result.markdown,
                        "links": result.links,
                        "success": True
                    }
                else:
                    print(f"⚠️  No content received on attempt {attempt + 1}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(5)
                        continue

        except Exception as e:
            print(f"❌ Crawl4AI attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(3)
                continue

    # Strategy 2: Fallback with minimal headers (if Playwright available)
    if PLAYWRIGHT_AVAILABLE:
        try:
            print("🎭 Trying Playwright fallback for Cloudflare bypass...")
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-accelerated-2d-canvas',
                        '--no-first-run',
                        '--no-zygote',
                        '--disable-gpu'
                    ]
                )

                context = await browser.new_context(
                    user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    viewport={'width': 1920, 'height': 1080}
                )

                page = await context.new_page()

                try:
                    # Navigate and wait for potential Cloudflare challenge
                    await page.goto(url, wait_until='networkidle', timeout=60000)

                    # Wait for Cloudflare challenge to complete (if present)
                    try:
                        await page.wait_for_selector('.cf-browser-verification', timeout=10000)
                        print("⏳ Cloudflare challenge detected, waiting for completion...")
                        await page.wait_for_selector('.cf-browser-verification', state='hidden', timeout=30000)
                        print("✅ Cloudflare challenge completed")
                    except:
                        pass  # No Cloudflare challenge or already passed

                    # Additional wait for dynamic content
                    await page.wait_for_load_state('networkidle')
                    await asyncio.sleep(3)

                    content = await page.content()
                    links = []

                    # Extract links
                    link_elements = await page.query_selector_all('a[href]')
                    for link in link_elements:
                        href = await link.get_attribute('href')
                        if href and href.startswith(('http://', 'https://')):
                            links.append({'href': href})

                    await browser.close()

                    print("✅ Playwright successfully retrieved content")
                    return {
                        "content": content,
                        "links": {"internal": [], "external": links},
                        "success": True
                    }

                except Exception as e:
                    print(f"❌ Playwright failed: {e}")
                    await browser.close()
                    return {"error": str(e), "content": "", "links": []}

        except Exception as e:
            print(f"❌ Playwright setup failed: {e}")

    print("❌ All Cloudflare bypass attempts failed")
    return {"error": "All bypass methods failed", "content": "", "links": []}

def search_google_custom_api(query: str, api_key: str = None, cx: str = None, num_results: int = 10) -> List[Dict[str, str]]:
    """Use Google Custom Search API to get search results"""
    if not api_key or not cx:
        print("Google Custom Search API key or Search Engine ID not configured")
        return []

    try:
        import requests

        url = "https://www.googleapis.com/customsearch/v1"
        params = {
            'key': api_key,
            'cx': cx,
            'q': query,
            'num': min(num_results, 10),  # API limit is 10 per request
            'start': 1
        }

        print(f"Making Google Custom Search API request for: {query}")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        links = []

        for item in data.get('items', []):
            links.append({
                'href': item.get('link', ''),
                'title': item.get('title', ''),
                'snippet': item.get('snippet', '')
            })

        print(f"Found {len(links)} results from Google Custom Search API")
        return links

    except Exception as e:
        print(f"Error with Google Custom Search API: {e}")
        return []

def format_json_llm(text):
    regex_pattern = r'```json(.*?)```'
    match = re.findall(regex_pattern,text, flags=re.S)

    if match:
        repaired_json = repair_json(match[0])
        res = json.loads(repaired_json)
        return res
    else:
        try:
            return json.loads(text)
        except Exception as e:
            return None

def convert_to_unified_format(lead_data):
    """Convert the extracted lead data to the unified format"""
    
    def extract_emails(data):
        """Extract emails from contact_info"""
        contact_info = data.get('contact_info', {})
        emails = []
        if contact_info.get('email') and contact_info.get('email') != 'NA':
            emails.append(contact_info.get('email'))
        return emails
    
    def extract_phones(data):
        """Extract phone numbers from contact_info"""
        contact_info = data.get('contact_info', {})
        phones = []
        if contact_info.get('phone') and contact_info.get('phone') != 'NA':
            phones.append(contact_info.get('phone'))
        return phones
    
    def extract_social_media(data):
        """Extract social media handles"""
        contact_info = data.get('contact_info', {})
        social_media = {}
        
        social_media['linkedin'] = contact_info.get('linkedin') if contact_info.get('linkedin') != 'NA' else None
        social_media['twitter'] = contact_info.get('twitter') if contact_info.get('twitter') != 'NA' else None
        social_media['facebook'] = contact_info.get('facebook') if contact_info.get('facebook') != 'NA' else None
        social_media['instagram'] = None
        social_media['youtube'] = None
        social_media['tiktok'] = None
        
        # Handle other social media links
        other_links = []
        socialmedialinks = contact_info.get('socialmedialinks', [])
        if isinstance(socialmedialinks, list):
            other_links.extend([link for link in socialmedialinks if link])
        
        return social_media, other_links
    
    def get_websites(data):
        """Extract website URLs"""
        websites = []
        contact_info = data.get('contact_info', {})
        
        if data.get('source_url'):
            websites.append(data.get('source_url'))
        
        if contact_info.get('website') and contact_info.get('website') != 'NA':
            websites.append(contact_info.get('website'))
            
        return websites
    
    # Extract social media data
    social_media, other_social_links = extract_social_media(lead_data)
    
    # Determine lead category based on type
    lead_category = "potential_customer" if lead_data.get('type', '').lower() == 'lead' else "competitor"
    lead_sub_category = lead_data.get('lead_sub_category', '')
    
    # Create current timestamp as ISO string
    current_time = datetime.now(timezone.utc).isoformat()

    unified_data = {
        "url": lead_data.get('source_url', ''),
        "platform": "web",
        "content_type": lead_data.get('content_type', ''),
        "source": "web-scraper",
        "profile": {
            "username": "",
            "full_name": lead_data.get('name', '') or lead_data.get('company_name', ''),
            "bio": lead_data.get('bio', ''),
            "location": lead_data.get('location', ''),
            "job_title": "",
            "employee_count": ""
        },
        "contact": {
            "emails": extract_emails(lead_data),
            "phone_numbers": extract_phones(lead_data),
            "address": lead_data.get('address', ''),
            "websites": get_websites(lead_data),
            "social_media_handles": {
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
            "scraped_at": current_time,
            "data_quality_score": "0.45"
        },
        "industry": lead_data.get('industry', ''),
        "revenue": "",
        "lead_category": lead_category,
        "lead_sub_category": lead_sub_category,
        "company_name": lead_data.get('company_name', ''),
        "company_type": lead_data.get('company_type', ''),
        "decision_makers": lead_data.get('name', ''),
        "bdr": "AKG",
        "product_interests": "",
        "timeline": "",
        "interest_level": ""
    }
    
    return unified_data

def check_lead_duplication(lead_data: Dict[str, Any], existing_leads: List[Dict[str, Any]]) -> bool:
    """
    Check if a lead is a duplicate based on the specified criteria:
    1. Email is different (if present)
    2. Phone is different (if present) 
    3. If both email and phone are empty, check full_name + url + company_name + company_type
    """
    lead_emails = lead_data.get('contact', {}).get('emails', [])
    lead_phones = lead_data.get('contact', {}).get('phone_numbers', [])
    lead_full_name = lead_data.get('profile', {}).get('full_name', '')
    lead_url = lead_data.get('url', '')
    lead_company_name = lead_data.get('company_name', '')
    lead_company_type = lead_data.get('company_type', '')
    
    # Normalize email and phone lists
    lead_emails = [email.lower().strip() for email in lead_emails if email and email.strip()]
    lead_phones = [phone.strip() for phone in lead_phones if phone and phone.strip()]
    
    for existing_lead in existing_leads:
        existing_emails = existing_lead.get('contact', {}).get('emails', [])
        existing_phones = existing_lead.get('contact', {}).get('phone_numbers', [])
        existing_full_name = existing_lead.get('profile', {}).get('full_name', '')
        existing_url = existing_lead.get('url', '')
        existing_company_name = existing_lead.get('company_name', '')
        existing_company_type = existing_lead.get('company_type', '')
        
        # Normalize existing email and phone lists
        existing_emails = [email.lower().strip() for email in existing_emails if email and email.strip()]
        existing_phones = [phone.strip() for phone in existing_phones if phone and phone.strip()]
        
        # Check email duplication
        if lead_emails and existing_emails:
            if any(email in existing_emails for email in lead_emails):
                return True
        
        # Check phone duplication
        if lead_phones and existing_phones:
            if any(phone in existing_phones for phone in lead_phones):
                return True
        
        # If both email and phone are empty, check other fields
        if not lead_emails and not lead_phones and not existing_emails and not existing_phones:
            if (lead_full_name.lower().strip() == existing_full_name.lower().strip() and
                lead_url.strip() == existing_url.strip() and
                lead_company_name.lower().strip() == existing_company_name.lower().strip() and
                lead_company_type.lower().strip() == existing_company_type.lower().strip()):
                return True
    
    return False

def store_unified_leads(leads: List[Dict[str, Any]], mongodb_manager, icp_identifier: str = 'default', export_csv: bool = False, csv_filename: str = 'leads_export.csv') -> Dict[str, Any]:
    """
    Store leads in unified_leads collection with duplication checking
    Optionally export to CSV file
    """
    if not leads:
        return {"stored": 0, "duplicates": 0, "errors": 0}
    
    try:
        # Get existing leads from unified_leads collection
        existing_leads = list(mongodb_manager.get_collection('unified_leads').find({}))
        
        stored_count = 0
        duplicate_count = 0
        error_count = 0
        
        for lead in leads:
            try:
                # Check for duplication
                if check_lead_duplication(lead, existing_leads):
                    duplicate_count += 1
                    continue
                
                # Add metadata with proper datetime handling
                current_time = datetime.now(timezone.utc)
                lead['created_at'] = current_time.isoformat()  # Convert to ISO string
                lead['source'] = 'web_crawler'
                lead['icp_identifier'] = icp_identifier
                
                # Ensure metadata.scraped_at is also ISO string if it exists
                if 'metadata' in lead and 'scraped_at' in lead['metadata']:
                    if isinstance(lead['metadata']['scraped_at'], str):
                        # Already a string, keep as is
                        pass
                    else:
                        # Convert datetime to ISO string
                        lead['metadata']['scraped_at'] = current_time.isoformat()
                
                # Store in unified_leads collection
                result = mongodb_manager.get_collection('unified_leads').insert_one(lead)
                if result.inserted_id:
                    stored_count += 1
                    # Add to existing_leads list to check against future leads
                    existing_leads.append(lead)
                else:
                    print(f"Failed to insert lead: {lead.get('url', 'Unknown URL')}")
                    error_count += 1
                    
            except Exception as e:
                print(f"Error storing individual lead: {e}")
                print(f"Lead data: {lead.get('url', 'Unknown URL')}")
                error_count += 1
        
        print(f"Unified leads storage complete: {stored_count} stored, {duplicate_count} duplicates, {error_count} errors")
        
        # # Export to CSV if requested
        # if export_csv and stored_count > 0:
        #     try:
        #         # Get only the successfully stored leads for CSV export
        #         successfully_stored_leads = []
        #         for lead in leads:
        #             if not check_lead_duplication(lead, existing_leads[:-stored_count]):  # Exclude recently added leads
        #                 successfully_stored_leads.append(lead)
                
        #         # Flatten the leads for CSV export
        #         flattened_leads = []
        #         for lead in successfully_stored_leads[:stored_count]:  # Only export stored leads
        #             flat_lead = {
        #                 'url': lead.get('url', ''),
        #                 'platform': lead.get('platform', ''),
        #                 'full_name': lead.get('profile', {}).get('full_name', ''),
        #                 'bio': lead.get('profile', {}).get('bio', ''),
        #                 'location': lead.get('profile', {}).get('location', ''),
        #                 'job_title': lead.get('profile', {}).get('job_title', ''),
        #                 'emails': ', '.join(lead.get('contact', {}).get('emails', [])),
        #                 'phone_numbers': ', '.join(lead.get('contact', {}).get('phone_numbers', [])),
        #                 'address': lead.get('contact', {}).get('address', ''),
        #                 'websites': ', '.join(lead.get('contact', {}).get('websites', [])),
        #                 'linkedin': lead.get('contact', {}).get('social_media_handles', {}).get('linkedin', ''),
        #                 'twitter': lead.get('contact', {}).get('social_media_handles', {}).get('twitter', ''),
        #                 'facebook': lead.get('contact', {}).get('social_media_handles', {}).get('facebook', ''),
        #                 'instagram': lead.get('contact', {}).get('social_media_handles', {}).get('instagram', ''),
        #                 'youtube': lead.get('contact', {}).get('social_media_handles', {}).get('youtube', ''),
        #                 'tiktok': lead.get('contact', {}).get('social_media_handles', {}).get('tiktok', ''),
        #                 'industry': lead.get('industry', ''),
        #                 'company_name': lead.get('company_name', ''),
        #                 'company_type': lead.get('company_type', ''),
        #                 'lead_category': lead.get('lead_category', ''),
        #                 'lead_sub_category': lead.get('lead_sub_category', ''),
        #                 'decision_makers': lead.get('decision_makers', ''),
        #                 'bdr': lead.get('bdr', ''),
        #                 'scraped_at': lead.get('metadata', {}).get('scraped_at', ''),
        #                 'data_quality_score': lead.get('metadata', {}).get('data_quality_score', ''),
        #                 'icp_identifier': icp_identifier,
        #                 'created_at': lead.get('created_at', '')
        #             }
        #             flattened_leads.append(flat_lead)
                
        #         if flattened_leads:
        #             df = pd.DataFrame(flattened_leads)
        #             df.to_csv(csv_filename, index=False)
        #             print(f"Leads exported to {csv_filename}")
                
        #     except Exception as e:
        #         print(f"Error exporting to CSV: {e}")
        
        return {
            "stored": stored_count,
            "duplicates": duplicate_count,
            "errors": error_count,
            "total_processed": len(leads)
        }
        
    except Exception as e:
        print(f"Error in store_unified_leads: {e}")
        import traceback
        traceback.print_exc()  # Print full traceback for debugging
        return {"stored": 0, "duplicates": 0, "errors": len(leads)}

async def process_urls_concurrently(links, max_concurrent=3):
    """Process multiple URLs concurrently with controlled concurrency"""
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def process_single_url(link):
        async with semaphore:
            href = link['href']

            if should_skip_url(href):
                print(f"Skipping irrelevant/social/junk URL: {href}")
                return None    
            
            lead_json_format = {
                "name": "",
                "contact_info": {
                    "email": "",
                    "phone": "",
                    "linkedin": "",
                    "twitter": "",
                    "website": "",
                    "others": "",
                    "socialmedialinks": []
                },
                "company_name": "",
                "time": "",
                "link_details": "provide a short description of the link",
                "type": "provide whether its a lead/competitor",
                "lead_sub_category": "",
                "what_we_can_offer": "",
                "source_url": "",
                "source_platform": "",
                "location": "",
                "industry": "",
                "content_type": "",
                "company_type": "",
                "bio": "",
                "address": ""
            }
            
            try:
                # Use Cloudflare-aware crawler for individual websites
                crawl_result = await crawl_with_cloudflare_handling(link['href'])

                if crawl_result.get('success'):
                    result = crawl_result['content']
                    truncated_result = result[:4000] if result else ""

                    model = "gemini-2.5-flash"
                    content = f'''From this profile/website extract important information for lead generation purposes. Focus on finding potential customers, not competitors. Include phone numbers and email addresses if found. Identify the source URL and the platform from which the information was extracted.

                                Profile/Website Content: {truncated_result}

                                Extract the information in the following json format and if any information is not present, leave the field empty. Also extract location, industry, company_type, bio, and address if available.

                                {json.dumps(lead_json_format)}

                                IMPORTANT: Only extract information if this appears to be a potential customer/lead. Return an empty dictionary if:
                                - This is a competitor or service provider in the same industry
                                - No contact information is available
                                - The content is not relevant to lead generation
                                '''

                    response = generate_content(model, content)
                    res = format_json_llm(response.text)

                    if res and res != {}:
                        res['source_url'] = href
                        # Since we filter out social media, all remaining links are websites
                        res['source_platform'] = 'Website'
                        return res
                        
            except Exception as e:
                print(f"Error processing {href}: {e}")
            return None
        
    # Process URLs concurrently
    tasks = [process_single_url(link) for link in links]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Filter out None results and exceptions
    final_results = []
    for result in results:
        if result and not isinstance(result, Exception):
            final_results.append(result)
    
    return final_results

async def main_google_search(google_search_url, use_api_fallback: bool = True):
    """Modified main function to handle Google search results with API fallback"""
    from urllib.parse import urlparse, parse_qs

    # Extract search query from URL
    parsed_url = urlparse(google_search_url)
    query_params = parse_qs(parsed_url.query)
    search_query = query_params.get('q', [''])[0]

    all_links = []

    # Try Google Custom Search API first if configured
    if use_api_fallback and GOOGLE_CUSTOM_SEARCH_API_KEY and GOOGLE_CUSTOM_SEARCH_CX:
        print(f"Trying Google Custom Search API for query: {search_query}")
        api_links = search_google_custom_api(
            search_query,
            GOOGLE_CUSTOM_SEARCH_API_KEY,
            GOOGLE_CUSTOM_SEARCH_CX,
            num_results=10  # Increased from default for more links
        )

        if api_links:
            # Convert API results to expected format
            for link in api_links:
                all_links.append({'href': link['href']})
            print(f"Using {len(all_links)} links from Google Custom Search API")
        else:
            print("Google Custom Search API failed, falling back to web crawling")

    # If API didn't work or isn't configured, try web crawling
    if not all_links:
        async with AsyncWebCrawler() as crawler:
            print(f"Accessing Google search URL: {google_search_url}")

            # Configure crawler with browser-like headers
            crawler_config = {
                "headers": {
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "DNT": "1",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Sec-Fetch-User": "?1",
                    "Cache-Control": "max-age=0",
                },
                "timeout": 30,
                "follow_redirects": True
            }

            try:
                result = await crawler.arun(url=google_search_url, **crawler_config)

                print(f"Search page loaded. Content length: {len(result.markdown) if result.markdown else 0}")

                # Try multiple methods to extract links
                external_links = result.links.get('external', [])
                all_links.extend(external_links[:10]) # Limit to 10 links

                # If no external links found, try to parse from HTML content
                # if not all_links and result.markdown:
                #     print("No external links found via crawler, trying manual parsing...")
                #     
                #     # Look for Google search result links in the HTML
                #     link_pattern = r'href="(https?://[^"]*?)"[^>]*?class="[^"]*?(?:yuRUbf|egMi0|d5oMvf)[^"]*?"'
                #     matches = re.findall(link_pattern, result.markdown, re.IGNORECASE)
                #     for match in matches[:10]:  # Limit to 10
                #         if match and not any(skip in match.lower() for skip in ['google.com', 'youtube.com', 'maps.google.com']):
                #             all_links.append({'href': match})

                await asyncio.sleep(2)  # Longer delay to avoid rate limiting

            except Exception as e:
                print(f"Error crawling Google search: {e}")
                print(f"URL attempted: {google_search_url}")
                return []

    print(f"Found {len(all_links)} links from Google search")
    if not all_links:
        print("Warning: No links extracted from Google search results")
        print("This might be due to:")
        print("1. Google blocking the crawler")
        print("2. CAPTCHA or anti-bot measures")
        print("3. Changes in Google's search result format")
        print("4. Network connectivity issues")

    final_output = await process_urls_concurrently(all_links[:10], max_concurrent=3)
        
    print(f"Total leads extracted: {len(final_output)}")
    social_count = sum(1 for lead in final_output if lead.get("type") == "social_media_profile")
    print(f"Social media leads: {social_count}")
    return final_output

def should_skip_url(url: str) -> bool:
    """Return True if the URL should be skipped as irrelevant."""
    url_lower = url.lower()

    # Skip invalid/empty URLs
    if not url or not url.startswith(("http://", "https://")):
        return True

    # Skip Google or internal redirect URLs
    skip_domains = [
        "google.com", "gstatic.com", "youtube.com", "maps.google",
        "policies.google", "support.google", "accounts.google",
        "webcache.googleusercontent.com"
    ]

    # Skip known social media (handled separately in your code)
    social_domains = [
        "facebook.com", "twitter.com", "x.com", "instagram.com",
        "linkedin.com", "tiktok.com", "reddit.com", "pinterest.com",
        "snapchat.com", "tumblr.com", "discord.com", "twitch.tv",
        "threads.net", "mastodon.social"
    ]

    # Skip random/unimportant URLs (ads, tracking, content farms, spammy)
    junk_patterns = [
        "doubleclick.net", "adservice.google", "amazon.in", "flipkart.com",
        "ebay.", "aliexpress.", "wikipedia.org", "quora.com",
        "medium.com", "wordpress.com", "blogspot.com",
        "imdb.com", "spotify.com", "apple.com"
    ]

    if any(domain in url_lower for domain in skip_domains + junk_patterns + social_domains):
        return True

    return False


def generic_web_crawl(icp_data, icp_identifier: str = 'default'):
    """Modified function using Google search approach"""
    start_time = time.time()
    
    # Extract ICP information
    product_name = icp_data["product_details"]["product_name"]
    product_category = icp_data["product_details"]["product_category"]
    usps = ", ".join(icp_data["product_details"]["usps"])
    pain_points_solved = ", ".join(icp_data["product_details"]["pain_points_solved"])
    
    target_industry = ", ".join(icp_data["icp_information"]["target_industry"])
    decision_maker_persona = ", ".join(icp_data["icp_information"]["decision_maker_persona"])
    region = ", ".join(icp_data["icp_information"]["region"])
    
    # Handle specific occasions (generic field)
    specific_occasions = icp_data["icp_information"].get("specific_occasions", 
                                                        icp_data["icp_information"].get("travel_occasions", []))
    if isinstance(specific_occasions, list):
        specific_occasions = ", ".join(specific_occasions)
    
    # Generate intent-based Google search queries
    prompt = f'''I run a {product_name} business in {product_category} and I want to find potential customers on the internet who might need my services — not competitors. 

TARGET PROFILE:
- Industries: {target_industry}
- Decision Makers: {decision_maker_persona}
- Geographic Focus: {region}
- Pain Points I Solve: {pain_points_solved}
- Key Benefits: {usps}
- Specific Use Cases: {specific_occasions}

Generate 3 Google search queries in **URL format** (https://www.google.com/search?q=...) that help me find potential customers expressing intent or need for my services.

REQUIREMENTS:
- Use double quotes for exact phrases potential customers might use
- Include location-based terms when relevant for {region}
- Focus on intent keywords (looking for, need, want, seeking, require, hiring)
- Use OR statements to combine related phrases
- Avoid competitor-focused searches
- Make queries realistic and copy-paste ready
- Generate a mix of full queries and shorter targeted queries for better results

Example formats:
Full query: https://www.google.com/search?q=%22looking+to+buy+a+flat%22+OR+%22need+apartment%22+OR+%22property+wanted%22
Short query: https://www.google.com/search?q=%22buy+flat%22+OR+%22need+apartment%22
Location query: https://www.google.com/search?q=%22looking+for+property%22+{region}

Provide the output in a json object with key "queries" and value as list of URLs.
'''

    model = "gemini-2.5-flash"
    response = generate_content(model, prompt)
    res = format_json_llm(response.text)
    
    search_queries = []
    if res and "queries" in res:
        search_queries = res["queries"]
        print(f"Generated {len(search_queries)} search queries")
    else:
        print("Error: Could not extract search queries. Using fallback.")
        # Fallback queries based on product type
        fallback_terms = product_name.replace(' ', '+').lower()
        search_queries = [
            f"https://www.google.com/search?q=%22looking+for%22+OR+%22need%22+{fallback_terms}",
            f"https://www.google.com/search?q=%22seeking%22+OR+%22require%22+{fallback_terms}"
        ]
    
    print(f"Search queries to execute: {search_queries}")
    
    final_output = []
    for query_url in search_queries:
        print(f"Executing search query: {query_url}")
        try:
            output = asyncio.run(main_google_search(query_url, use_api_fallback=True))
            final_output.extend(output)
            time.sleep(2)  # Longer delay between searches to avoid rate limiting
        except Exception as e:
            print(f"Error executing search {query_url}: {e}")
    
    # Convert to unified format and filter valid leads
    unified_output = []
    for lead in final_output:
        contact_info = lead.get("contact_info", {})
        # Enhanced validation for lead quality (including social media)
        has_contact = (contact_info.get("email") and contact_info.get("email") not in ["NA", "", "N/A"]) or \
                     (contact_info.get("phone") and contact_info.get("phone") not in ["NA", "", "N/A"]) or \
                     (contact_info.get("linkedin") and contact_info.get("linkedin") not in ["NA", "", "N/A"]) or \
                     (contact_info.get("website") and contact_info.get("website") not in ["NA", "", "N/A"]) or \
                     (contact_info.get("twitter") and contact_info.get("twitter") not in ["NA", "", "N/A"]) or \
                     (contact_info.get("facebook") and contact_info.get("facebook") not in ["NA", "", "N/A"]) or \
                     (contact_info.get("instagram") and contact_info.get("instagram") not in ["NA", "", "N/A"]) or \
                     (contact_info.get("socialmedialinks") and len(contact_info.get("socialmedialinks", [])) > 0)
        
        # For social media profiles, accept if we have URL even without company name
        is_social_media = lead.get("type") == "social_media_profile"
        has_url = lead.get("source_url") and lead.get("source_url") not in ["", "NA"]
        
        if has_contact:
            # Additional quality check - ensure lead has meaningful content OR is social media
            if (lead.get("name") and lead.get("name") not in ["", "NA"]) or \
               (lead.get("company_name") and lead.get("company_name") not in ["", "NA"]) or \
               (is_social_media and has_url):
                unified_lead = convert_to_unified_format(lead)
                unified_output.append(unified_lead)
    
    # Store leads with duplication checking
    try:
        mongodb_manager = get_mongodb_manager()
        storage_results = store_unified_leads(unified_output, mongodb_manager, icp_identifier, export_csv=True)
        print(f"Unified leads storage: {storage_results['stored']} stored, {storage_results['duplicates']} duplicates, {storage_results['errors']} errors")
    except Exception as e:
        print(f"Error storing unified leads: {e}")
        # Fallback: save to file
        try:
            with open('leads_unified_google_search.json', 'w') as f:
                json.dump(unified_output, f, indent=2)
            print("Unified leads saved to leads_unified_google_search.json (fallback)")
            
            # Also save as CSV
            if unified_output:
                flattened_leads = []
                for lead in unified_output:
                    flat_lead = {
                        'url': lead.get('url', ''),
                        'platform': lead.get('platform', ''),
                        'full_name': lead.get('profile', {}).get('full_name', ''),
                        'bio': lead.get('profile', {}).get('bio', ''),
                        'location': lead.get('profile', {}).get('location', ''),
                        'job_title': lead.get('profile', {}).get('job_title', ''),
                        'emails': ', '.join(lead.get('contact', {}).get('emails', [])),
                        'phone_numbers': ', '.join(lead.get('contact', {}).get('phone_numbers', [])),
                        'address': lead.get('contact', {}).get('address', ''),
                        'websites': ', '.join(lead.get('contact', {}).get('websites', [])),
                        'linkedin': lead.get('contact', {}).get('social_media_handles', {}).get('linkedin', ''),
                        'twitter': lead.get('contact', {}).get('social_media_handles', {}).get('twitter', ''),
                        'facebook': lead.get('contact', {}).get('social_media_handles', {}).get('facebook', ''),
                        'instagram': lead.get('contact', {}).get('social_media_handles', {}).get('instagram', ''),
                        'youtube': lead.get('contact', {}).get('social_media_handles', {}).get('youtube', ''),
                        'tiktok': lead.get('contact', {}).get('social_media_handles', {}).get('tiktok', ''),
                        'industry': lead.get('industry', ''),
                        'company_name': lead.get('company_name', ''),
                        'company_type': lead.get('company_type', ''),
                        'lead_category': lead.get('lead_category', ''),
                        'lead_sub_category': lead.get('lead_sub_category', ''),
                        'decision_makers': lead.get('decision_makers', ''),
                        'bdr': lead.get('bdr', ''),
                        'scraped_at': lead.get('metadata', {}).get('scraped_at', ''),
                        'data_quality_score': lead.get('metadata', {}).get('data_quality_score', ''),
                        'icp_identifier': icp_identifier
                    }
                    flattened_leads.append(flat_lead)
                
                df = pd.DataFrame(flattened_leads)
                df.to_csv('leads_unified_google_search.csv', index=False)
                print("Unified leads also saved to leads_unified_google_search.csv (fallback)")
        except Exception as fallback_error:
            print(f"Error in fallback file saving: {fallback_error}")
    
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")
    print(f"Total high-quality leads found: {len(unified_output)}")
    
    return unified_output

async def extract_linkedin_profile(profile_url: str) -> Dict[str, Any]:
    """Extract information from a LinkedIn profile using focused prompt"""
    async with AsyncWebCrawler() as crawler:
        try:
            result = await crawler.arun(url=profile_url)
            profile_content = result.markdown
            
            if not profile_content:
                return {}
                
            lead_json_format = {
                "name": "",
                "contact_info": {
                    "email": "",
                    "phone": "",
                    "linkedin": profile_url,
                    "twitter": "",
                    "website": "",
                    "others": "",
                    "socialmedialinks": []
                },
                "company_name": "",
                "time": "",
                "link_details": "LinkedIn profile extraction",
                "type": "lead",
                "what_we_can_offer": "",
                "source_url": profile_url,
                "source_platform": "LinkedIn",
                "location": "",
                "industry": "",
                "company_type": "",
                "bio": "",
                "address": ""
            }
            
            model = "gemini-2.5-flash"
            content = f'''From this LinkedIn profile extract important information for lead generation purposes. 
Profile Info: {profile_content[:4000]}

Extract the information in the following json format and if any information is not present fill it with NA. 
{json.dumps(lead_json_format)}

Return empty dictionary if the profile is not a potential lead or if it's a competitor.'''

            response = generate_content(model, content)
            res = format_json_llm(response.text)
            
            if res and res != {}:
                return res
            else:
                return {}
                
        except Exception as e:
            print(f"Error extracting LinkedIn profile {profile_url}: {e}")
            return {}

async def run_web_crawler_async(icp_data: Dict[str, Any], icp_identifier: str = 'default') -> Dict[str, Any]:
    """Async wrapper with Google search approach"""
    try:
        start_time = time.time()
        
        # Extract ICP information  
        product_name = icp_data["product_details"]["product_name"]
        product_category = icp_data["product_details"]["product_category"]
        usps = ", ".join(icp_data["product_details"]["usps"])
        pain_points_solved = ", ".join(icp_data["product_details"]["pain_points_solved"])
        
        target_industry = ", ".join(icp_data["icp_information"]["target_industry"])
        decision_maker_persona = ", ".join(icp_data["icp_information"]["decision_maker_persona"])
        region = ", ".join(icp_data["icp_information"]["region"])
        
        # Handle specific occasions (generic field)
        specific_occasions = icp_data["icp_information"].get("specific_occasions", 
                                                            icp_data["icp_information"].get("travel_occasions", []))
        if isinstance(specific_occasions, list):
            specific_occasions = ", ".join(specific_occasions)
        
        # Generate Google search queries
        prompt = f'''I run a {product_name} business and want to find potential customers who might need my services.

TARGET: {target_industry} companies
PAIN POINTS: {pain_points_solved}
USE CASES: {specific_occasions}

Generate 2 Google search queries in **URL format** (https://www.google.com/search?q=...) 
that help me find potential customers, prospects, or decision-makers. 

Requirements:
- Use double quotes for exact phrases potential customers might use
- Focus on intent keywords (looking for, need, want, seeking, require, hiring)
- Use OR statements to combine related phrases
- Avoid competitor-focused searches
- Make queries realistic and copy-paste ready
- Generate a mix of full queries and shorter targeted queries for better results
- keep it short and concise

Example queries:
Full query: https://www.google.com/search?q=%22looking+to+buy+a+flat%22+OR+%22need+apartment%22+OR+%22property+wanted%22
Short query: https://www.google.com/search?q=%22buy+flat%22+OR+%22need+apartment%22
Location query: https://www.google.com/search?q=%22looking+for+property%22+{region}

Return as: {{"queries": ["url1", "url2", "url3"]}}
'''
        
        model = "gemini-2.5-flash"
        response = generate_content(model, prompt)
        res = format_json_llm(response.text)
        
        search_queries = []
        if res and "queries" in res:
            search_queries = res["queries"][:2]  # 2 queries
        else:
            fallback_terms = product_name.replace(' ', '+').lower()
            search_queries = [
                f"https://www.google.com/search?q=%22looking+for%22+{fallback_terms}"
            ]
        # search_queries = [
        #     "https://www.google.com/search?q=real+estate+agents+in+New+York",
        #     "https://www.google.com/search?q=buy+commercial+property+New+York"
        # ]
        final_output = []
        for query_url in search_queries:
            print(f"Executing async search: {query_url}")
            output = await main_google_search(query_url, use_api_fallback=True)
            final_output.extend(output)
        
        # Convert to unified format and filter valid leads
        unified_output = []
        for lead in final_output:
            contact_info = lead.get("contact_info", {})
            # Enhanced validation for lead quality (including social media)
            has_contact = (contact_info.get("email") and contact_info.get("email") not in ["NA", "", "N/A"]) or \
                         (contact_info.get("phone") and contact_info.get("phone") not in ["NA", "", "N/A"]) or \
                         (contact_info.get("linkedin") and contact_info.get("linkedin") not in ["NA", "", "N/A"]) or \
                         (contact_info.get("website") and contact_info.get("website") not in ["NA", "", "N/A"]) or \
                         (contact_info.get("twitter") and contact_info.get("twitter") not in ["NA", "", "N/A"]) or \
                         (contact_info.get("facebook") and contact_info.get("facebook") not in ["NA", "", "N/A"]) or \
                         (contact_info.get("instagram") and contact_info.get("instagram") not in ["NA", "", "N/A"]) or \
                         (contact_info.get("socialmedialinks") and len(contact_info.get("socialmedialinks", [])) > 0)
            
            # For social media profiles, accept if we have URL even without company name
            is_social_media = lead.get("type") == "social_media_profile"
            has_url = lead.get("source_url") and lead.get("source_url") not in ["", "NA"]
            
            # Special handling for social media leads - they should always pass if they have a URL
            if is_social_media and has_url:
                unified_lead = convert_to_unified_format(lead)
                unified_output.append(unified_lead)
                continue
            
            if has_contact:
                # Additional quality check - ensure lead has meaningful content
                if (lead.get("name") and lead.get("name") not in ["", "NA"]) or \
                   (lead.get("company_name") and lead.get("company_name") not in ["", "NA"]):
                    unified_lead = convert_to_unified_format(lead)
                    unified_output.append(unified_lead)
        
        print(f"Final unified_output count: {len(unified_output)}")
        social_in_unified = sum(1 for lead in unified_output if lead.get('contact', {}).get('social_media_handles', {}).get('other'))
        print(f"Social media leads in unified_output: {social_in_unified}")
        
        # Store leads
        storage_results = {"stored": 0, "duplicates": 0, "errors": 0}
        try:
            mongodb_manager = get_mongodb_manager()
            storage_results = store_unified_leads(unified_output, mongodb_manager, icp_identifier)
        except Exception as e:
            print(f"Error storing leads: {e}")
            storage_results = {"stored": 0, "duplicates": 0, "errors": len(unified_output)}
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        return {
            "success": True,
            "data": unified_output,
            "summary": {
                "total_leads_found": len(unified_output),
                "leads_stored": storage_results["stored"],
                "duplicates_found": storage_results["duplicates"],
                "errors": storage_results["errors"],
                "queries_executed": len(search_queries),
                "execution_time_seconds": execution_time
            },
            "storage_results": storage_results
        }
        
    except Exception as e:
        print(f"Error in run_web_crawler_async: {e}")
        return {
            "success": False,
            "error": str(e),
            "data": [],
            "summary": {
                "total_leads_found": 0,
                "leads_stored": 0,
                "duplicates_found": 0,
                "errors": 1,
                "queries_executed": 0,
                "execution_time_seconds": 0
            }
        }

def load_icp_from_json(json_file_path: str) -> Dict[str, Any]:
    """Load ICP data from a JSON file"""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            icp_data = json.load(f)
        print(f"✅ Successfully loaded ICP data from {json_file_path}")
        return icp_data
    except FileNotFoundError:
        print(f"❌ Error: JSON file not found at {json_file_path}")
        return {}
    except json.JSONDecodeError as e:
        print(f"❌ Error: Invalid JSON format in {json_file_path}: {e}")
        return {}
    except Exception as e:
        print(f"❌ Error loading JSON file: {e}")
        return {}

# Hardcoded ICP data for Social Media Marketing Services
MARKETING_ICP_DATA = {
    "product_details": {
        "product_name": "Social Media Marketing Services",
        "product_category": "Digital Marketing",
        "usps": [
            "Viral content creation",
            "Influencer partnerships",
            "Brand awareness campaigns",
            "Social media strategy consulting",
            "Content calendar management"
        ],
        "pain_points_solved": [
            "Low social media engagement",
            "Inconsistent brand presence",
            "Difficulty reaching target audience",
            "Lack of viral content strategy",
            "Poor social media ROI",
            "Time-consuming content creation"
        ]
    },
    "icp_information": {
        "target_industry": [
            "E-commerce",
            "Small Business",
            "Content Creators",
            "Influencers",
            "Restaurants",
            "Retail Stores",
            "Service Providers"
        ],
        "decision_maker_persona": [
            "Small business owners",
            "Marketing managers",
            "Content creators",
            "Social media coordinators",
            "Entrepreneurs",
            "Brand managers"
        ],
        "region": [
            "United States",
            "Canada",
            "United Kingdom",
            "Australia"
        ],
        "budget_range": "$1,000-$10,000",
        "specific_occasions": [
            "Product launches",
            "Brand awareness campaigns",
            "Customer engagement",
            "Lead generation",
            "Market expansion",
            "Seasonal promotions"
        ]
    }
}

REAL_ESTATE_ICP_DATA = {
    "product_details": {
        "product_name": "Commercial Real Estate Investment Services",
        "product_category": "Real Estate Investment",
        "usps": [
            "Prime property location scouting",
            "Investment property analysis",
            "Commercial real estate financing",
            "Property management solutions",
            "Market trend insights"
        ],
        "pain_points_solved": [
            "Difficulty finding investment properties",
            "Lack of market knowledge",
            "Financing challenges",
            "Property management burdens",
            "Poor investment returns",
            "Time-consuming property search"
        ]
    },
    "icp_information": {
        "target_industry": [
            "Real Estate Investors",
            "Property Developers",
            "Business Owners",
            "High-Net-Worth Individuals",
            "Investment Firms",
            "Commercial Property Seekers"
        ],
        "decision_maker_persona": [
            "Real estate investors",
            "Property developers",
            "Business expansion managers",
            "Investment advisors",
            "Entrepreneurs seeking locations",
            "Commercial property buyers"
        ],
        "region": [
            "United States",
            "Canada",
            "United Kingdom",
            "Australia",
            "Major metropolitan areas"
        ],
        "budget_range": "$100,000-$5,000,000",
        "specific_occasions": [
            "Business expansion",
            "Investment portfolio growth",
            "Commercial property acquisition",
            "Location scouting",
            "Real estate investment opportunities",
            "Property portfolio diversification"
        ]
    }
}

def create_sample_icp_json(filename: str = 'healthcare_icp.json'):
    """Create a sample ICP JSON file for reference"""
    sample_icp = {
        "product_details": {
            "product_name": "Healthcare Management Software",
            "product_category": "Healthcare Technology",
            "usps": [
                "HIPAA compliant patient management",
                "Integrated electronic health records",
                "Automated billing and insurance processing",
                "Real-time appointment scheduling",
                "Advanced analytics and reporting"
            ],
            "pain_points_solved": [
                "Manual patient record keeping",
                "Billing errors and delays",
                "Appointment scheduling conflicts",
                "Compliance documentation hassles",
                "Limited patient data insights"
            ]
        },
        "icp_information": {
            "target_industry": [
                "Private medical practices",
                "Dental clinics",
                "Specialty healthcare providers",
                "Medical groups",
                "Healthcare facilities"
            ],
            "company_size": "Small to medium healthcare practices",
            "decision_maker_persona": [
                "Practice owners",
                "Medical directors",
                "Office managers",
                "Healthcare administrators",
                "IT managers in healthcare"
            ],
            "region": ["Local metropolitan areas", "Healthcare districts", "Medical communities"],
            "budget_range": "$50,000-$500,000",
            "specific_occasions": [
                "Practice expansion",
                "Digital transformation",
                "Compliance upgrades",
                "Patient volume growth",
                "Operational efficiency improvements"
            ]
        }
    }

    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(sample_icp, f, indent=2, ensure_ascii=False)
        print(f"✅ Sample ICP JSON created: {filename}")
        return True
    except Exception as e:
        print(f"❌ Error creating sample JSON file: {e}")
        return False

if __name__ == "__main__":
    import sys

    # Check Cloudflare bypass setup
    cloudflare_ready = setup_cloudflare_bypass()
    if cloudflare_ready:
        print("🛡️  Cloudflare bypass is ready!")
    else:
        print("🔒 Cloudflare bypass not configured - some sites may be inaccessible")

    # Use hardcoded ICP data instead of loading from JSON
    icp_data = REAL_ESTATE_ICP_DATA
    icp_identifier = 'real_estate'

    print(f"🏥 Running Lead Generation Campaign for: {icp_data.get('product_details', {}).get('product_name', 'Unknown Product')}")
    result = asyncio.run(run_web_crawler_async(icp_data, icp_identifier=icp_identifier))

    # Print summary instead of full JSON to avoid datetime serialization issues
    if result["success"]:
        print("✅ Campaign completed successfully!")
        print(f"📊 Total leads found: {result['summary']['total_leads_found']}")
        print(f"💾 Leads stored: {result['summary']['leads_stored']}")
        print(f"🔄 Duplicates found: {result['summary']['duplicates_found']}")
        print(f"❌ Errors: {result['summary']['errors']}")
        print(".2f")
    else:
        print(f"❌ Campaign failed: {result.get('error', 'Unknown error')}")

def generic_web_crawl_sync(icp_data, icp_identifier: str = 'default'):
    """Synchronous version of generic_web_crawl for simpler use cases"""
    print("Running synchronous web crawler...")
    
    # Extract ICP information
    product_name = icp_data["product_details"]["product_name"]
    product_category = icp_data["product_details"]["product_category"]
    usps = ", ".join(icp_data["product_details"]["usps"])
    pain_points_solved = ", ".join(icp_data["product_details"]["pain_points_solved"])
    
    target_industry = ", ".join(icp_data["icp_information"]["target_industry"])
    decision_maker_persona = ", ".join(icp_data["icp_information"]["decision_maker_persona"])
    region = ", ".join(icp_data["icp_information"]["region"])
    
    # Handle specific occasions
    specific_occasions = icp_data["icp_information"].get("specific_occasions", 
                                                        icp_data["icp_information"].get("travel_occasions", []))
    if isinstance(specific_occasions, list):
        specific_occasions = ", ".join(specific_occasions)
    
    # Generate search queries
    prompt = f'''I run a {product_name} business and want to find potential customers who might need my services.

TARGET: {target_industry} companies
PAIN POINTS: {pain_points_solved}
USE CASES: {specific_occasions}

Generate 3 Google search queries in **URL format** (https://www.google.com/search?q=...) 
that help me find potential customers, prospects, or decision-makers. 

Requirements:
- Use double quotes for exact phrases potential customers might use
- Focus on intent keywords (looking for, need, want, seeking, require, hiring)
- Use OR statements to combine related phrases
- Avoid competitor-focused searches
- Make queries realistic and copy-paste ready
- Generate a mix of full queries and shorter targeted queries for better results

Example queries:
Full query: https://www.google.com/search?q=%22looking+to+buy+a+flat%22+OR+%22need+apartment%22+OR+%22property+wanted%22
Short query: https://www.google.com/search?q=%22buy+flat%22+OR+%22need+apartment%22
Location query: https://www.google.com/search?q=%22looking+for+property%22+{region}

Return as: {{"queries": ["url1", "url2", "url3"]}}
'''

    model = "gemini-2.5-flash"
    response = generate_content(model, prompt)
    res = format_json_llm(response.text)
    
    search_queries = []
    if res and "queries" in res:
        search_queries = res["queries"][:3]
    else:
        fallback_terms = product_name.replace(' ', '+').lower()
        search_queries = [
            f"https://www.google.com/search?q=%22looking+for%22+{fallback_terms}"
        ]
    
    print(f"Generated search queries: {search_queries}")
    
    # Run synchronous crawling
    final_output = []
    for query_url in search_queries:
        print(f"Processing search: {query_url}")
        try:
            # Use asyncio to run the async function synchronously
            output = asyncio.run(main_google_search(query_url, use_api_fallback=True))
            final_output.extend(output)
            time.sleep(5)  # Longer delay to avoid rate limiting
        except Exception as e:
            print(f"Error processing search {query_url}: {e}")
    
    # Convert to unified format
    unified_output = []
    for lead in final_output:
        contact_info = lead.get("contact_info", {})
        if (contact_info.get("email") and contact_info.get("email") not in ["NA", "", "N/A"]) or \
           (contact_info.get("phone") and contact_info.get("phone") not in ["NA", "", "N/A"]):
            unified_lead = convert_to_unified_format(lead)
            unified_output.append(unified_lead)
    
    # Store results
    try:
        mongodb_manager = get_mongodb_manager()
        storage_results = store_unified_leads(unified_output, mongodb_manager, icp_identifier, export_csv=True)
        print(f"Stored {storage_results['stored']} leads, {storage_results['duplicates']} duplicates")
    except Exception as e:
        print(f"Database storage failed: {e}")
        # Fallback already handled in store_unified_leads
    
    print(f"Found {len(unified_output)} high-quality leads")
    return unified_output

def test_google_search_url(url):
    """Test if a Google search URL is accessible and extractable"""
    print(f"Testing Google search URL: {url}")
    print(f"URL length: {len(url)} characters")

    # Check URL components
    from urllib.parse import urlparse, parse_qs
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)

    print(f"Domain: {parsed.netloc}")
    print(f"Path: {parsed.path}")
    print(f"Query parameters: {list(query_params.keys())}")

    if 'q' in query_params:
        search_query = query_params['q'][0]
        print(f"Search query: {search_query}")
        print(f"Query length: {len(search_query)} characters")

        # Check for potential issues
        if len(search_query) > 2048:
            print("⚠️  WARNING: Search query is very long (>2048 chars)")
            print("Google might truncate or reject this query")

        if '"' in search_query:
            quote_count = search_query.count('"')
            print(f"Found {quote_count} quote pairs in query")
            if quote_count % 2 != 0:
                print("⚠️  WARNING: Unmatched quotes in search query")

    return True

async def debug_google_search(google_search_url):
    """Debug Google search crawling with detailed output"""
    print("=" * 60)
    print("DEBUGGING GOOGLE SEARCH CRAWLING")
    print("=" * 60)

    test_google_search_url(google_search_url)

    async with AsyncWebCrawler() as crawler:
        try:
            print("\n1. Attempting to load Google search page...")

            crawler_config = {
                "headers": {
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "DNT": "1",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Sec-Fetch-User": "?1",
                    "Cache-Control": "max-age=0",
                },
                "timeout": 30
            }

            result = await crawler.arun(url=google_search_url, **crawler_config)

            print("✓ Page loaded successfully")
            print(f"   Status code: {result.status_code if hasattr(result, 'status_code') else 'Unknown'}")
            print(f"   Content length: {len(result.markdown) if result.markdown else 0} characters")
            print(f"   Title: {result.title if hasattr(result, 'title') else 'No title'}")

            # Check for common blocking indicators
            if result.markdown:
                content_lower = result.markdown.lower()
                if 'captcha' in content_lower:
                    print("⚠️  CAPTCHA detected - Google is blocking the crawler")
                if 'unusual traffic' in content_lower:
                    print("⚠️  'Unusual traffic' detected - Google rate limiting")
                if 'blocked' in content_lower:
                    print("⚠️  Request appears to be blocked")
                if len(result.markdown) < 1000:
                    print("⚠️  Content is very short - might be a redirect or error page")

            print("\n2. Analyzing links...")
            external_links = result.links.get('external', [])
            print(f"   External links found: {len(external_links)}")

            if external_links:
                print("   Sample external links:")
                for i, link in enumerate(external_links[:3]):
                    print(f"     {i+1}. {link.get('href', 'No href')}")

            # Try manual parsing
            print("\n3. Attempting manual link extraction...")
            if result.markdown:
                import re
                # Google search result link patterns
                patterns = [
                    r'href="(https?://[^"]*?)"[^>]*?class="[^"]*?(?:yuRUbf|egMi0|d5oMvf)[^"]*?"',
                    r'href="(https?://[^"]*?)"[^>]*?data-ved="[^"]*"[^>]*?class="[^"]*?result[^"]*?"',
                    r'href="(https?://[^"]*?)"[^>]*?ping="[^"]*?"'
                ]

                manual_links = []
                for pattern in patterns:
                    matches = re.findall(pattern, result.markdown, re.IGNORECASE)
                    manual_links.extend(matches)

                # Remove duplicates and filter
                manual_links = list(set(manual_links))
                manual_links = [link for link in manual_links
                              if link and not any(skip in link.lower()
                              for skip in ['google.com', 'youtube.com', 'maps.google.com', 'googleusercontent.com'])]

                print(f"   Manual extraction found: {len(manual_links)} links")

                if manual_links:
                    print("   Sample manual links:")
                    for i, link in enumerate(manual_links[:3]):
                        print(f"     {i+1}. {link}")

            print("\n4. Recommendations:")
            if not external_links and not manual_links:
                print("   • Try using a different User-Agent string")
                print("   • Add random delays between requests")
                print("   • Consider using Google Custom Search API instead")
                print("   • Check if Google is blocking your IP")
                print("   • Try accessing Google from a different network")

            print("=" * 60)

        except Exception as e:
            print(f"✗ Error during debugging: {e}")
            print("=" * 60)

# Test the specific Google search URL provided by the user
if __name__ == "__main__":
    import asyncio

    # Test the problematic URL
    test_url = 'https://www.google.com/search?q=%22seeking+investment+property%22+OR+%22want+to+buy+commercial+real+estate%22+OR+%22need+new+business+location%22+OR+%22property+investment+opportunities%22'

    print("Testing the provided Google search URL...")
    test_google_search_url(test_url)

    print("\nTesting Google Custom Search API...")
    # Test API if configured
    if GOOGLE_CUSTOM_SEARCH_API_KEY and GOOGLE_CUSTOM_SEARCH_CX:
        from urllib.parse import urlparse, parse_qs
        parsed_url = urlparse(test_url)
        query_params = parse_qs(parsed_url.query)
        search_query = query_params.get('q', [''])[0]

        api_results = search_google_custom_api(search_query, GOOGLE_CUSTOM_SEARCH_API_KEY, GOOGLE_CUSTOM_SEARCH_CX)
        print(f"API returned {len(api_results)} results")
        if api_results:
            print("Sample API results:")
            for i, result in enumerate(api_results[:3]):
                print(f"  {i+1}. {result.get('title', 'No title')}")
                print(f"     URL: {result.get('href', 'No URL')}")
    else:
        print("Google Custom Search API not configured")
        print("To use the API:")
        print("1. Go to https://console.developers.google.com/")
        print("2. Create a project and enable Custom Search API")
        print("3. Create credentials (API key)")
        print("4. Go to https://cse.google.com/ and create a Custom Search Engine")
        print("5. Set GOOGLE_CUSTOM_SEARCH_API_KEY and GOOGLE_CUSTOM_SEARCH_CX variables")

    # Comment out the debug analysis to reduce noise
    # print("\nRunning debug analysis...")
    # asyncio.run(debug_google_search(test_url))