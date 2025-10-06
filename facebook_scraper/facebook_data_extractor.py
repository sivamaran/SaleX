"""
Facebook Data Extractor 
Uses browser automation with JSON-LD extraction and custom selectors
"""

import asyncio
import json
import re
import time
from typing import Dict, Any, Optional, List
from bs4 import BeautifulSoup
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from playwright.async_api import Page
from web_scraper.utils.anti_detection import AntiDetectionManager # Import AntiDetectionManager
from database.mongodb_manager import get_mongodb_manager # Import the MongoDB manager

class FacebookDataExtractor:
    """Facebook data extractor"""
    
    def __init__(self):
        self.mongodb_manager = get_mongodb_manager()

    async def extract_facebook_data(self, page: Page, url: str, adm: Optional[AntiDetectionManager] = None) -> Dict[str, Any]:
        """Extract Facebook data from a specific URL using an existing Playwright page"""
        print(f"Extracting Facebook data from: {url}")
        
        try:
            # Navigate to the URL
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            
            # Progressive waits
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
            
            # Dismiss popups
            try:
                await self._dismiss_popups(page, adm)
            except Exception:
                pass

            # Basic scroll to trigger lazy content
            try:
                h = await page.evaluate("() => document.body.scrollHeight")
                if adm:
                    await adm.execute_human_behavior(page, behavior_type='scroll', position=int(h))
                else:
                    # Fallback if ADM is not available
                    await page.evaluate(f"window.scrollTo(0, {int(h)})")
                
                # Light mouse move near the center to simulate activity
                viewport = await page.viewport_size()
                if viewport:
                    if adm:
                        await adm.execute_human_behavior(page, behavior_type='mousemove', position=(int(viewport['width'] * 0.6), int(viewport['height'] * 0.6)))
            except Exception:
                pass
            
            # Dismiss popups again after interactions
            try:
                await self._dismiss_popups(page, adm)
            except Exception:
                pass

            html_content = await page.content()
            rendered_text = await page.evaluate("document.body.innerText")
            
            extracted_data = {
                'url': url,
                'url_type': 'unknown', # Initialize as unknown, will be detected later
                'html_length': len(html_content),
                'text_length': len(rendered_text),
                'json_ld_data': {},
                'meta_data': {},
                'extracted_data': {},
                'page_analysis': {}
            }
            
            json_ld_data = await self._extract_json_ld_data(html_content, 'unknown') # Pass 'unknown' for now
            extracted_data['json_ld_data'] = json_ld_data
            
            meta_data = await self._extract_meta_data(html_content)
            extracted_data['meta_data'] = meta_data
            print(f"DEBUG: Extracted meta_data: {json.dumps(meta_data, indent=2)}")
            
            # Detect URL type after meta_data is available
            url_type = await self._detect_url_type(url, html_content, meta_data, page)
            extracted_data['url_type'] = url_type # Update url_type in extracted_data
            
            combined_data = await self._combine_data_sources(json_ld_data, meta_data, url_type)
            extracted_data['extracted_data'] = combined_data

            # Extract contact info and category
            contact_info = self._extract_contact_info(html_content) # Use html_content for better email/phone extraction
            extracted_data['extracted_data']['emails'] = list(contact_info['emails'])
            extracted_data['extracted_data']['phone_numbers'] = list(contact_info['phone_numbers'])
            
            if url_type == 'page':
                page_category = await self._extract_page_category(page)
                if page_category:
                    extracted_data['extracted_data']['category'] = page_category

            page_analysis = await self._analyze_page_content(rendered_text, html_content, url_type)
            extracted_data['page_analysis'] = page_analysis
            
            return extracted_data
            
        except Exception as e:
            print(f"❌ Error extracting data from {url}: {e}")
            return {
                'url': url,
                'error': str(e),
                'success': False
            }

    async def _detect_url_type(self, url: str, html_content: str, meta_data: Dict[str, Any], page: Page) -> str:
        """Detect the type of Facebook URL (profile, page, post, etc.) using URL patterns, Open Graph meta tags, and page content."""
        og_type = meta_data.get('open_graph', {}).get('og:type', '').lower()
        
        # 1. Prioritize "restricted" if login/signup content is dominant
        login_indicators = [
            "log in to facebook", "sign up for facebook", "facebook helps you connect and share",
            "you must log in to continue", "the page may have been removed"
        ]
        
        for indicator in login_indicators:
            if indicator in html_content.lower():
                # Check if it's primarily a login page by looking for form elements
                if await page.locator('form[action*="login.php"]').is_visible():
                    return 'restricted'
        
        # 2. Specific URL patterns
        if re.search(r'facebook\.com/profile\.php\?id=\d+', url):
            return 'profile'
        if re.search(r'facebook\.com/groups/[^/]+', url):
            return 'group'
        if re.search(r'facebook\.com/events/[^/]+', url):
            return 'event'
        if re.search(r'facebook\.com/[^/]+/posts/\d+', url) or re.search(r'facebook\.com/story\.php\?story_fbid=\d+&id=\d+', url):
            return 'post'
        if re.search(r'facebook\.com/(?:pages|pg)/[^/]+', url) or re.search(r'facebook\.com/[^/]+/about/?', url):
            return 'page'
        
        # 3. Open Graph meta tags (less reliable for type, but good for general content)
        if og_type == 'profile':
            return 'profile'
        if og_type in ['website', 'article', 'business.business', 'books.book', 'music.song', 'video.movie', 'video.tv_show', 'video.episode']:
            return 'page' # Pages often use these generic OG types
        if og_type == 'community':
            return 'group'
        if og_type == 'event':
            return 'event'
        if og_type in ['article', 'video.other', 'music.song']: # Posts can be video.other
            return 'post'

        # 4. Element-based detection (for publicly accessible pages only)
        # These are less reliable if the page content is dynamic or behind a login wall
        try:
            if await page.locator('//span[text()="Add Friend" or text()="Follow"]').is_visible():
                return 'profile'
            if await page.locator('//span[text()="Like Page" or text()="Send Message"]').is_visible():
                return 'page'
            if await page.locator('//span[text()="Join Group" or text()="Joined"]').is_visible():
                return 'group'
            if await page.locator('//span[text()="Interested" or text()="Going"]').is_visible():
                return 'event'
        except Exception:
            pass # Ignore errors if elements are not found or page is restricted

        # 5. Generic username URLs (e.g., facebook.com/facebook) - assume page if not restricted
        if re.search(r'facebook\.com/[a-zA-Z0-9_.]+$', url):
            # Exclude known system paths
            if not re.search(r'facebook\.com/(?:friends|messages|bookmarks|saved|notifications|settings|help|privacy|terms|login|recover|marketplace|games|gaming|watch|live|stories|search|groups|events|findfriends|developers|business|creators|community|gamingvideo|jobs|weather|coronavirus|news|shops|offers|donations)/?$', url):
                return 'page' 

        return 'unknown'

    def _extract_contact_info(self, html_content: str) -> Dict[str, set]:
        """Extracts emails and phone numbers from HTML content."""
        emails = set(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?:\.[a-zA-Z]{2,})?', html_content, re.IGNORECASE))
        
        # More robust phone number regex for various formats, including international
        phone_numbers = set(re.findall(
            r'(?:(?:\+|00)\d{1,3}[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}|\d{2,4}[-.\s]?\d{2,4}[-.\s]?\d{2,4})', 
            html_content
        ))
        
        # Remove common false positives for phone numbers (e.g., years, simple numbers)
        phone_numbers = {p for p in phone_numbers if len(re.sub(r'\D', '', p)) >= 7}
        
        return {'emails': emails, 'phone_numbers': phone_numbers}

    async def _extract_page_category(self, page: Page) -> Optional[str]:
        """Extracts the category for a Facebook Page."""
        try:
            # Look for category text near the page title or in the 'About' section
            # This is highly dependent on Facebook's ever-changing HTML structure.
            
            # Attempt 1: Look for specific spans/divs containing category info
            # Example selectors, these might need adjustment based on real-world observation
            category_selectors = [
                '//div[contains(@class, "x1iyjqo2")]//span[contains(@class, "x193iq5w") and @dir="auto"]', # Common text element for category
                '//a[contains(@href, "/categories/")]/span', # Link to category
                '//div[contains(@aria-label, "Category")]/span', # Aria label based
            ]
            
            for selector in category_selectors:
                category_locator = page.locator(selector)
                if await category_locator.count() > 0 and await category_locator.first.is_visible():
                    return await category_locator.first.text_content()
            
            # Attempt 2: Look for Open Graph type as a fallback for category
            og_type_meta = await page.locator('meta[property="og:type"]').get_attribute('content')
            if og_type_meta and og_type_meta not in ['website', 'article', 'video.other']:
                return og_type_meta.replace('business.', '').replace('books.', '').replace('music.', '') # Clean up

            # Attempt 3: Search for common category keywords in page text (less precise)
            category_keywords = ["artist", "musician", "brand", "product", "company", "organization", "public figure", "community", "blog", "news", "shopping", "retail", "local business", "nonprofit organization", "government organization", "education", "media", "sports", "entertainment"]
            
            for keyword in category_keywords:
                if await page.locator(f'text="{keyword}"').count() > 0:
                    return keyword.capitalize()
            
            return None # No category found
        except Exception as e:
            print(f"Error extracting page category: {e}")
            return None

    async def _extract_json_ld_data(self, html_content: str, url_type: str) -> Dict[str, Any]:
        """Extract JSON-LD data"""
        print("🔍 Extracting JSON-LD data (primary source)...")
        json_ld_data = {
            'found': False,
            'raw_json': None,
            'parsed_data': {},
            'data_type': None,
            'extraction_success': False
        }
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            json_ld_scripts = soup.find_all('script', type='application/ld+json')
            
            if not json_ld_scripts:
                print("❌ No JSON-LD scripts found")
                return json_ld_data
            
            print(f"✅ Found {len(json_ld_scripts)} JSON-LD script(s)")
            
            for script in json_ld_scripts:
                if script.string:
                    try:
                        json_data = json.loads(script.string)
                        json_ld_data['raw_json'] = json_data
                        json_ld_data['found'] = True
                        
                        if url_type == 'profile':
                            parsed_data = await self._parse_profile_json_ld(json_data)
                            json_ld_data['data_type'] = 'profile'
                        elif url_type == 'page':
                            parsed_data = await self._parse_page_json_ld(json_data)
                            json_ld_data['data_type'] = 'page'
                        elif url_type == 'post':
                            parsed_data = await self._parse_post_json_ld(json_data)
                            json_ld_data['data_type'] = 'post'
                        else:
                            parsed_data = await self._parse_generic_json_ld(json_data)
                            json_ld_data['data_type'] = 'generic'
                        
                        json_ld_data['parsed_data'] = parsed_data
                        json_ld_data['extraction_success'] = True
                        print(f"✅ Successfully parsed JSON-LD for {url_type}")
                        break
                    except json.JSONDecodeError as e:
                        print(f"❌ JSON-LD parsing error: {e}")
                        continue
                    except Exception as e:
                        print(f"❌ Error parsing JSON-LD: {e}")
                        continue
        except Exception as e:
            print(f"❌ Error extracting JSON-LD: {e}")
        return json_ld_data

    async def _parse_profile_json_ld(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse profile JSON-LD data"""
        profile_data = {}
        if json_data.get('@type') == 'Person':
            profile_data['name'] = json_data.get('name')
            profile_data['description'] = json_data.get('description')
            profile_data['url'] = json_data.get('url')
            if 'image' in json_data and isinstance(json_data['image'], dict):
                profile_data['image'] = json_data['image'].get('url')
            if 'mainEntityOfPage' in json_data and isinstance(json_data['mainEntityOfPage'], dict):
                profile_data['mainEntityOfPage'] = json_data['mainEntityOfPage'].get('@id')
            print(f"✅ Extracted profile data from JSON-LD: {profile_data.get('name', 'Unknown')}")
        return profile_data

    async def _parse_page_json_ld(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse page JSON-LD data"""
        page_data = {}
        # Handle cases where Facebook might embed multiple JSON-LD objects in an array
        if isinstance(json_data, list):
            for item in json_data:
                if item.get('@type') in ['Organization', 'LocalBusiness', 'WebSite']:
                    json_data = item # Use the first relevant object
                    break
            else: # No relevant object found in list
                return page_data

        if json_data.get('@type') in ['Organization', 'LocalBusiness', 'WebSite']:
            page_data['name'] = json_data.get('name')
            page_data['description'] = json_data.get('description')
            page_data['url'] = json_data.get('url')
            page_data['address'] = json_data.get('address')
            page_data['telephone'] = json_data.get('telephone')
            page_data['priceRange'] = json_data.get('priceRange')
            page_data['aggregateRating'] = json_data.get('aggregateRating')
            if 'image' in json_data and isinstance(json_data['image'], dict):
                page_data['image'] = json_data['image'].get('url')
            if 'logo' in json_data and isinstance(json_data['logo'], dict):
                page_data['logo'] = json_data['logo'].get('url')
            if 'sameAs' in json_data:
                page_data['sameAs'] = json_data['sameAs']
            if 'numberOfFollowers' in json_data:
                page_data['numberOfFollowers'] = json_data['numberOfFollowers']
            if 'member' in json_data and isinstance(json_data['member'], list):
                page_data['members'] = [m.get('name') for m in json_data['member'] if isinstance(m, dict)]
            print(f"✅ Extracted page data from JSON-LD: {page_data.get('name', 'Unknown')}")
        return page_data
    
    async def _parse_post_json_ld(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse post JSON-LD data"""
        post_data = {}
        if json_data.get('@type') in ['Article', 'SocialMediaPosting', 'NewsArticle', 'BlogPosting']:
            post_data['headline'] = json_data.get('headline')
            post_data['articleBody'] = json_data.get('articleBody')
            post_data['datePublished'] = json_data.get('datePublished')
            post_data['dateModified'] = json_data.get('dateModified')
            post_data['url'] = json_data.get('url') or json_data.get('mainEntityOfPage', {}).get('@id')
            if 'author' in json_data and isinstance(json_data['author'], dict):
                post_data['author_name'] = json_data['author'].get('name')
                post_data['author_url'] = json_data['author'].get('url')
            if 'publisher' in json_data and isinstance(json_data['publisher'], dict):
                post_data['publisher_name'] = json_data['publisher'].get('name')
                post_data['publisher_url'] = json_data['publisher'].get('url')
            if 'image' in json_data and isinstance(json_data['image'], dict):
                post_data['image'] = json_data['image'].get('url')
            if 'interactionStatistic' in json_data and isinstance(json_data['interactionStatistic'], list):
                for stat in json_data['interactionStatistic']:
                    if stat.get('interactionType') == 'http://schema.org/LikeAction':
                        post_data['likes'] = stat.get('userInteractionCount')
                    elif stat.get('interactionType') == 'http://schema.org/CommentAction':
                        post_data['comments'] = stat.get('userInteractionCount')
            print(f"✅ Extracted post data from JSON-LD: {post_data.get('headline', 'Unknown')[:50]}...")
        return post_data

    async def _parse_generic_json_ld(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse generic JSON-LD data"""
        generic_data = {}
        # Handle cases where Facebook might embed multiple JSON-LD objects in an array
        if isinstance(json_data, list):
            # Try to find the most descriptive object (e.g., Article, Organization, Person)
            for item in json_data:
                if item.get('@type') in ['Article', 'SocialMediaPosting', 'NewsArticle', 'BlogPosting', 'Organization', 'LocalBusiness', 'Person', 'WebSite']:
                    json_data = item
                    break
            else: # No specific type found, use the first object if available
                if json_data:
                    json_data = json_data[0]

        generic_data['type'] = json_data.get('@type')
        generic_data['name'] = json_data.get('name')
        generic_data['headline'] = json_data.get('headline') # Often found in generic articles
        generic_data['description'] = json_data.get('description')
        generic_data['url'] = json_data.get('url') or json_data.get('@id')
        if 'image' in json_data and isinstance(json_data['image'], dict):
            generic_data['image'] = json_data['image'].get('url')
        if 'datePublished' in json_data:
            generic_data['datePublished'] = json_data['datePublished']
        if 'author' in json_data and isinstance(json_data['author'], dict):
            generic_data['author_name'] = json_data['author'].get('name')
        print(f"✅ Extracted generic data from JSON-LD: {generic_data.get('type', 'Unknown')}")
        return generic_data

    async def _extract_meta_data(self, html_content: str) -> Dict[str, Any]:
        """Extract meta data from HTML content"""
        print("🔍 Extracting meta data (secondary source)...")
        soup = BeautifulSoup(html_content, 'html.parser')
        meta_data = {
            'open_graph': {},
            'twitter': {},
            'other_meta': {},
            'title': '',
            'description': ''
        }
        meta_tags = soup.find_all('meta')
        for meta in meta_tags:
            name = meta.get('name') or meta.get('property')
            content = meta.get('content')
            if name and content:
                if name.startswith('og:'):
                    meta_data['open_graph'][name] = content
                elif name.startswith('twitter:'):
                    meta_data['twitter'][name] = content
                else:
                    meta_data['other_meta'][name] = content
        
        title_tag = soup.find('title')
        if title_tag:
            meta_data['title'] = title_tag.text
        
        description_tag = soup.find('meta', attrs={'name': 'description'})
        if description_tag:
            meta_data['description'] = description_tag.get('content', '')
        
        print(f"✅ Extracted meta data: {len(meta_data['open_graph'])} OpenGraph, {len(meta_data['twitter'])} Twitter")
        return meta_data

    async def _combine_data_sources(self, json_ld_data: Dict[str, Any], meta_data: Dict[str, Any], url_type: str) -> Dict[str, Any]:
        """Combine data from JSON-LD and meta sources"""
        print("🔍 Combining data sources...")
        combined_data = {}
        if json_ld_data.get('extraction_success'):
            combined_data.update(json_ld_data.get('parsed_data', {}))
        
        if meta_data:
            og_data = meta_data.get('open_graph', {})
            if og_data:
                combined_data['og_title'] = og_data.get('og:title', '')
                combined_data['og_description'] = og_data.get('og:description', '')
                combined_data['og_image'] = og_data.get('og:image', '')
                combined_data['og_url'] = og_data.get('og:url', '')
            
            twitter_data = meta_data.get('twitter', {})
            if twitter_data:
                combined_data['twitter_title'] = twitter_data.get('twitter:title', '')
                combined_data['twitter_description'] = twitter_data.get('twitter:description', '')
                combined_data['twitter_image'] = twitter_data.get('twitter:image', '')
            
            combined_data['page_title'] = meta_data.get('title', '')
            combined_data['page_description'] = meta_data.get('description', '')
        
        print(f"✅ Combined data sources: {len(combined_data)} fields")
        return combined_data

    async def _analyze_page_content(self, rendered_text: str, html_content: str, url_type: str) -> Dict[str, Any]:
        """Analyze page content for Facebook-specific data"""
        print("🔍 Analyzing page content...")
        analysis = {
            'facebook_keywords': [],
            'content_type': url_type,
            'text_summary': ''
        }
        
        facebook_keywords = [
            'likes', 'comments', 'shares', 'friends', 'followers', 'posts', 'photos',
            'videos', 'about', 'community', 'reviews', 'events', 'groups', 'marketplace',
            'facebook', 'meta'
        ]
        
        found_keywords = []
        for keyword in facebook_keywords:
            if keyword.lower() in rendered_text.lower():
                found_keywords.append(keyword)
        
        analysis['facebook_keywords'] = found_keywords
        
        lines = rendered_text.split('\n')
        non_empty_lines = [line.strip() for line in lines if line.strip()]
        analysis['text_summary'] = ' | '.join(non_empty_lines[:10])
        
        print(f"✅ Page content analysis completed. Found {len(found_keywords)} keywords.")
        return analysis

    async def save_facebook_data_to_json(self, extracted_data: Dict[str, Any], filename: str = "facebook_data.json") -> None:
        """Save Facebook data to a structured JSON file"""
        facebook_data = {
            "metadata": {
                "scraping_timestamp": time.time(),
                "scraping_date": time.strftime("%Y-%m-%d %H:%M:%S"),
                "extractor_version": "facebook_data_extractor_v1.0",
                "url": extracted_data.get('url'),
                "url_type": extracted_data.get('url_type'),
                "platform": "facebook",
                "data_sources": ["json_ld", "meta_tags"]
            },
            "extraction_summary": {
                "success": not extracted_data.get('error'),
                "json_ld_found": extracted_data.get('json_ld_data', {}).get('found', False),
                "json_ld_success": extracted_data.get('json_ld_data', {}).get('extraction_success', False),
                "meta_data_found": bool(extracted_data.get('meta_data', {}).get('open_graph')),
                "html_content_length": extracted_data.get('html_length', 0),
                "text_content_length": extracted_data.get('text_length', 0)
            },
            "extracted_data": {
                "json_ld_data": extracted_data.get('json_ld_data', {}),
                "meta_data": extracted_data.get('meta_data', {}),
                "combined_data": extracted_data.get('extracted_data', {}),
                "page_analysis": extracted_data.get('page_analysis', {})
            }
        }
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(facebook_data, f, indent=2, ensure_ascii=False, default=str)
            print(f"\n✅ Facebook data saved to: {filename}")
        except Exception as e:
            print(f"❌ Error saving Facebook data to {filename}: {e}")

    async def _dismiss_popups(self, page: Page, adm: Optional[AntiDetectionManager] = None):
        """Attempt to close common consent/sign-in/newsletter popups with human-like actions."""
        text_buttons = [
            "Accept all", "Accept All", "Accept", "I agree", "I Agree", "Allow all", "Allow All",
            "Got it", "Okay", "OK", "Close", "No thanks", "Not now", "Not Now", "Dismiss",
            "Only allow essential cookies", "Allow all cookies",
        ]
        selectors = [
            "#onetrust-accept-btn-handler", ".onetrust-close-btn-handler", ".ot-pc-refuse-all-handler",
            "#cky-consent-accept", ".cky-consent-btn-accept",
            "button[aria-label='Close']", "button[aria-label='close']", "[data-testid='close']",
            ".modal-close, .modal__close, .close, .close-button",
            ".newsletter, .cookie, .cookies, .gdpr, .consent",
        ]
        
        async def _click_if_visible(locator):
            try:
                if await locator.is_visible():
                    if adm:
                        await asyncio.sleep(0.2) # small human-like pause
                    await locator.click(timeout=1500)
                    return True
            except Exception:
                return False
            return False

        # Try role-based buttons with text
        for label in text_buttons:
            try:
                btn = page.get_by_role("button", name=label)
                if await _click_if_visible(btn):
                    return
            except Exception:
                pass

        # Try text locators for links/divs that act as buttons
        for label in text_buttons:
            try:
                el = page.get_by_text(label, exact=False)
                if await _click_if_visible(el):
                    return
            except Exception:
                pass

        # Try CSS selectors
        for sel in selectors:
            try:
                loc = page.locator(sel)
                if await _click_if_visible(loc):
                    return
            except Exception:
                continue

        # Last resort: press Escape to dismiss dialogs
        try:
            await page.keyboard.press("Escape")
        except Exception:
            pass

        # Small human-like mouse wiggle which can also reveal close buttons
        try:
            vp = await page.viewport_size()
            if vp:
                x, y = int(vp['width'] * 0.9), int(vp['height'] * 0.1)
                await page.mouse.move(x, y)
                await page.mouse.move(x - 20, y + 10)
        except Exception:
            pass
