"""
Browser Manager - Task 1: Basic Infrastructure with Anti-Detection
Handles browser automation with comprehensive stealth configuration
Supports both Instagram and LinkedIn scraping
"""

import asyncio
import random
import time
import re
from typing import Optional, Dict, Any
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from fake_useragent import UserAgent
from linkedin_scraper.anti_detection import AntiDetectionManager, create_stealth_browser_context, execute_human_behavior


class BrowserManager:
    """Manages browser automation with comprehensive anti-detection features for Instagram and LinkedIn"""
    
    def __init__(self, headless: bool = True, enable_anti_detection: bool = True, is_mobile: bool = False, platform: str = "instagram"):
        self.headless = headless
        self.enable_anti_detection = enable_anti_detection
        self.is_mobile = is_mobile
        self.platform = platform.lower()  
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.ua = UserAgent()
        
        # Initialize anti-detection manager
        if self.enable_anti_detection:
            self.anti_detection = AntiDetectionManager(
                enable_fingerprint_evasion=True,
                enable_behavioral_mimicking=True,
                enable_network_obfuscation=True
            )
        else:
            self.anti_detection = None
        
    async def start(self) -> None:
        """Initialize browser with comprehensive anti-detection configuration"""
        self.playwright = await async_playwright().start()
        
        if self.enable_anti_detection and self.anti_detection:
            # Use advanced anti-detection configuration
            self.browser, self.context = await create_stealth_browser_context(
                self.playwright, self.anti_detection, is_mobile=self.is_mobile
            )
        else:
            # Fallback to basic stealth configuration
            browser_args = [
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor',
                '--disable-extensions',
                '--disable-plugins',
                '--disable-images',
            ]
            
            self.browser = await self.playwright.chromium.launch(
                headless=self.headless,
                args=browser_args
            )
            
            # Platform-specific user agent
            if self.platform == "linkedin":
                user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            else:
                user_agent = self.ua.random
            
            self.context = await self.browser.new_context(
                user_agent=user_agent,
                viewport={'width': 1920, 'height': 1080},
                locale='en-US',
                timezone_id='America/New_York',
            )
            
            # Add basic stealth scripts
            await self.context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
                
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5],
                });
                
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en'],
                });
            """)
        
        self.page = await self.context.new_page()
        
        # Set additional headers based on platform
        if self.platform == "linkedin":
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
            }
        else:
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
        
        await self.page.set_extra_http_headers(headers)
        
    async def stop(self) -> None:
        """Clean up browser resources"""
        if self.page:
            await self.page.close()
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if hasattr(self, 'playwright'):
            await self.playwright.stop()
            
    async def navigate_to(self, url: str, wait_time: int =3, referer: Optional[str] = None) -> None:
        """Navigate to URL with human-like delays and anti-detection measures
        Optionally sends a Google-like referer to simulate navigation from search results.
        """
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
        
        # Apply network obfuscation delay
        if self.enable_anti_detection and self.anti_detection:
            delay = await self.anti_detection.calculate_request_delay()
            await asyncio.sleep(delay)
        else:
            # Random delay to mimic human behavior
            await asyncio.sleep(random.uniform(1, 3))
        
        # Use Playwright per-navigation referer if provided
        if referer:
            await self.page.goto(url, wait_until='domcontentloaded', referer=referer)
        else:
            await self.page.goto(url, wait_until='domcontentloaded')
        
        # Update request count for anti-detection tracking
        if self.enable_anti_detection and self.anti_detection:
            self.anti_detection.request_count += 1
            self.anti_detection.last_request_time = time.time()
        
        # Wait for page to load
        await asyncio.sleep(wait_time)
        
    async def close_popup(self) -> bool:
        """Attempt to close platform-specific popup"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
            
        try:
            # Wait a bit for popup to load
            await asyncio.sleep(2)
            
            if self.platform == "instagram":
                return await self._close_instagram_popup()
            elif self.platform == "linkedin":
                return await self._close_linkedin_popup()
            else:
                return await self._close_generic_popup()
                
        except Exception as e:
            print(f"  - Error closing popup: {e}")
            return False
    
    async def _close_instagram_popup(self) -> bool:
        """Attempt to close Instagram login/signup popup"""
        # Common selectors for Instagram popup close buttons
        close_selectors = [
            'button[aria-label="Close"]',
            'button[aria-label="Close dialog"]',
            'svg[aria-label="Close"]',
            'div[aria-label="Close"]',
            'button[type="button"] svg[aria-label="Close"]',
            'div[role="dialog"] button[aria-label="Close"]',
            'div[role="dialog"] svg[aria-label="Close"]',
            'div[data-testid="close-button"]',
            'button[data-testid="close-button"]',
            'svg[data-testid="close-button"]',
            # Generic close button selectors
            'button:has-text("×")',
            'button:has-text("✕")',
            'button:has-text("X")',
            'div:has-text("×")',
            'div:has-text("✕")',
            'div:has-text("X")',
            # Instagram specific
            'div[role="dialog"] button',
            'div[role="dialog"] div[aria-label="Close"]',
            'div[role="dialog"] svg[aria-label="Close"]'
        ]
        
        return await self._try_close_with_selectors(close_selectors, "Instagram")
    
    async def _close_linkedin_popup(self) -> bool:
        """Attempt to close LinkedIn login/signup popup"""
        # Common selectors for LinkedIn popup close buttons
        close_selectors = [
            # LinkedIn specific close buttons
            'button[aria-label="Dismiss"]',
            'button[aria-label="Close"]',
            'button[data-control-name="close"]',
            'button[data-test-id="close-button"]',
            'button[class*="close"]',
            'button[class*="dismiss"]',
            'div[aria-label="Close"]',
            'div[aria-label="Dismiss"]',
            'svg[aria-label="Close"]',
            'svg[aria-label="Dismiss"]',
            # Generic close button selectors
            'button:has-text("×")',
            'button:has-text("✕")',
            'button:has-text("X")',
            'button:has-text("Close")',
            'button:has-text("Dismiss")',
            'div:has-text("×")',
            'div:has-text("✕")',
            'div:has-text("X")',
            # LinkedIn modal specific
            'div[role="dialog"] button[aria-label="Close"]',
            'div[role="dialog"] button[aria-label="Dismiss"]',
            'div[role="dialog"] svg[aria-label="Close"]',
            'div[role="dialog"] svg[aria-label="Dismiss"]',
            # LinkedIn sign-in modal
            'div[data-test-id="sign-in-modal"] button[aria-label="Close"]',
            'div[data-test-id="sign-in-modal"] button[aria-label="Dismiss"]',
            # LinkedIn overlay close
            'div[class*="overlay"] button[aria-label="Close"]',
            'div[class*="modal"] button[aria-label="Close"]'
        ]
        
        return await self._try_close_with_selectors(close_selectors, "LinkedIn")
    
    async def _close_generic_popup(self) -> bool:
        """Attempt to close generic popup"""
        close_selectors = [
            'button[aria-label="Close"]',
            'button[aria-label="Dismiss"]',
            'button[class*="close"]',
            'button[class*="dismiss"]',
            'div[aria-label="Close"]',
            'div[aria-label="Dismiss"]',
            'svg[aria-label="Close"]',
            'svg[aria-label="Dismiss"]',
            'button:has-text("×")',
            'button:has-text("✕")',
            'button:has-text("X")',
            'button:has-text("Close")',
            'button:has-text("Dismiss")'
        ]
        
        return await self._try_close_with_selectors(close_selectors, "Generic")
    
    async def _try_close_with_selectors(self, selectors: list, platform_name: str) -> bool:
        """Try to close popup using a list of selectors"""
        for selector in selectors:
            try:
                # Check if element exists
                element = await self.page.query_selector(selector)
                if element:
                    print(f"  - Found close button with selector: {selector}")
                    
                    # Click the close button
                    await element.click()
                    print(f"  - Clicked close button")
                    
                    # Wait for popup to close
                    await asyncio.sleep(2)
                    
                    # Verify popup is closed by checking if close button still exists
                    element_after = await self.page.query_selector(selector)
                    if not element_after:
                        print(f"  - {platform_name} popup successfully closed")
                        return True
                    else:
                        print(f"  - {platform_name} popup may still be visible, trying next selector")
                        
            except Exception as e:
                print(f"  - Error with selector '{selector}': {e}")
                continue
        
        # If no close button found, try pressing Escape key
        print(f"  - No close button found, trying Escape key")
        await self.page.keyboard.press('Escape')
        await asyncio.sleep(1)
        
        # Check if any dialog is still present
        dialog = await self.page.query_selector('div[role="dialog"]')
        if not dialog:
            print(f"  - {platform_name} popup closed with Escape key")
            return True
        else:
            print(f"  - {platform_name} popup still visible after Escape key")
            return False
            
    async def navigate_to_with_popup_close(self, url: str, wait_time: int = 3, referer: Optional[str] = None) -> bool:
        """Navigate to URL and attempt to close any popup
        Optionally sends a referer (e.g., Google) during navigation.
        """
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
            
        # Navigate to URL
        await self.navigate_to(url, wait_time, referer=referer)
        
        # Try to close popup
        popup_closed = await self.close_popup()
        
        return popup_closed
        
    async def get_page_content(self) -> str:
        """Get current page HTML content"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
        #self.page.content() = Returns the full HTML source of the current page after JavaScript has run.
        return await self.page.content()
        
    async def get_rendered_text(self) -> str:
        """Get text content after JavaScript rendering"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
        #Returns only the visible text(no tags) inside the <body> tag after JavaScript has rendered it.
        return await self.page.text_content('body')
        
    async def get_page_title(self) -> str:
        """Get page title"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
        return await self.page.title()
        
    async def get_page_url(self) -> str:
        """Get current page URL"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
        return self.page.url
        
    async def check_popup_visible(self) -> bool:
        """Check if platform-specific popup is still visible"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
            
        try:
            # Check for common popup indicators
            popup_selectors = [
                'div[role="dialog"]',
                'div[data-testid="login-form"]',
                'div[data-testid="signup-form"]',
                'div[aria-label="Close"]',
                'button[aria-label="Close"]',
                'div[role="dialog"] button[aria-label="Close"]'
            ]
            
            # Platform-specific selectors
            if self.platform == "linkedin":
                popup_selectors.extend([
                    'div[data-test-id="sign-in-modal"]',
                    'div[class*="overlay"]',
                    'div[class*="modal"]',
                    'div[data-test-id="login-form"]',
                    'div[data-test-id="signup-form"]'
                ])
            
            for selector in popup_selectors:
                element = await self.page.query_selector(selector)
                if element:
                    return True
                    
            return False
            
        except Exception as e:
            print(f"Error checking popup visibility: {e}")
            return False
            
    async def check_for_platform_content(self) -> dict:
        """Check for platform-specific content and elements"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
            
        if self.platform == "instagram":
            return await self._check_for_instagram_content()
        elif self.platform == "linkedin":
            return await self._check_for_linkedin_content()
        else:
            return await self._check_for_generic_content()
    
    async def _check_for_instagram_content(self) -> dict:
        """Check for Instagram-specific content and elements"""
        analysis = {
            'has_instagram_elements': False,
            'has_login_form': False,
            'has_profile_content': False,
            'has_posts': False,
            'has_stories': False,
            'page_type': 'unknown'
        }
        
        try:
            # Check for Instagram-specific elements
            instagram_selectors = [
                'div[data-testid="user-avatar"]',
                'div[data-testid="post-container"]',
                'div[data-testid="story-item"]',
                'div[data-testid="login-form"]',
                'div[data-testid="profile-header"]',
                'div[data-testid="user-info"]',
                'div[data-testid="post"]',
                'div[data-testid="story"]',
                'div[data-testid="feed"]',
                'div[data-testid="explore"]'
            ]
            
            for selector in instagram_selectors:
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        analysis['has_instagram_elements'] = True
                        break
                except:
                    continue
            
            # Check for login form
            login_selectors = [
                'form[action*="login"]',
                'input[name="username"]',
                'input[name="password"]',
                'button[type="submit"]'
            ]
            
            for selector in login_selectors:
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        analysis['has_login_form'] = True
                        break
                except:
                    continue
            
            # Check for profile content
            profile_selectors = [
                'div[data-testid="user-avatar"]',
                'div[data-testid="profile-header"]',
                'div[data-testid="user-info"]',
                'h1',  # Profile name
                'span[data-testid="user-bio"]'
            ]
            
            for selector in profile_selectors:
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        analysis['has_profile_content'] = True
                        break
                except:
                    continue
            
            # Check for posts
            post_selectors = [
                'div[data-testid="post-container"]',
                'div[data-testid="post"]',
                'article',
                'div[role="button"]'
            ]
            
            for selector in post_selectors:
                try:
                    elements = await self.page.query_selector_all(selector)
                    if len(elements) > 0:
                        analysis['has_posts'] = True
                        break
                except:
                    continue
            
            # Determine page type
            if analysis['has_login_form']:
                analysis['page_type'] = 'login_page'
            elif analysis['has_profile_content']:
                analysis['page_type'] = 'profile_page'
            elif analysis['has_posts']:
                analysis['page_type'] = 'feed_page'
            elif analysis['has_instagram_elements']:
                analysis['page_type'] = 'instagram_page'
                
        except Exception as e:
            print(f"Error analyzing Instagram content: {e}")
            
        return analysis
    
    async def _check_for_linkedin_content(self) -> dict:
        """Check for LinkedIn-specific content with JSON-LD focus"""
        analysis = {
            'has_linkedin_elements': False,
            'has_login_form': False,
            'has_profile_content': False,
            'has_company_content': False,
            'has_post_content': False,
            'has_newsletter_content': False,
            'has_json_ld': False,
            'has_meta_tags': False,
            'page_type': 'unknown',
            'json_ld_type': None
        }
        
        try:
            # PRIMARY: Check for JSON-LD data (most reliable)
            json_ld_scripts = await self.page.query_selector_all('script[type="application/ld+json"]')
            if json_ld_scripts:
                analysis['has_json_ld'] = True
                print(f"✅ Found {len(json_ld_scripts)} JSON-LD script(s)")
                
                # Parse JSON-LD to determine content type
                for script in json_ld_scripts:
                    try:
                        script_content = await script.text_content()
                        if script_content:
                            import json
                            json_data = json.loads(script_content)
                            
                            # Check for Person type (profiles)
                            if '@graph' in json_data:
                                for item in json_data['@graph']:
                                    if item.get('@type') == 'Person':
                                        analysis['has_profile_content'] = True
                                        analysis['page_type'] = 'profile_page'
                                        analysis['json_ld_type'] = 'Person'
                                        print("✅ Found Person JSON-LD data (profile)")
                                        break
                                    elif item.get('@type') == 'Organization':
                                        analysis['has_company_content'] = True
                                        analysis['page_type'] = 'company_page'
                                        analysis['json_ld_type'] = 'Organization'
                                        print("✅ Found Organization JSON-LD data (company)")
                                        break
                            elif json_data.get('@type') == 'Person':
                                analysis['has_profile_content'] = True
                                analysis['page_type'] = 'profile_page'
                                analysis['json_ld_type'] = 'Person'
                                print("✅ Found Person JSON-LD data (profile)")
                            elif json_data.get('@type') == 'Organization':
                                analysis['has_company_content'] = True
                                analysis['page_type'] = 'company_page'
                                analysis['json_ld_type'] = 'Organization'
                                print("✅ Found Organization JSON-LD data (company)")
                            elif json_data.get('@type') == 'DiscussionForumPosting':
                                analysis['has_post_content'] = True
                                analysis['page_type'] = 'post_page'
                                analysis['json_ld_type'] = 'DiscussionForumPosting'
                                print("✅ Found DiscussionForumPosting JSON-LD data (post)")
                            elif json_data.get('@type') == 'Article':
                                analysis['has_newsletter_content'] = True
                                analysis['page_type'] = 'newsletter_page'
                                analysis['json_ld_type'] = 'Article'
                                print("✅ Found Article JSON-LD data (newsletter)")
                                
                    except (json.JSONDecodeError, Exception) as e:
                        print(f"❌ JSON-LD parsing error: {e}")
                        continue
            else:
                analysis['has_json_ld'] = False
                print("❌ No JSON-LD scripts found")
            
            # SECONDARY: Check for meta tags (social media data)
            meta_tags = await self.page.query_selector_all('meta[property^="og:"], meta[name^="twitter:"]')
            if meta_tags:
                analysis['has_meta_tags'] = True
                print(f"✅ Found {len(meta_tags)} social media meta tags")
            else:
                analysis['has_meta_tags'] = False
            
            # FALLBACK: Check for login forms
            login_selectors = [
                'form[action*="login"]',
                'input[name="session_key"]',
                'input[name="session_password"]',
                'button[type="submit"]',
                'div[data-test-id="sign-in-modal"]',
                'div[class*="login"]',
                'div[class*="signin"]'
            ]
            
            for selector in login_selectors:
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        analysis['has_login_form'] = True
                        break
                except:
                    continue
            
            # FALLBACK: Check for LinkedIn-specific elements if JSON-LD didn't determine type
            if analysis['page_type'] == 'unknown':
                # Check for profile content
                profile_selectors = [
                    'div[class*="profile"]',
                    'div[data-test-id="profile"]',
                    'h1[class*="profile"]',
                    'div[class*="summary"]',
                    'div[class*="experience"]',
                    'div[class*="education"]'
                ]
                
                for selector in profile_selectors:
                    try:
                        element = await self.page.query_selector(selector)
                        if element:
                            analysis['has_profile_content'] = True
                            analysis['page_type'] = 'profile_page'
                            break
                    except:
                        continue
                
                # Check for company content
                company_selectors = [
                    'div[class*="company"]',
                    'div[data-test-id="company"]',
                    'div[class*="organization"]',
                    'div[class*="about"]',
                    'div[class*="industry"]'
                ]
                
                for selector in company_selectors:
                    try:
                        element = await self.page.query_selector(selector)
                        if element:
                            analysis['has_company_content'] = True
                            analysis['page_type'] = 'company_page'
                            break
                    except:
                        continue
                
                # Check for post content
                post_selectors = [
                    'div[class*="post"]',
                    'div[data-test-id="post"]',
                    'article',
                    'div[class*="feed"]',
                    'div[class*="content"]'
                ]
                
                for selector in post_selectors:
                    try:
                        elements = await self.page.query_selector_all(selector)
                        if len(elements) > 0:
                            analysis['has_post_content'] = True
                            analysis['page_type'] = 'post_page'
                            break
                    except:
                        continue
                
                # Check for newsletter content
                newsletter_selectors = [
                    'div[class*="newsletter"]',
                    'div[data-test-id="newsletter"]',
                    'div[class*="article"]',
                    'div[class*="content"]'
                ]
                
                for selector in newsletter_selectors:
                    try:
                        element = await self.page.query_selector(selector)
                        if element:
                            analysis['has_newsletter_content'] = True
                            analysis['page_type'] = 'newsletter_page'
                            break
                    except:
                        continue
            
            # Final fallback for login pages
            if analysis['page_type'] == 'unknown' and analysis['has_login_form']:
                analysis['page_type'] = 'login_page'
                
        except Exception as e:
            print(f"Error analyzing LinkedIn content: {e}")
            
        return analysis
    
    async def _check_for_generic_content(self) -> dict:
        """Check for generic content and elements"""
        analysis = {
            'has_elements': False,
            'has_login_form': False,
            'has_content': False,
            'page_type': 'unknown'
        }
        
        try:
            # Generic content detection
            content_selectors = [
                'div[class*="content"]',
                'div[class*="main"]',
                'div[class*="body"]',
                'article',
                'section'
            ]
            
            for selector in content_selectors:
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        analysis['has_elements'] = True
                        analysis['has_content'] = True
                        break
                except:
                    continue
            
            # Check for login form
            login_selectors = [
                'form[action*="login"]',
                'input[name="username"]',
                'input[name="password"]',
                'input[name="email"]',
                'button[type="submit"]'
            ]
            
            for selector in login_selectors:
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        analysis['has_login_form'] = True
                        break
                except:
                    continue
            
            # Determine page type
            if analysis['has_login_form']:
                analysis['page_type'] = 'login_page'
            elif analysis['has_content']:
                analysis['page_type'] = 'content_page'
            else:
                analysis['page_type'] = 'unknown'
                
        except Exception as e:
            print(f"Error analyzing generic content: {e}")
            
        return analysis
        
    async def get_page_metadata(self) -> dict:
        """Get comprehensive page metadata"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
            
        metadata = {
            'title': await self.page.title(),
            'url': self.page.url,
            'content_length': 0,
            'rendered_text_length': 0,
            'has_javascript': False,
            'load_time': 0
        }
        
        try:
            # Get content lengths
            content = await self.page.content()
            metadata['content_length'] = len(content)
            
            rendered_text = await self.page.text_content('body')
            metadata['rendered_text_length'] = len(rendered_text)
            
            # Check for JavaScript
            js_elements = await self.page.query_selector_all('script')
            metadata['has_javascript'] = len(js_elements) > 0
            
        except Exception as e:
            print(f"Error getting metadata: {e}")
            
        return metadata
        
    async def get_network_logs(self) -> list:
        """Get network request logs for analysis"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
        
        # This is a simplified version - in a real implementation,
        # you'd want to capture network events during navigation
        return []
        
    async def take_screenshot(self, path: str) -> None:
        """Take screenshot for debugging"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
        await self.page.screenshot(path=path)
        
    async def take_full_page_screenshot(self, path: str) -> None:
        """Take full page screenshot including scrollable content"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
        await self.page.screenshot(path=path, full_page=True)
    
    async def execute_human_scroll(self, target_position: int, current_position: int = None) -> None:
        """Execute human-like scrolling behavior"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
        
        if current_position is None:
            current_position = await self.page.evaluate("window.pageYOffset")
        
        if self.enable_anti_detection and self.anti_detection:
            await execute_human_behavior(
                self.page, 
                self.anti_detection, 
                'scroll', 
                position=target_position, 
                current_position=current_position
            )
        else:
            # Simple scroll without anti-detection
            await self.page.evaluate(f"window.scrollTo(0, {target_position})")
            await asyncio.sleep(random.uniform(0.5, 1.5))
    
    async def execute_human_mouse_move(self, x: int, y: int) -> None:
        """Execute human-like mouse movement"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
        
        if self.enable_anti_detection and self.anti_detection:
            await execute_human_behavior(
                self.page, 
                self.anti_detection, 
                'mousemove', 
                position=(x, y)
            )
        else:
            # Simple mouse movement without anti-detection
            await self.page.mouse.move(x, y)
            await asyncio.sleep(random.uniform(0.1, 0.3))
    
    async def execute_human_click(self, x: int, y: int) -> None:
        """Execute human-like click behavior"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
        
        if self.enable_anti_detection and self.anti_detection:
            await execute_human_behavior(
                self.page, 
                self.anti_detection, 
                'click', 
                position=(x, y)
            )
        else:
            # Simple click without anti-detection
            await self.page.mouse.click(x, y)
            await asyncio.sleep(random.uniform(0.2, 0.5))
    
    async def get_stealth_report(self) -> Dict[str, Any]:
        """Get comprehensive stealth report"""
        if self.enable_anti_detection and self.anti_detection:
            return await self.anti_detection.get_stealth_report()
        else:
            return {
                'anti_detection_enabled': False,
                'message': 'Anti-detection features are disabled'
            }

    def detect_url_type(self, url: str) -> str:
        """Detect the type of URL based on LinkedIn patterns"""
        url_lower = url.lower()
        
        # LinkedIn URL patterns
        if '/company/' in url_lower:
            return 'company'
        elif '/newsletters/' in url_lower:
            return 'newsletter'
        elif '/in/' in url_lower:
            return 'profile'
        elif '/posts/' in url_lower:
            return 'post'
        else:
            return 'unknown'


async def test_browser_manager():
    """Test function for Task 1: Basic Infrastructure with detailed debugging"""
    print("=" * 80)
    print("TESTING BROWSER MANAGER - DETAILED DEBUGGING")
    print("=" * 80)
    
    # Test with LinkedIn platform
    manager = BrowserManager(headless=False, platform="linkedin")  # Set to False to see what's happening
    
    try:
        # Test 1: Browser Startup
        print("\n1. TESTING BROWSER STARTUP...")
        await manager.start()
        print("✓ Browser started successfully")
        print(f"  - Platform: {manager.platform}")
        print(f"  - Headless mode: {manager.headless}")
        
        # Test 2: Navigation
        print("\n2. TESTING NAVIGATION...")
        target_url = "https://www.linkedin.com/in/williamhgates/"
        print(f"  - Target URL: {target_url}")
        
        # Use the new navigation method that attempts to close popup
        popup_closed = await manager.navigate_to_with_popup_close(target_url)
        print("✓ Navigation completed")
        print(f"  - Popup closed: {popup_closed}")
        
        # Get current URL to see if we were redirected
        current_url = await manager.get_page_url()
        print(f"  - Current URL: {current_url}")
        
        # Test 3: Page Metadata Analysis
        print("\n3. PAGE METADATA ANALYSIS...")
        metadata = await manager.get_page_metadata()
        print(f"  - Page Title: '{metadata['title']}'")
        print(f"  - HTML Content Length: {metadata['content_length']:,} characters")
        print(f"  - Rendered Text Length: {metadata['rendered_text_length']:,} characters")
        print(f"  - Has JavaScript: {metadata['has_javascript']}")
        
        # Test 4: Content Analysis
        print("\n4. CONTENT ANALYSIS...")
        
        # Get HTML content
        html_content = await manager.get_page_content()
        print(f"  - Raw HTML extracted: {len(html_content):,} characters")
        
        # Show first 500 characters of HTML
        print("\n  HTML Preview (first 500 chars):")
        print("  " + "-" * 50)
        print("  " + html_content[:500].replace('\n', '\n  '))
        print("  " + "-" * 50)
        
        # Get rendered text content
        rendered_text = await manager.get_rendered_text()
        print(f"\n  - Rendered text extracted: {len(rendered_text):,} characters")
        
        # Show first 500 characters of rendered text
        print("\n  Rendered Text Preview (first 500 chars):")
        print("  " + "-" * 50)
        print("  " + rendered_text[:500].replace('\n', '\n  '))
        print("  " + "-" * 50)
        
        # Test 5: LinkedIn-Specific Analysis
        print("\n5. LINKEDIN CONTENT ANALYSIS...")
        
        # Check if popup is still visible
        popup_visible = await manager.check_popup_visible()
        print(f"  - Popup still visible: {popup_visible}")
        
        linkedin_analysis = await manager.check_for_platform_content()
        print(f"  - Has LinkedIn Elements: {linkedin_analysis['has_linkedin_elements']}")
        print(f"  - Has Login Form: {linkedin_analysis['has_login_form']}")
        print(f"  - Has Profile Content: {linkedin_analysis['has_profile_content']}")
        print(f"  - Has Company Content: {linkedin_analysis['has_company_content']}")
        print(f"  - Has Post Content: {linkedin_analysis['has_post_content']}")
        print(f"  - Has Newsletter Content: {linkedin_analysis['has_newsletter_content']}")
        print(f"  - Page Type: {linkedin_analysis['page_type']}")
        
        # Test 6: URL Type Detection
        print("\n6. URL TYPE DETECTION...")
        url_type = manager.detect_url_type(target_url)
        print(f"  - Detected URL Type: {url_type}")
        
        # Test 7: Screenshots
        print("\n7. SCREENSHOT TESTING...")
        
        # Take regular screenshot
        screenshot_path = "test_linkedin_screenshot.png"
        await manager.take_screenshot(screenshot_path)
        print(f"  ✓ Regular screenshot saved: {screenshot_path}")
        
        # Take full page screenshot
        full_screenshot_path = "test_linkedin_screenshot_full.png"
        await manager.take_full_page_screenshot(full_screenshot_path)
        print(f"  ✓ Full page screenshot saved: {full_screenshot_path}")
        
        print("\n" + "=" * 80)
        print("TEST SUMMARY")
        print("=" * 80)
        print(f"✓ Browser automation: WORKING")
        print(f"✓ Navigation: WORKING (reached: {current_url})")
        print(f"✓ Popup handling: {'SUCCESS' if popup_closed else 'FAILED'}")
        print(f"✓ Content extraction: WORKING ({metadata['content_length']:,} chars)")
        print(f"✓ JavaScript rendering: {'WORKING' if metadata['has_javascript'] else 'NOT DETECTED'}")
        print(f"✓ Screenshots: WORKING (2 files created)")
        print(f"✓ LinkedIn detection: {linkedin_analysis['page_type'].upper()}")
        print(f"✓ URL type detection: {url_type.upper()}")
        
        if popup_visible:
            print("⚠️  NOTE: LinkedIn popup is still visible - content access limited")
            print("   LinkedIn requires authentication or popup closure to view full content")
        elif linkedin_analysis['page_type'] == 'login_page':
            print("⚠️  NOTE: LinkedIn is showing login page - this is expected behavior")
            print("   LinkedIn requires authentication to view profile content")
        elif linkedin_analysis['page_type'] == 'profile_page':
            print("✓ SUCCESS: LinkedIn profile content detected!")
        elif linkedin_analysis['has_post_content']:
            print("✓ SUCCESS: LinkedIn post content detected!")
        else:
            print("⚠️  NOTE: LinkedIn content type unclear - check screenshots for details")
        
        print("\nTask 1: Basic Infrastructure - PASSED")
        
    except Exception as e:
        print(f"\n❌ Task 1: Basic Infrastructure - FAILED: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        await manager.stop()
        print("\n✓ Browser cleanup completed")
        print("\n📁 Check the following files for visual verification:")
        print("   - test_linkedin_screenshot.png (viewport screenshot)")
        print("   - test_linkedin_screenshot_full.png (full page screenshot)")


async def test_all_linkedin_urls():
    """Test browser manager with different LinkedIn URL types"""
    print("=" * 80)
    print("TESTING ALL LINKEDIN URL TYPES")
    print("=" * 80)
    
    # Different LinkedIn URL types to test
    test_urls = [
        {
            "type": "Profile",
            "url": "https://www.linkedin.com/in/williamhgates/",
            "expected": "profile_page"
        },
        {
            "type": "Company",
            "url": "https://www.linkedin.com/company/microsoft/",
            "expected": "company_page"
        },
        {
            "type": "Post",
            "url": "https://www.linkedin.com/posts/aiqod_inside-aiqod-how-were-building-enterprise-ready-activity-7348224698146541568-N7oQ",
            "expected": "post_page"
        },
        {
            "type": "Newsletter",
            "url": "https://www.linkedin.com/newsletters/aiqod-insider-7325820451622940672",
            "expected": "newsletter_page"
        }
    ]
    
    manager = BrowserManager(headless=False, platform="linkedin")
    
    try:
        await manager.start()
        print("✓ Browser started successfully")
        
        results = []
        
        for i, test_case in enumerate(test_urls, 1):
            print(f"\n{'='*60}")
            print(f"TEST {i}: {test_case['type']}")
            print(f"URL: {test_case['url']}")
            print(f"Expected: {test_case['expected']}")
            print(f"{'='*60}")
            
            try:
                # Navigate and close popup
                popup_closed = await manager.navigate_to_with_popup_close(test_case['url'])
                current_url = await manager.get_page_url()
                
                # Get metadata
                metadata = await manager.get_page_metadata()
                rendered_text = await manager.get_rendered_text()
                
                # Check popup status
                popup_visible = await manager.check_popup_visible()
                
                # Analyze LinkedIn content
                linkedin_analysis = await manager.check_for_platform_content()
                
                # Detect URL type
                url_type = manager.detect_url_type(test_case['url'])
                
                # Take screenshot
                screenshot_path = f"test_{test_case['type'].replace(' ', '_').lower()}.png"
                await manager.take_screenshot(screenshot_path)
                
                # Store results
                result = {
                    "type": test_case['type'],
                    "url": test_case['url'],
                    "current_url": current_url,
                    "popup_closed": popup_closed,
                    "popup_visible": popup_visible,
                    "page_title": metadata['title'],
                    "content_length": metadata['content_length'],
                    "text_length": metadata['rendered_text_length'],
                    "page_type": linkedin_analysis['page_type'],
                    "detected_url_type": url_type,
                    "has_profile": linkedin_analysis['has_profile_content'],
                    "has_company": linkedin_analysis['has_company_content'],
                    "has_post": linkedin_analysis['has_post_content'],
                    "has_newsletter": linkedin_analysis['has_newsletter_content'],
                    "screenshot": screenshot_path,
                    "success": popup_closed and not popup_visible
                }
                
                results.append(result)
                
                # Print summary
                print(f"✓ Navigation: {'SUCCESS' if popup_closed else 'FAILED'}")
                print(f"✓ Popup Status: {'CLOSED' if not popup_visible else 'VISIBLE'}")
                print(f"✓ Page Type: {linkedin_analysis['page_type']}")
                print(f"✓ Detected URL Type: {url_type}")
                print(f"✓ Content Length: {metadata['content_length']:,} chars")
                print(f"✓ Screenshot: {screenshot_path}")
                
                # Show content preview
                preview = rendered_text[:200].replace('\n', ' ').strip()
                print(f"✓ Content Preview: {preview}...")
                
            except Exception as e:
                print(f"❌ Error testing {test_case['type']}: {e}")
                results.append({
                    "type": test_case['type'],
                    "url": test_case['url'],
                    "error": str(e),
                    "success": False
                })
        
        # Print final summary
        print(f"\n{'='*80}")
        print("FINAL TEST SUMMARY")
        print(f"{'='*80}")
        
        successful_tests = [r for r in results if r.get('success', False)]
        failed_tests = [r for r in results if not r.get('success', False)]
        
        print(f"✓ Successful Tests: {len(successful_tests)}/{len(results)}")
        print(f"❌ Failed Tests: {len(failed_tests)}/{len(results)}")
        
        print(f"\nSUCCESSFUL TESTS:")
        for result in successful_tests:
            print(f"  ✓ {result['type']}: {result['page_type']} ({result['content_length']:,} chars)")
        
        if failed_tests:
            print(f"\nFAILED TESTS:")
            for result in failed_tests:
                error = result.get('error', 'Unknown error')
                print(f"  ❌ {result['type']}: {error}")
        
        print(f"\n📁 Screenshots saved:")
        for result in results:
            if 'screenshot' in result:
                print(f"  - {result['screenshot']}")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await manager.stop()
        print("\n✓ Browser cleanup completed")


if __name__ == "__main__":
    # Uncomment the test you want to run
    # asyncio.run(test_browser_manager())  # Original detailed test
    asyncio.run(test_all_linkedin_urls())  # Test all URL types 