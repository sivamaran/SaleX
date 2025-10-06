"""
Browser Manager - Task 1: Basic Infrastructure with Anti-Detection
Handles browser automation with comprehensive stealth configuration
"""

import asyncio
import random
import time
from typing import Optional, Dict, Any
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from fake_useragent import UserAgent
from instagram_scraper.src.anti_detection import AntiDetectionManager, create_stealth_browser_context, execute_human_behavior


class BrowserManager:
    """Manages browser automation with comprehensive anti-detection features"""
    
    def __init__(self, headless: bool = True, enable_anti_detection: bool = True, is_mobile: bool = False):
        self.headless = headless
        self.enable_anti_detection = enable_anti_detection
        self.is_mobile = is_mobile
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
                '--disable-features=VizDisplayCompositor',
                '--disable-extensions',
                '--disable-plugins',
                '--disable-images',
            ]
            
            self.browser = await self.playwright.chromium.launch(
                headless=self.headless,
                args=browser_args
            )
            
            self.context = await self.browser.new_context(
                user_agent=self.ua.random,
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
        
        # Set additional headers
        await self.page.set_extra_http_headers({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
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
            
    async def navigate_to(self, url: str, wait_time: int = 3, max_retries: int = 3) -> None:
        """Navigate to URL with human-like delays, anti-detection measures, and robust retry logic"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
        
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                # Apply network obfuscation delay with exponential backoff
                if self.enable_anti_detection and self.anti_detection:
                    delay = await self.anti_detection.calculate_request_delay()
                    # Add exponential backoff for retries
                    if attempt > 0:
                        delay *= (2 ** attempt)  # Exponential backoff
                    await asyncio.sleep(delay)
                else:
                    # Random delay to mimic human behavior with backoff
                    base_delay = random.uniform(1, 3)
                    if attempt > 0:
                        base_delay *= (2 ** attempt)  # Exponential backoff
                    await asyncio.sleep(base_delay)
                
                # Enhanced navigation with better error handling
                navigation_options = {
                    'wait_until': 'domcontentloaded',
                    'timeout': 45000,  # Increased timeout
                }
                
                # Add retry-specific options
                if attempt > 0:
                    navigation_options['wait_until'] = 'networkidle'  # More thorough wait on retries
                    navigation_options['timeout'] = 60000  # Longer timeout for retries
                
                await self.page.goto(url, **navigation_options)
                
                # Update request count for anti-detection tracking
                if self.enable_anti_detection and self.anti_detection:
                    self.anti_detection.request_count += 1
                    self.anti_detection.last_request_time = time.time()
                
                # Wait for page to load
                await asyncio.sleep(wait_time)
                return  # Success, exit retry loop
                
            except Exception as e:
                last_exception = e
                error_msg = str(e)
                
                # Check if it's a network error that we should retry
                if any(err in error_msg for err in [
                    "ERR_CONNECTION_RESET", "net::ERR_", "ERR_NETWORK_CHANGED", 
                    "ERR_INTERNET_DISCONNECTED", "ERR_CONNECTION_REFUSED",
                    "ERR_CONNECTION_TIMED_OUT", "ERR_NAME_NOT_RESOLVED"
                ]):
                    print(f"⚠️ Network error navigating to {url} (attempt {attempt + 1}/{max_retries}): {error_msg}")
                    
                    if attempt < max_retries - 1:
                        # Enhanced retry logic for server environments
                        base_delay = 5.0  # Start with 5 seconds
                        retry_delay = min(60, base_delay * (2 ** attempt) + random.uniform(2, 8))  # Cap at 60 seconds with jitter
                        print(f"   Retrying in {retry_delay:.1f} seconds...")
                        await asyncio.sleep(retry_delay)
                        
                        # Try multiple recovery strategies
                        recovery_success = False
                        
                        # Strategy 1: Try to refresh the page context
                        try:
                            await self.page.reload(wait_until='domcontentloaded', timeout=30000)
                            recovery_success = True
                            print(f"   ✓ Page reload successful")
                        except Exception as reload_error:
                            print(f"   ⚠️ Page reload failed: {reload_error}")
                        
                        # Strategy 2: If reload failed, try creating a new page
                        if not recovery_success:
                            try:
                                await self.page.close()
                                self.page = await self.context.new_page()
                                print(f"   ✓ New page created")
                                recovery_success = True
                            except Exception as new_page_error:
                                print(f"   ⚠️ New page creation failed: {new_page_error}")
                        
                        # Strategy 3: If still failing, try recreating context
                        if not recovery_success and attempt >= 2:
                            try:
                                await self.context.close()
                                self.context = await self.browser.new_context(
                                    user_agent=self.ua.random,
                                    viewport={'width': 1920, 'height': 1080},
                                    locale='en-US',
                                    timezone_id='America/New_York',
                                )
                                self.page = await self.context.new_page()
                                print(f"   ✓ New context created")
                                recovery_success = True
                            except Exception as context_error:
                                print(f"   ⚠️ Context recreation failed: {context_error}")
                        
                        if recovery_success:
                            continue
                        else:
                            print(f"   ❌ All recovery strategies failed")
                    else:
                        print(f"❌ Max retries reached for {url}")
                        break
                else:
                    # Non-network error, don't retry
                    print(f"❌ Non-network error navigating to {url}: {error_msg}")
                    break
        
        # If we get here, all retries failed
        if last_exception:
            raise last_exception
        
    async def close_instagram_popup(self) -> bool:
        """Attempt to close Instagram login/signup popup"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
            
        try:
            # Wait a bit for popup to load
            await asyncio.sleep(2)
            
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
            
            for selector in close_selectors:
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
                            print(f"  - Popup successfully closed")
                            return True
                        else:
                            print(f"  - Popup may still be visible, trying next selector")
                            
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
                print(f"  - Popup closed with Escape key")
                return True
            else:
                print(f"  - Popup still visible after Escape key")
                return False
                
        except Exception as e:
            print(f"  - Error closing popup: {e}")
            return False
            
    async def navigate_to_with_popup_close(self, url: str, wait_time: int = 3) -> bool:
        """Navigate to URL and attempt to close any popup"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
            
        # Navigate to URL
        await self.navigate_to(url, wait_time)
        
        # Try to close popup
        popup_closed = await self.close_instagram_popup()
        
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
        """Check if Instagram popup is still visible"""
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
            
            for selector in popup_selectors:
                element = await self.page.query_selector(selector)
                if element:
                    return True
                    
            return False
            
        except Exception as e:
            print(f"Error checking popup visibility: {e}")
            return False
            
    async def check_for_instagram_content(self) -> dict:
        """Check for Instagram-specific content and elements"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
            
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


async def test_browser_manager():
    """Test function for Task 1: Basic Infrastructure with detailed debugging"""
    print("=" * 80)
    print("TESTING BROWSER MANAGER - DETAILED DEBUGGING")
    print("=" * 80)
    
    manager = BrowserManager(headless=False)  # Set to False to see what's happening
    
    try:
        # Test 1: Browser Startup
        print("\n1. TESTING BROWSER STARTUP...")
        await manager.start()
        print("✓ Browser started successfully")
        print(f"  - User Agent: {manager.ua.random}")
        print(f"  - Headless mode: {manager.headless}")
        
        # Test 2: Navigation
        print("\n2. TESTING NAVIGATION...")
        #target_url = "https://www.instagram.com/tarinipeshawaria/?hl=en"
        #target_url = "https://www.instagram.com/travelandleisure/?hl=en"
        target_url = "https://www.instagram.com/reel/CSb6-Rap2Ip/"
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
        
        # Test 5: Instagram-Specific Analysis
        print("\n5. INSTAGRAM CONTENT ANALYSIS...")
        
        # Check if popup is still visible
        popup_visible = await manager.check_popup_visible()
        print(f"  - Popup still visible: {popup_visible}")
        
        instagram_analysis = await manager.check_for_instagram_content()
        print(f"  - Has Instagram Elements: {instagram_analysis['has_instagram_elements']}")
        print(f"  - Has Login Form: {instagram_analysis['has_login_form']}")
        print(f"  - Has Profile Content: {instagram_analysis['has_profile_content']}")
        print(f"  - Has Posts: {instagram_analysis['has_posts']}")
        print(f"  - Page Type: {instagram_analysis['page_type']}")
        
        # Additional analysis based on popup status
        if popup_visible:
            print("  ⚠️  Popup is blocking content access")
        else:
            print("  ✓ Popup closed - content should be accessible")
        
        # Test 6: JavaScript Content Detection
        print("\n6. JAVASCRIPT CONTENT DETECTION...")
        
        # Check if we can see dynamic content
        try:
            # Wait a bit more for JavaScript to load
            await asyncio.sleep(5)
            
            # Get content again after waiting
            updated_content = await manager.get_page_content()
            updated_text = await manager.get_rendered_text()
            
            print(f"  - Content after 5s wait: {len(updated_content):,} chars")
            print(f"  - Text after 5s wait: {len(updated_text):,} chars")
            
            if len(updated_text) > len(rendered_text):
                print("  ✓ JavaScript content detected - text content increased")
            else:
                print("  - No additional JavaScript content detected")
                
        except Exception as e:
            print(f"  - Error checking JavaScript content: {e}")
        
        # Test 7: Screenshots
        print("\n7. SCREENSHOT TESTING...")
        
        # Take regular screenshot
        screenshot_path = "test_screenshot.png"
        await manager.take_screenshot(screenshot_path)
        print(f"  ✓ Regular screenshot saved: {screenshot_path}")
        
        # Take full page screenshot
        full_screenshot_path = "test_screenshot_full.png"
        await manager.take_full_page_screenshot(full_screenshot_path)
        print(f"  ✓ Full page screenshot saved: {full_screenshot_path}")
        
        # Test 8: Content Comparison
        print("\n8. CONTENT COMPARISON ANALYSIS...")
        
        # Check if content contains Instagram-specific text
        instagram_keywords = ['instagram', 'follow', 'like', 'comment', 'share', 'post', 'story']
        found_keywords = []
        
        for keyword in instagram_keywords:
            if keyword.lower() in rendered_text.lower():
                found_keywords.append(keyword)
        
        print(f"  - Instagram keywords found: {found_keywords}")
        
        # Check for login-related content
        login_keywords = ['log in', 'sign in', 'username', 'password', 'login']
        login_found = any(keyword in rendered_text.lower() for keyword in login_keywords)
        print(f"  - Login form detected: {login_found}")
        
        # Check for profile content
        profile_keywords = ['followers', 'following', 'posts', 'bio', 'profile']
        profile_found = any(keyword in rendered_text.lower() for keyword in profile_keywords)
        print(f"  - Profile content detected: {profile_found}")
        
        print("\n" + "=" * 80)
        print("TEST SUMMARY")
        print("=" * 80)
        print(f"✓ Browser automation: WORKING")
        print(f"✓ Navigation: WORKING (reached: {current_url})")
        print(f"✓ Popup handling: {'SUCCESS' if popup_closed else 'FAILED'}")
        print(f"✓ Content extraction: WORKING ({metadata['content_length']:,} chars)")
        print(f"✓ JavaScript rendering: {'WORKING' if metadata['has_javascript'] else 'NOT DETECTED'}")
        print(f"✓ Screenshots: WORKING (2 files created)")
        print(f"✓ Instagram detection: {instagram_analysis['page_type'].upper()}")
        
        if popup_visible:
            print("⚠️  NOTE: Instagram popup is still visible - content access limited")
            print("   Instagram requires authentication or popup closure to view full content")
        elif instagram_analysis['page_type'] == 'login_page':
            print("⚠️  NOTE: Instagram is showing login page - this is expected behavior")
            print("   Instagram requires authentication to view profile content")
        elif instagram_analysis['page_type'] == 'profile_page':
            print("✓ SUCCESS: Instagram profile content detected!")
        elif instagram_analysis['has_posts']:
            print("✓ SUCCESS: Instagram post/reel content detected!")
        else:
            print("⚠️  NOTE: Instagram content type unclear - check screenshots for details")
        
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
        print("   - test_screenshot.png (viewport screenshot)")
        print("   - test_screenshot_full.png (full page screenshot)")


async def test_all_instagram_urls():
    """Test browser manager with different Instagram URL types"""
    print("=" * 80)
    print("TESTING ALL INSTAGRAM URL TYPES")
    print("=" * 80)
    
    # Different Instagram URL types to test
    '''
    test_urls = [
        {
            "type": "User Account",
            "url": "https://www.instagram.com/tarinipeshawaria/?hl=en",
            "expected": "profile_page"
        },
        {
            "type": "Reel (Simple)",
            "url": "https://www.instagram.com/reel/CSb6-Rap2Ip/",
            "expected": "feed_page"
        },
        {
            "type": "Reel (With User)",
            "url": "https://www.instagram.com/traveltomtom/reel/DMpdea2sPwT/?hl=en",
            "expected": "feed_page"
        },
        {
            "type": "Post (With User)",
            "url": "https://www.instagram.com/travelandleisure/p/DMsKjDfROwu/?hl=en",
            "expected": "feed_page"
        },
        {
            "type": "Post (Simple)",
            "url": "https://www.instagram.com/p/DMsercXMVeZ/",
            "expected": "feed_page"
        }
    ]
    '''
    test_urls = [
        {
            "type": "User Account",
            "url": "https://www.instagram.com/tarinipeshawaria/?hl=en",
            "expected": "profile_page"
        },
        {
            "type": "Post (Simple)",
            "url": "https://www.instagram.com/p/DMsercXMVeZ/",
            "expected": "feed_page"
        },
        {
            "type": "Reel (Simple)",
            "url": "https://www.instagram.com/reel/CSb6-Rap2Ip/",
            "expected": "feed_page"
        }
    ]
    manager = BrowserManager(headless=False)
    
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
                
                # Analyze Instagram content
                instagram_analysis = await manager.check_for_instagram_content()
                
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
                    "page_type": instagram_analysis['page_type'],
                    "has_posts": instagram_analysis['has_posts'],
                    "has_profile": instagram_analysis['has_profile_content'],
                    "screenshot": screenshot_path,
                    "success": popup_closed and not popup_visible
                }
                
                results.append(result)
                
                # Print summary
                print(f"✓ Navigation: {'SUCCESS' if popup_closed else 'FAILED'}")
                print(f"✓ Popup Status: {'CLOSED' if not popup_visible else 'VISIBLE'}")
                print(f"✓ Page Type: {instagram_analysis['page_type']}")
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
    asyncio.run(test_all_instagram_urls())  # Test all URL types 