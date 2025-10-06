"""
YouTube Browser Manager - Task 1: Basic Infrastructure with Anti-Detection
Handles browser automation with comprehensive stealth configuration for YouTube
"""

import asyncio
import random
import time
from typing import Optional, Dict, Any
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from fake_useragent import UserAgent
from yt_scraper.anti_detection import AntiDetectionManager, create_stealth_browser_context, execute_human_behavior


class YouTubeBrowserManager:
    """Manages browser automation with comprehensive anti-detection features for YouTube"""
    
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
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor',
                '--disable-extensions',
                '--disable-plugins',
                '--disable-images',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-renderer-backgrounding',
                '--disable-notifications'
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
                permissions=['geolocation', 'notifications'],
                extra_http_headers={
                    'Accept-Language': 'en-US,en;q=0.9'
                }
            )
            
            # Add basic stealth scripts for YouTube
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
                
                // YouTube-specific optimizations
                Object.defineProperty(navigator, 'maxTouchPoints', {
                    get: () => 0,
                });
                
                Object.defineProperty(screen, 'colorDepth', {
                    get: () => 24,
                });
            """)
        
        self.page = await self.context.new_page()
        
        # Set additional headers for YouTube
        await self.page.set_extra_http_headers({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
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
            
    async def navigate_to(self, url: str, wait_time: int = 5) -> None:
        """Navigate to URL with human-like delays and anti-detection measures"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
        
        # Apply network obfuscation delay
        if self.enable_anti_detection and self.anti_detection:
            delay = await self.anti_detection.calculate_request_delay()
            await asyncio.sleep(delay)
        else:
            # Random delay to mimic human behavior
            await asyncio.sleep(random.uniform(1, 3))
        
        try:
            await self.page.goto(url, wait_until='networkidle', timeout=30000)
        except Exception:
            # Fallback to domcontentloaded if networkidle fails
            await self.page.goto(url, wait_until='domcontentloaded', timeout=20000)
        
        # Update request count for anti-detection tracking
        if self.enable_anti_detection and self.anti_detection:
            self.anti_detection.request_count += 1
            self.anti_detection.last_request_time = time.time()
        
        # Wait for page to load
        await asyncio.sleep(wait_time)
        
    async def close_youtube_popups(self) -> bool:
        """Attempt to close YouTube popups (cookies, notifications, etc.)"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
            
        try:
            # Wait a bit for popups to load
            await asyncio.sleep(3)
            
            popups_closed = False
            
            # Common selectors for YouTube popup close buttons
            close_selectors = [
                # Cookie consent
                'button[aria-label="Accept all"]',
                'button[aria-label="Accept the use of cookies and other data for the purposes described"]',
                'button:has-text("Accept all")',
                'button:has-text("I agree")',
                'tp-yt-paper-button:has-text("ACCEPT ALL")',
                
                # Notification popups
                'button[aria-label="No thanks"]',
                'button[aria-label="Not now"]',
                'button:has-text("No thanks")',
                'button:has-text("Not now")',
                'yt-button-renderer:has-text("No thanks")',
                
                # Generic close buttons
                'button[aria-label="Close"]',
                'button[aria-label="Dismiss"]',
                'button[title="Close"]',
                'button[title="Dismiss"]',
                'yt-icon-button[aria-label="Close"]',
                'yt-icon-button[aria-label="Dismiss"]',
                
                # YouTube-specific close buttons
                'ytd-button-renderer[aria-label="No thanks"]',
                'ytd-button-renderer[aria-label="Not now"]',
                'paper-button:has-text("No thanks")',
                'paper-button:has-text("Not now")',
                
                # Cookie banner specific
                '#dialog button:has-text("Accept")',
                '#dialog button:has-text("OK")',
                '.consent-bump-lightbox button:has-text("I AGREE")',
                
                # Ad overlay close buttons
                '.ytp-ad-overlay-close-button',
                '.ytp-ad-skip-button-modern',
                'button.ytp-ad-skip-button'
            ]
            
            for selector in close_selectors:
                try:
                    # Wait a bit for elements to be ready
                    await asyncio.sleep(1)
                    
                    # Check if element exists and is visible
                    element = await self.page.query_selector(selector)
                    if element:
                        is_visible = await element.is_visible()
                        if is_visible:
                            print(f"  - Found popup close button with selector: {selector}")
                            
                            # Click the close button
                            await element.click()
                            print(f"  - Clicked popup close button")
                            popups_closed = True
                            
                            # Wait for popup to close
                            await asyncio.sleep(2)
                            
                except Exception as e:
                    # Continue with next selector if this one fails
                    continue
            
            # Try pressing Escape key as fallback
            if not popups_closed:
                print(f"  - No popup close buttons found, trying Escape key")
                await self.page.keyboard.press('Escape')
                await asyncio.sleep(2)
                popups_closed = True
            
            return popups_closed
                
        except Exception as e:
            print(f"  - Error closing popups: {e}")
            return False
            
    async def navigate_to_with_popup_close(self, url: str, wait_time: int = 5) -> bool:
        """Navigate to URL and attempt to close any popups"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
            
        # Navigate to URL
        await self.navigate_to(url, wait_time)
        
        # Try to close popups
        popup_closed = await self.close_youtube_popups()
        
        return popup_closed
        
    async def get_page_content(self) -> str:
        """Get current page HTML content"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
        return await self.page.content()
        
    async def get_rendered_text(self) -> str:
        """Get text content after JavaScript rendering"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
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
        """Check if YouTube popups are still visible"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
            
        try:
            # Check for common popup indicators
            popup_selectors = [
                '[role="dialog"]',
                '.consent-bump-lightbox',
                '#dialog',
                'ytd-consent-bump-lightbox-renderer',
                'tp-yt-paper-dialog',
                'ytd-popup-container',
                '.ytp-ad-overlay-container'
            ]
            
            for selector in popup_selectors:
                element = await self.page.query_selector(selector)
                if element:
                    is_visible = await element.is_visible()
                    if is_visible:
                        return True
                    
            return False
            
        except Exception as e:
            print(f"Error checking popup visibility: {e}")
            return False
            
    async def check_for_youtube_content(self) -> dict:
        """Check for YouTube-specific content and elements"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
            
        analysis = {
            'has_youtube_elements': False,
            'has_video_player': False,
            'has_channel_content': False,
            'has_shorts': False,
            'has_video_info': False,
            'page_type': 'unknown'
        }
        
        try:
            # Check for YouTube-specific elements
            youtube_selectors = [
                '#player',
                '#movie_player',
                'ytd-watch-flexy',
                'ytd-browse',
                'ytd-shorts',
                'ytd-channel-header-renderer',
                'ytd-video-details-renderer',
                '#meta',
                '#info',
                '#description'
            ]
            
            for selector in youtube_selectors:
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        analysis['has_youtube_elements'] = True
                        break
                except:
                    continue
            
            # Check for video player
            player_selectors = [
                '#player',
                '#movie_player',
                '.html5-video-player',
                'video'
            ]
            
            for selector in player_selectors:
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        analysis['has_video_player'] = True
                        break
                except:
                    continue
            
            # Check for channel content
            channel_selectors = [
                'ytd-channel-header-renderer',
                '#channel-header',
                'yt-formatted-string#subscriber-count',
                '#subscriber-count',
                'ytd-c4-tabbed-header-renderer'
            ]
            
            for selector in channel_selectors:
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        analysis['has_channel_content'] = True
                        break
                except:
                    continue
            
            # Check for video info
            video_info_selectors = [
                'ytd-video-primary-info-renderer',
                '#info',
                '#meta',
                'ytd-video-details-renderer',
                'h1.ytd-video-primary-info-renderer'
            ]
            
            for selector in video_info_selectors:
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        analysis['has_video_info'] = True
                        break
                except:
                    continue
            
            # Check for Shorts
            shorts_selectors = [
                'ytd-shorts',
                'ytd-reel-video-renderer',
                '#shorts-player',
                '[is-shorts]'
            ]
            
            for selector in shorts_selectors:
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        analysis['has_shorts'] = True
                        break
                except:
                    continue
            
            # Determine page type based on URL and content
            current_url = self.page.url
            if '/shorts/' in current_url or analysis['has_shorts']:
                analysis['page_type'] = 'shorts_page'
            elif '/watch?v=' in current_url and analysis['has_video_player']:
                analysis['page_type'] = 'video_page'
            elif '/@' in current_url or '/channel/' in current_url or '/c/' in current_url:
                analysis['page_type'] = 'channel_page'
            elif analysis['has_channel_content']:
                analysis['page_type'] = 'channel_page'
            elif analysis['has_video_info']:
                analysis['page_type'] = 'video_page'
            elif analysis['has_youtube_elements']:
                analysis['page_type'] = 'youtube_page'
                
        except Exception as e:
            print(f"Error analyzing YouTube content: {e}")
            
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

    async def wait_for_video_load(self, timeout: int = 30) -> bool:
        """Wait for YouTube video to load"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
        
        try:
            # Wait for video player to be ready
            await self.page.wait_for_selector('#movie_player', timeout=timeout * 1000)
            
            # Wait additional time for metadata to load
            await asyncio.sleep(3)
            
            return True
        except Exception as e:
            print(f"Video load timeout: {e}")
            return False

    async def wait_for_channel_load(self, timeout: int = 30) -> bool:
        """Wait for YouTube channel page to load"""
        if not self.page:
            raise RuntimeError("Browser not started. Call start() first.")
        
        try:
            # Wait for channel header to be ready
            await self.page.wait_for_selector('ytd-channel-header-renderer', timeout=timeout * 1000)
            
            # Wait additional time for metadata to load
            await asyncio.sleep(3)
            
            return True
        except Exception as e:
            print(f"Channel load timeout: {e}")
            return False


async def test_youtube_browser_manager():
    """Test function for YouTube Browser Manager"""
    print("=" * 80)
    print("TESTING YOUTUBE BROWSER MANAGER")
    print("=" * 80)
    
    manager = YouTubeBrowserManager(headless=False)  # Set to False to see what's happening
    
    try:
        # Test 1: Browser Startup
        print("\n1. TESTING BROWSER STARTUP...")
        await manager.start()
        print("✓ Browser started successfully")
        print(f"  - User Agent: {manager.ua.random}")
        print(f"  - Headless mode: {manager.headless}")
        
        # Test different YouTube URLs
        test_urls = [
            {
                "type": "YouTube Video",
                "url": "https://www.youtube.com/watch?v=p08KNMOUD3Y",
                "expected": "video_page"
            },
            {
                "type": "YouTube Shorts",
                "url": "https://www.youtube.com/shorts/POfQdMSNpIc",
                "expected": "shorts_page"
            },
            {
                "type": "YouTube Channel",
                "url": "https://www.youtube.com/@starterstory",
                "expected": "channel_page"
            }
        ]
        
        for i, test_case in enumerate(test_urls, 2):
            print(f"\n{i}. TESTING {test_case['type'].upper()}...")
            print(f"  - Target URL: {test_case['url']}")
            
            # Navigate and close popups
            popup_closed = await manager.navigate_to_with_popup_close(test_case['url'])
            print("✓ Navigation completed")
            print(f"  - Popups handled: {popup_closed}")
            
            # Get current URL to see if we were redirected
            current_url = await manager.get_page_url()
            print(f"  - Current URL: {current_url}")
            
            # Get page metadata
            metadata = await manager.get_page_metadata()
            print(f"  - Page Title: '{metadata['title']}'")
            print(f"  - HTML Content Length: {metadata['content_length']:,} characters")
            print(f"  - Rendered Text Length: {metadata['rendered_text_length']:,} characters")
            
            # Analyze YouTube content
            youtube_analysis = await manager.check_for_youtube_content()
            print(f"  - Has YouTube Elements: {youtube_analysis['has_youtube_elements']}")
            print(f"  - Has Video Player: {youtube_analysis['has_video_player']}")
            print(f"  - Has Channel Content: {youtube_analysis['has_channel_content']}")
            print(f"  - Has Video Info: {youtube_analysis['has_video_info']}")
            print(f"  - Page Type: {youtube_analysis['page_type']}")
            
            # Take screenshot
            screenshot_path = f"test_{test_case['type'].replace(' ', '_').lower()}.png"
            await manager.take_screenshot(screenshot_path)
            print(f"  - Screenshot saved: {screenshot_path}")
        
        print(f"\n{'='*80}")
        print("YOUTUBE BROWSER MANAGER TEST COMPLETED")
        print(f"{'='*80}")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await manager.stop()
        print("\n✓ Browser cleanup completed")


if __name__ == "__main__":
    asyncio.run(test_youtube_browser_manager())