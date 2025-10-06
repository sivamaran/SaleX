from __future__ import annotations

import asyncio
import time
import random
from typing import Optional, Dict, List, Any, Tuple

from bs4 import BeautifulSoup
from loguru import logger
from playwright.async_api import Page, BrowserContext, Browser, async_playwright

from web_scraper.data_models.models import PageContent
from web_scraper.utils.anti_detection import AntiDetectionManager, create_stealth_browser_context, execute_human_behavior
from web_scraper.ai_integration.ai import extract_client_info_from_sections  # Import the AI integration

from bs4 import BeautifulSoup
from loguru import logger
from playwright.async_api import Page, BrowserContext, Browser, async_playwright

from web_scraper.data_models.models import PageContent
from web_scraper.utils.anti_detection import AntiDetectionManager, create_stealth_browser_context, execute_human_behavior
from web_scraper.ai_integration.ai import extract_client_info_from_sections  # Import the AI integration


class CompanyDirectoryScraper:
    def __init__(self, directory_url: str, company_name: str, search_input_selectors: Optional[List[str]] = None,
                 search_button_selectors: Optional[List[str]] = None, next_page_selector: Optional[str] = None,
                 max_pages: int = 10):
        self.directory_url = directory_url
        self.company_name = company_name

        # Use comprehensive selector arrays if not provided
        self.search_input_selectors = search_input_selectors or [
            'input[type="search"]',
            'input[name="q"]',
            'input[name="query"]',
            'input[name="search"]',
            'input[placeholder*="search" i]',
            'input[placeholder*="find" i]',
            'input[placeholder*="company" i]',
            '.search-input',
            '#search-input',
            '.search-box input',
            '.search-form input',
            'input.search-field',
            'input.query',
            'input.search-query'
        ]

        self.search_button_selectors = search_button_selectors or [
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
            '.search-submit',
            'form button[type="submit"]'
        ]

        self.next_page_selector = next_page_selector
        self.max_pages = max_pages

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
            await self._close_browser()  # Close existing if any

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
            await self.page.evaluate("window.scrollBy(0, 200)")  # Initial scroll
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
            elapsed_seconds=0,  # This will be calculated in the main run function
            encoding="utf-8",
            content_type="text/html",
            html=html,
            text=text_only,
            metadata={},
        )

    async def scrape_directory(self) -> List[Dict[str, Any]]:
        """Main function to search for company in directory, paginate through results, extract HTML, and process with LLM."""
        all_extracted_data = []
        current_page_num = 1

        await self._initialize_browser()

        try:
            # Navigate to the directory URL
            page_content = await self._navigate_to_url(self.directory_url)
            logger.info(f"Loaded directory page: {page_content.url}")

            # Check for redirection to login page
            if "login" in self.page.url.lower():
                logger.error(f"Redirected to login page: {self.page.url}. Please ensure the directory_url is accessible or handle login separately.")
                return []

            # Perform search for the company name
            try:
                search_input_element = self.page.locator(self.search_input_selector)
                if not await search_input_element.is_visible():
                    logger.error(f"Search input field with selector '{self.search_input_selector}' not visible on {self.page.url}. Check selector.")
                    return []
                await search_input_element.fill(self.company_name)

                # Simulate human-like click on the search button
                search_button_element = self.page.locator(self.search_button_selector)
                if await search_button_element.is_visible():
                    bbox = await search_button_element.bounding_box()
                    if bbox:
                        x, y = bbox['x'] + bbox['width'] / 2, bbox['y'] + bbox['height'] / 2
                        await execute_human_behavior(self.page, self.anti_detection_manager, behavior_type='click', position=(x, y))
                    else:
                        await search_button_element.click()  # Fallback click
                else:
                    logger.warning(f"Search button with selector '{self.search_button_selector}' not visible, attempting direct click.")
                    await self.page.click(self.search_button_selector)

                await self.page.wait_for_load_state("networkidle")
                logger.info(f"Searched for company: {self.company_name}")
            except Exception as e:
                logger.error(f"Failed to perform search on {self.page.url}: {e}")
                return []

            # Scrape up to max_pages of results
            while current_page_num <= self.max_pages:
                logger.info(f"Scraping results page {current_page_num} for '{self.company_name}'")

                # Extract the full HTML of the current results page
                current_page_html = await self.page.content()

                # Prepare data for LLM processing
                ai_input_data = {
                    "sections": [{"section": {"text": current_page_html, "tag": "body"}, "priority_score": 1.0}],
                    "structured_data": []  # Can be enhanced if needed
                }

                # Process with LLM (Gemini) to extract relevant data
                extracted_data = extract_client_info_from_sections(ai_input_data, self.page.url)
                if extracted_data and extracted_data.get("contacts"):
                    all_extracted_data.extend(extracted_data["contacts"])
                    logger.info(f"Extracted {len(extracted_data['contacts'])} contacts from page {current_page_num}")

                # Handle pagination to next page
                if self.next_page_selector and current_page_num < self.max_pages:
                    try:
                        next_button = self.page.locator(self.next_page_selector)
                        if await next_button.is_visible():
                            bbox = await next_button.bounding_box()
                            if bbox:
                                x, y = bbox['x'] + bbox['width'] / 2, bbox['y'] + bbox['height'] / 2
                                await execute_human_behavior(self.page, self.anti_detection_manager, behavior_type='click', position=(x, y))
                            else:
                                await next_button.click()  # Fallback click
                            await self.page.wait_for_load_state("networkidle")
                            current_page_num += 1
                        else:
                            logger.info("No next page button found, ending pagination.")
                            break
                    except Exception as e:
                        logger.warning(f"Error navigating to next page: {e}. Ending pagination.")
                        break
                else:
                    logger.info(f"Reached maximum pages ({self.max_pages}) or no next page selector provided.")
                    break

        finally:
            await self._close_browser()

        return all_extracted_data

    async def search_and_extract(self, company_name: Optional[str] = None, max_pages: Optional[int] = None) -> Dict[str, Any]:
        """Search for companies and extract data using LLM processing with comprehensive selector fallback."""
        if company_name:
            self.company_name = company_name
        if max_pages:
            self.max_pages = max_pages

        results = {
            "search_query": self.company_name,
            "directory_url": self.directory_url,
            "pages_processed": 0,
            "total_companies_found": 0,
            "extracted_data": [],
            "errors": []
        }

        await self._initialize_browser()

        try:
            # Navigate to directory
            logger.info(f"Navigating to directory: {self.directory_url}")
            page_content = await self._navigate_to_url(self.directory_url)

            # Check for login redirection
            if "login" in self.page.url.lower():
                error_msg = f"Redirected to login page: {self.page.url}"
                logger.error(error_msg)
                results["errors"].append(error_msg)
                return results

            # Find and interact with search input
            search_input = await self._find_search_input()
            if not search_input:
                error_msg = "Could not find search input field"
                logger.error(error_msg)
                results["errors"].append(error_msg)
                return results

            # Fill search input with human-like typing
            logger.info(f"Filling search input with: {self.company_name}")
            await search_input.click()
            await search_input.fill("")
            await search_input.type(self.company_name, delay=random.randint(100, 200))

            # Find and click search button
            search_button = await self._find_search_button()
            if search_button:
                logger.info("Clicking search button")
                await self._click_element_human_like(search_button)
            else:
                # Try submitting form directly
                logger.warning("No search button found, trying to submit form")
                await search_input.press("Enter")

            # Wait for search results
            await self.page.wait_for_load_state("networkidle", timeout=10000)
            await asyncio.sleep(random.uniform(2, 4))  # Additional wait for dynamic content

            # Process search results pages
            current_page = 1
            while current_page <= self.max_pages:
                logger.info(f"Processing results page {current_page}")

                # Get page HTML
                page_html = await self.page.content()

                # Process with LLM
                ai_input_data = {
                    "sections": [{"section": {"text": page_html, "tag": "body"}, "priority_score": 1.0}],
                    "structured_data": []
                }

                try:
                    extracted_data = extract_client_info_from_sections(ai_input_data, self.page.url)
                    if extracted_data and extracted_data.get("contacts"):
                        results["extracted_data"].extend(extracted_data["contacts"])
                        results["total_companies_found"] += len(extracted_data["contacts"])
                        logger.info(f"Extracted {len(extracted_data['contacts'])} companies from page {current_page}")
                except Exception as e:
                    error_msg = f"LLM extraction failed on page {current_page}: {e}"
                    logger.error(error_msg)
                    results["errors"].append(error_msg)

                results["pages_processed"] = current_page

                # Check for next page
                if current_page < self.max_pages and self.next_page_selector:
                    try:
                        next_button = self.page.locator(self.next_page_selector)
                        if await next_button.is_visible(timeout=5000):
                            logger.info("Navigating to next page")
                            await self._click_element_human_like(next_button)
                            await self.page.wait_for_load_state("networkidle", timeout=10000)
                            await asyncio.sleep(random.uniform(1, 3))
                            current_page += 1
                        else:
                            logger.info("No more pages available")
                            break
                    except Exception as e:
                        logger.warning(f"Could not navigate to next page: {e}")
                        break
                else:
                    break

        except Exception as e:
            error_msg = f"Search and extract failed: {e}"
            logger.error(error_msg)
            results["errors"].append(error_msg)
        finally:
            await self._close_browser()

        logger.info(f"Search completed: {results['total_companies_found']} companies found across {results['pages_processed']} pages")
        return results

    async def _find_search_input(self) -> Optional[Any]:
        """Find search input using multiple selectors."""
        for selector in self.search_input_selectors:
            try:
                element = await self.page.query_selector(selector)
                if element and await element.is_visible():
                    logger.info(f"Found search input with selector: {selector}")
                    return element
            except Exception:
                continue
        return None

    async def _find_search_button(self) -> Optional[Any]:
        """Find search button using multiple selectors."""
        for selector in self.search_button_selectors:
            try:
                element = await self.page.query_selector(selector)
                if element and await element.is_visible():
                    logger.info(f"Found search button with selector: {selector}")
                    return element
            except Exception:
                continue
        return None

    async def _click_element_human_like(self, element) -> None:
        """Click element with human-like behavior."""
        try:
            bbox = await element.bounding_box()
            if bbox:
                x = bbox['x'] + bbox['width'] / 2 + random.randint(-10, 10)
                y = bbox['y'] + bbox['height'] / 2 + random.randint(-5, 5)
                await self.page.mouse.move(x, y)
                await asyncio.sleep(random.uniform(0.1, 0.3))
                await self.page.mouse.click(x, y)
            else:
                await element.click()
        except Exception as e:
            logger.warning(f"Human-like click failed, using direct click: {e}")
            await element.click()


# Example usage
async def main():
    # Example for a company directory like Kompass
    scraper = CompanyDirectoryScraper(
        directory_url="https://www.kompass.com/en/search/",
        company_name="Microsoft Corporation"
    )

    # Use the new search_and_extract method
    results = await scraper.search_and_extract()

    print(f"Search completed: {results['total_companies_found']} companies found")
    print(f"Pages processed: {results['pages_processed']}")
    print(f"Extracted data: {len(results['extracted_data'])} items")

    if results['errors']:
        print(f"Errors encountered: {len(results['errors'])}")
        for error in results['errors']:
            print(f"  - {error}")

    # Print sample extracted data
    for i, company in enumerate(results['extracted_data'][:3]):  # Show first 3
        print(f"Company {i+1}: {company.get('company_name', 'Unknown')}")

    return results


if __name__ == "__main__":
    asyncio.run(main())


from web_scraper.data_models.models import PageContent
from web_scraper.utils.anti_detection import AntiDetectionManager, create_stealth_browser_context, execute_human_behavior
from web_scraper.ai_integration.ai import extract_client_info_from_sections  # Import the AI integration


class CompanyDirectoryScraper:
    def __init__(self, directory_url: str, company_name: str, search_input_selectors: Optional[List[str]] = None,
                 search_button_selectors: Optional[List[str]] = None, next_page_selector: Optional[str] = None,
                 max_pages: int = 10):
        self.directory_url = directory_url
        self.company_name = company_name

        # Use comprehensive selector arrays if not provided
        self.search_input_selectors = search_input_selectors or [
            'input[type="search"]',
            'input[name="q"]',
            'input[name="query"]',
            'input[name="search"]',
            'input[placeholder*="search" i]',
            'input[placeholder*="find" i]',
            'input[placeholder*="company" i]',
            '.search-input',
            '#search-input',
            '.search-box input',
            '.search-form input',
            'input.search-field',
            'input.query',
            'input.search-query'
        ]

        self.search_button_selectors = search_button_selectors or [
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
            '.search-submit',
            'form button[type="submit"]'
        ]

        self.next_page_selector = next_page_selector
        self.max_pages = max_pages

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
            await self._close_browser()  # Close existing if any

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
            await self.page.evaluate("window.scrollBy(0, 200)")  # Initial scroll
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
            elapsed_seconds=0,  # This will be calculated in the main run function
            encoding="utf-8",
            content_type="text/html",
            html=html,
            text=text_only,
            metadata={},
        )

    async def scrape_directory(self) -> List[Dict[str, Any]]:
        """Main function to search for company in directory, paginate through results, extract HTML, and process with LLM."""
        all_extracted_data = []
        current_page_num = 1

        await self._initialize_browser()

        try:
            # Navigate to the directory URL
            page_content = await self._navigate_to_url(self.directory_url)
            logger.info(f"Loaded directory page: {page_content.url}")

            # Check for redirection to login page
            if "login" in self.page.url.lower():
                logger.error(f"Redirected to login page: {self.page.url}. Please ensure the directory_url is accessible or handle login separately.")
                return []

            # Perform search for the company name
            try:
                search_input_element = self.page.locator(self.search_input_selector)
                if not await search_input_element.is_visible():
                    logger.error(f"Search input field with selector '{self.search_input_selector}' not visible on {self.page.url}. Check selector.")
                    return []
                await search_input_element.fill(self.company_name)

                # Simulate human-like click on the search button
                search_button_element = self.page.locator(self.search_button_selector)
                if await search_button_element.is_visible():
                    bbox = await search_button_element.bounding_box()
                    if bbox:
                        x, y = bbox['x'] + bbox['width'] / 2, bbox['y'] + bbox['height'] / 2
                        await execute_human_behavior(self.page, self.anti_detection_manager, behavior_type='click', position=(x, y))
                    else:
                        await search_button_element.click()  # Fallback click
                else:
                    logger.warning(f"Search button with selector '{self.search_button_selector}' not visible, attempting direct click.")
                    await self.page.click(self.search_button_selector)

                await self.page.wait_for_load_state("networkidle")
                logger.info(f"Searched for company: {self.company_name}")
            except Exception as e:
                logger.error(f"Failed to perform search on {self.page.url}: {e}")
                return []

            # Scrape up to max_pages of results
            while current_page_num <= self.max_pages:
                logger.info(f"Scraping results page {current_page_num} for '{self.company_name}'")

                # Extract the full HTML of the current results page
                current_page_html = await self.page.content()

                # Prepare data for LLM processing
                ai_input_data = {
                    "sections": [{"section": {"text": current_page_html, "tag": "body"}, "priority_score": 1.0}],
                    "structured_data": []  # Can be enhanced if needed
                }

                # Process with LLM (Gemini) to extract relevant data
                extracted_data = extract_client_info_from_sections(ai_input_data, self.page.url)
                if extracted_data and extracted_data.get("contacts"):
                    all_extracted_data.extend(extracted_data["contacts"])
                    logger.info(f"Extracted {len(extracted_data['contacts'])} contacts from page {current_page_num}")

                # Handle pagination to next page
                if self.next_page_selector and current_page_num < self.max_pages:
                    try:
                        next_button = self.page.locator(self.next_page_selector)
                        if await next_button.is_visible():
                            bbox = await next_button.bounding_box()
                            if bbox:
                                x, y = bbox['x'] + bbox['width'] / 2, bbox['y'] + bbox['height'] / 2
                                await execute_human_behavior(self.page, self.anti_detection_manager, behavior_type='click', position=(x, y))
                            else:
                                await next_button.click()  # Fallback click
                            await self.page.wait_for_load_state("networkidle")
                            current_page_num += 1
                        else:
                            logger.info("No next page button found, ending pagination.")
                            break
                    except Exception as e:
                        logger.warning(f"Error navigating to next page: {e}. Ending pagination.")
                        break
                else:
                    logger.info(f"Reached maximum pages ({self.max_pages}) or no next page selector provided.")
                    break

        finally:
            await self._close_browser()

        return all_extracted_data

    async def search_and_extract(self, company_name: Optional[str] = None, max_pages: Optional[int] = None) -> Dict[str, Any]:
        """Search for companies and extract data using LLM processing with comprehensive selector fallback."""
        if company_name:
            self.company_name = company_name
        if max_pages:
            self.max_pages = max_pages

        results = {
            "search_query": self.company_name,
            "directory_url": self.directory_url,
            "pages_processed": 0,
            "total_companies_found": 0,
            "extracted_data": [],
            "errors": []
        }

        await self._initialize_browser()

        try:
            # Navigate to directory
            logger.info(f"Navigating to directory: {self.directory_url}")
            page_content = await self._navigate_to_url(self.directory_url)

            # Check for login redirection
            if "login" in self.page.url.lower():
                error_msg = f"Redirected to login page: {self.page.url}"
                logger.error(error_msg)
                results["errors"].append(error_msg)
                return results

            # Find and interact with search input
            search_input = await self._find_search_input()
            if not search_input:
                error_msg = "Could not find search input field"
                logger.error(error_msg)
                results["errors"].append(error_msg)
                return results

            # Fill search input with human-like typing
            logger.info(f"Filling search input with: {self.company_name}")
            await search_input.click()
            await search_input.fill("")
            await search_input.type(self.company_name, delay=random.randint(100, 200))

            # Find and click search button
            search_button = await self._find_search_button()
            if search_button:
                logger.info("Clicking search button")
                await self._click_element_human_like(search_button)
            else:
                # Try submitting form directly
                logger.warning("No search button found, trying to submit form")
                await search_input.press("Enter")

            # Wait for search results
            await self.page.wait_for_load_state("networkidle", timeout=10000)
            await asyncio.sleep(random.uniform(2, 4))  # Additional wait for dynamic content

            # Process search results pages
            current_page = 1
            while current_page <= self.max_pages:
                logger.info(f"Processing results page {current_page}")

                # Get page HTML
                page_html = await self.page.content()

                # Process with LLM
                ai_input_data = {
                    "sections": [{"section": {"text": page_html, "tag": "body"}, "priority_score": 1.0}],
                    "structured_data": []
                }

                try:
                    extracted_data = extract_client_info_from_sections(ai_input_data, self.page.url)
                    if extracted_data and extracted_data.get("contacts"):
                        results["extracted_data"].extend(extracted_data["contacts"])
                        results["total_companies_found"] += len(extracted_data["contacts"])
                        logger.info(f"Extracted {len(extracted_data['contacts'])} companies from page {current_page}")
                except Exception as e:
                    error_msg = f"LLM extraction failed on page {current_page}: {e}"
                    logger.error(error_msg)
                    results["errors"].append(error_msg)

                results["pages_processed"] = current_page

                # Check for next page
                if current_page < self.max_pages and self.next_page_selector:
                    try:
                        next_button = self.page.locator(self.next_page_selector)
                        if await next_button.is_visible(timeout=5000):
                            logger.info("Navigating to next page")
                            await self._click_element_human_like(next_button)
                            await self.page.wait_for_load_state("networkidle", timeout=10000)
                            await asyncio.sleep(random.uniform(1, 3))
                            current_page += 1
                        else:
                            logger.info("No more pages available")
                            break
                    except Exception as e:
                        logger.warning(f"Could not navigate to next page: {e}")
                        break
                else:
                    break

        except Exception as e:
            error_msg = f"Search and extract failed: {e}"
            logger.error(error_msg)
            results["errors"].append(error_msg)
        finally:
            await self._close_browser()

        logger.info(f"Search completed: {results['total_companies_found']} companies found across {results['pages_processed']} pages")
        return results

    async def _find_search_input(self) -> Optional[Any]:
        """Find search input using multiple selectors."""
        for selector in self.search_input_selectors:
            try:
                element = await self.page.query_selector(selector)
                if element and await element.is_visible():
                    logger.info(f"Found search input with selector: {selector}")
                    return element
            except Exception:
                continue
        return None

    async def _find_search_button(self) -> Optional[Any]:
        """Find search button using multiple selectors."""
        for selector in self.search_button_selectors:
            try:
                element = await self.page.query_selector(selector)
                if element and await element.is_visible():
                    logger.info(f"Found search button with selector: {selector}")
                    return element
            except Exception:
                continue
        return None

    async def _click_element_human_like(self, element) -> None:
        """Click element with human-like behavior."""
        try:
            bbox = await element.bounding_box()
            if bbox:
                x = bbox['x'] + bbox['width'] / 2 + random.randint(-10, 10)
                y = bbox['y'] + bbox['height'] / 2 + random.randint(-5, 5)
                await self.page.mouse.move(x, y)
                await asyncio.sleep(random.uniform(0.1, 0.3))
                await self.page.mouse.click(x, y)
            else:
                await element.click()
        except Exception as e:
            logger.warning(f"Human-like click failed, using direct click: {e}")
            await element.click()
async def main():
    # Example for a company directory like Kompass
    scraper = CompanyDirectoryScraper(
        directory_url="https://www.example-directory.com/search",  # Replace with actual directory URL
        company_name="Example Company Inc.",  # Replace with company to search
        search_input_selector="input[name='query']",  # Replace with actual selector
        search_button_selector="button[type='submit']",  # Replace with actual selector
        next_page_selector="a.next",  # Replace with actual selector
        max_pages=10
    )
    results = await scraper.scrape_directory()
    logger.info(f"Total extracted data: {len(results)}")
    for item in results:
        logger.info(item)


if __name__ == "__main__":
    asyncio.run(main())