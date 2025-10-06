import asyncio
from typing import Dict, Optional, Tuple

from loguru import logger
from playwright.async_api import Browser, BrowserContext, async_playwright

from web_scraper.utils.anti_detection import AntiDetectionManager, create_stealth_browser_context


class BrowserManager:
    """
    Manages Playwright browser instances and contexts for efficient scraping.
    Ensures a single browser instance is used for multiple contexts.
    """

    def __init__(self, proxy: Optional[Dict[str, str]] = None):
        self.proxy = proxy
        self.browser: Optional[Browser] = None
        self.adm: Optional[AntiDetectionManager] = None
        self._lock = asyncio.Lock()  # To prevent multiple concurrent browser launches

    async def _ensure_browser(self):
        """Ensures a single browser instance is launched and ready."""
        async with self._lock:
            if self.browser is None or not self.browser.is_connected():
                logger.info("Launching new Playwright browser instance...")
                pw = await async_playwright().start()
                try:
                    self.adm = AntiDetectionManager(
                        enable_fingerprint_evasion=True,
                        enable_behavioral_mimicking=True,
                        enable_network_obfuscation=True,
                    )
                    # Use create_stealth_browser_context to get a stealthy browser
                    # We only need the browser object here, not the context
                    temp_browser, _ = await create_stealth_browser_context(
                        pw, self.adm, is_mobile=False
                    )
                    self.browser = temp_browser
                    logger.info("Playwright browser launched with anti-detection features.")
                except Exception as e:
                    logger.warning(f"Anti-detection browser launch failed, falling back to regular browser: {e}")
                    self.browser = await pw.chromium.launch(
                        headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"], proxy=self.proxy
                    )
                    self.adm = None  # Disable ADM if it failed
                
                # Keep playwright instance alive for the duration of the browser
                self._pw_instance = pw 

    async def get_new_context(self) -> Tuple[BrowserContext, Optional[AntiDetectionManager]]:
        """
        Retrieves a new browser context from the managed browser instance.
        Ensures the browser is launched before creating a context.
        """
        await self._ensure_browser()
        if self.browser is None:
            raise RuntimeError("Browser is not initialized.")

        logger.info("Creating new browser context.")
        context = await self.browser.new_context()
        if self.adm:
            # Apply anti-detection to the new context if ADM is enabled
            # Note: create_stealth_browser_context already creates a context with ADM,
            # but for subsequent contexts from the same browser, we might need to apply
            # specific routes or settings if they are not browser-wide.
            # For now, assuming browser-wide ADM is sufficient or handled by new_context defaults.
            pass
        return context, self.adm

    async def close_browser(self):
        """Closes the browser instance and any associated Playwright resources."""
        async with self._lock:
            if self.browser:
                logger.info("Closing Playwright browser instance.")
                await self.browser.close()
                self.browser = None
                if self._pw_instance:
                    await self._pw_instance.stop()
                    self._pw_instance = None

    async def __aenter__(self):
        await self._ensure_browser()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close_browser()
