from __future__ import annotations

import asyncio
import time
from typing import Optional

from bs4 import BeautifulSoup
from loguru import logger

from web_scraper.data_models.models import PageContent
from web_scraper.utils.anti_detection import AntiDetectionManager, execute_human_behavior, create_stealth_browser_context


async def _ensure_playwright():
	try:
		from playwright.async_api import async_playwright  # type: ignore
		return async_playwright
	except Exception as e:
		raise RuntimeError(
			"Playwright is not installed. Please install it and run 'playwright install chromium'."
		) from e


async def _create_context(pw):
	try:
		# Optional anti-detection integration
		adm = AntiDetectionManager(
			enable_fingerprint_evasion=True,
			enable_behavioral_mimicking=True,
			enable_network_obfuscation=True,
		)
		browser, context = await create_stealth_browser_context(pw, adm, is_mobile=False)
		return browser, context, adm
	except Exception as e:
		logger.warning(f"Anti-detection context failed, falling back: {e}")
		browser = await pw.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])  # type: ignore
		context = await browser.new_context()
		return browser, context, None


async def _dismiss_popups(page, adm: Optional[AntiDetectionManager] = None):
	"""Attempt to close common consent/sign-in/newsletter popups with human-like actions."""
	# Common selectors/texts across consent managers and modals
	text_buttons = [
		"Accept all",
		"Accept All",
		"Accept",
		"I agree",
		"I Agree",
		"Allow all",
		"Allow All",
		"Got it",
		"Okay",
		"OK",
		"Close",
		"No thanks",
		"Not now",
		"Not Now",
		"Dismiss",
	]
	cookie_specific = [
		"Only allow essential cookies",
		"Allow all cookies",
	]
	selectors = [
		"#onetrust-accept-btn-handler",
		".onetrust-close-btn-handler",
		".ot-pc-refuse-all-handler",
		"#cky-consent-accept",
		".cky-consent-btn-accept",
		"button[aria-label='Close']",
		"button[aria-label='close']",
		"[data-testid='close']",
		".modal-close, .modal__close, .close, .close-button",
		".newsletter, .cookie, .cookies, .gdpr, .consent",
	]

	async def _click_if_visible(locator):
		try:
			if await locator.is_visible():
				# slight human-like pause
				if adm:
					await asyncio.sleep(0.2)
				await locator.click(timeout=1500)
				return True
		except Exception:
			return False
		return False

	# Try role-based buttons with text
	try:
		for label in text_buttons + cookie_specific:
			btn = page.get_by_role("button", name=label)
			if await _click_if_visible(btn):
				return
	except Exception:
		pass

	# Try text locators for links/divs that act as buttons
	try:
		for label in text_buttons + cookie_specific:
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


async def fetch_dynamic_async(url: str, wait_for_selector: Optional[str] = None, timeout_ms: int = 30000) -> PageContent:
	async_playwright = await _ensure_playwright()
	start = time.time()
	async with async_playwright() as pw:
		browser, context, adm = await _create_context(pw)
		page = await context.new_page()
		logger.info(f"Fetching URL (dynamic): {url}")
		# Network obfuscation: delay before navigating and increment counters
		if adm is not None:
			try:
				delay = await adm.calculate_request_delay()
				if delay > 0:
					await asyncio.sleep(delay)
				adm.request_count += 1
				adm.last_request_time = time.time()
				# Optional: rotate fingerprint between navigations (recreate context if needed)
				if await adm.should_rotate_fingerprint():
					try:
						await context.close()
						await browser.close()
						browser, context, adm = await _create_context(pw)
						page = await context.new_page()
					except Exception:
						pass
			except Exception:
				pass
		try:
			resp = await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
		except Exception as e:
			# Check if it's a network-related error
			error_str = str(e).lower()
			if any(network_error in error_str for network_error in [
				'net::err_http2_protocol_error', 'net::err_name_not_resolved', 
				'timeout', 'connection', 'network', 'dns', 'refused'
			]):
				logger.warning(f"Network error accessing {url}: {e}")
				raise RuntimeError(f"Network error: {e}") from e
			else:
				raise
		# Progressive waits per plan
		try:
			await page.wait_for_load_state("networkidle", timeout=timeout_ms // 2)
		except Exception:
			pass
		if wait_for_selector:
			try:
				await page.wait_for_selector(wait_for_selector, timeout=timeout_ms // 2)
			except Exception:
				pass
		# Try dismissing popups early
		try:
			await _dismiss_popups(page, adm)
		except Exception:
			pass
		# Basic scroll to trigger lazy content
		try:
			h = await page.evaluate("() => document.body.scrollHeight")
			manager_for_actions = adm if adm is not None else AntiDetectionManager()
			await execute_human_behavior(page, manager_for_actions, behavior_type='scroll', position=int(h))
			# Light mouse move near the center to simulate activity
			viewport = await page.viewport_size()
			if viewport:
				await execute_human_behavior(page, manager_for_actions, behavior_type='mousemove', position=(int(viewport['width'] * 0.6), int(viewport['height'] * 0.6)))
		except Exception:
			pass
		# Try dismissing popups again after interactions
		try:
			await _dismiss_popups(page, adm)
		except Exception:
			pass

		html = await page.content()
		status = (resp.status if resp else 200)
		elapsed = time.time() - start

		await context.close()
		await browser.close()

		# Normalize to text-only per Phase 3.1 pipeline basics
		soup = BeautifulSoup(html, "lxml")
		for tag in soup.find_all("script"):
			if tag.get("type") not in ("application/ld+json", "application/json"):
				tag.decompose()

		# Still remove all <style> tags
		for tag in soup.find_all("style"):
			tag.decompose()
		
		text_only = soup.get_text(separator="\n", strip=True)

		return PageContent(
			url=url,
			status_code=status,
			elapsed_seconds=elapsed,
			encoding="utf-8",
			content_type="text/html",
			html=html,
			text=text_only,
			metadata={},
		)


def fetch_dynamic(url: str, wait_for_selector: Optional[str] = None, timeout_ms: int = 30000) -> PageContent:
	try:
		try:
			loop = asyncio.get_event_loop()
		except RuntimeError:
			loop = asyncio.new_event_loop()
			asyncio.set_event_loop(loop)
		return loop.run_until_complete(
			fetch_dynamic_async(url, wait_for_selector=wait_for_selector, timeout_ms=timeout_ms)
		)
	except RuntimeError as e:
		logger.error(str(e))
		raise


def smoke_test_scraper_dynamic(url: str = "https://www.instagram.com/p/DCI2BSPSz0A/?hl=en", wait_for_selector: Optional[str] = None) -> dict:
	"""Small smoke test for dynamic scraper.

	Returns a dict with success flag and key fields. If Playwright is missing
	or navigation fails, returns an error description instead of raising.
	"""
	try:
		page = fetch_dynamic(url, wait_for_selector=wait_for_selector, timeout_ms=15000)
		return {
			"ok": True,
			"url": str(page.url),
			"status_code": page.status_code,
			"elapsed_seconds": page.elapsed_seconds,
			"encoding": page.encoding,
			"content_type": page.content_type,
			"text_preview": page.text[:200],
		}
	except Exception as e:
		return {"ok": False, "url": url, "error": str(e)}
