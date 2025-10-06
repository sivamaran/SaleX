from __future__ import annotations

import asyncio
import time
from typing import Optional

import chardet
import requests
from bs4 import BeautifulSoup
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from web_scraper.data_models.models import PageContent
from web_scraper.utils.anti_detection import AntiDetectionManager


_DEFAULT_HEADERS = {
	"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
	"Accept-Language": "en-US,en;q=0.9",
	"Cache-Control": "no-cache",
	"Pragma": "no-cache",
	"Upgrade-Insecure-Requests": "1",
	"User-Agent": (
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
		"Chrome/118.0.0.0 Safari/537.36"
	),
}


class StaticScraper:
	def __init__(self, timeout: int = 30, headers: Optional[dict] = None, anti_detection: Optional[AntiDetectionManager] = None):
		self.timeout = timeout
		self.headers = headers or dict(_DEFAULT_HEADERS)
		self.anti_detection = anti_detection or AntiDetectionManager(
			enable_fingerprint_evasion=True,
			enable_behavioral_mimicking=True,
			enable_network_obfuscation=True,
		)
		# Reuse a single session for connection pooling and cookie jar reuse
		self._session = requests.Session()

	def _build_headers(self) -> dict:
		# Merge baseline headers with stealth headers; avoid brotli/zstd which requests may not decode
		stealth = self.anti_detection._generate_stealth_headers(is_mobile=False)
		if "Accept-Encoding" in stealth:
			stealth["Accept-Encoding"] = "gzip, deflate"
		# Prefer stealth UA and sec-ch-* while retaining any explicit overrides passed by user
		merged = dict(self.headers)
		merged.update(stealth)
		return merged

	def _apply_network_delay_and_rotate_if_needed(self) -> None:
		# Apply human-like request spacing and rotate fingerprint periodically
		try:
			delay = asyncio.run(self.anti_detection.calculate_request_delay())
		except RuntimeError:
			# If we're inside an event loop (rare for static), fall back to no delay
			delay = 0.0
		if delay > 0:
			time.sleep(delay)
		self.anti_detection.request_count += 1
		self.anti_detection.last_request_time = time.time()
		try:
			should_rotate = asyncio.run(self.anti_detection.should_rotate_fingerprint())
		except RuntimeError:
			should_rotate = False
		if should_rotate:
			setattr(self.anti_detection, 'last_fingerprint_rotation', time.time())
			setattr(self.anti_detection, 'fingerprint_rotation_count', getattr(self.anti_detection, 'fingerprint_rotation_count', 0) + 1)

	@retry(
		stop=stop_after_attempt(3),
		wait=wait_exponential(multiplier=1, min=1, max=8),
		retry=retry_if_exception_type((requests.RequestException,)),
	)
	def fetch(self, url: str) -> PageContent:
		logger.info(f"Fetching URL (static): {url}")
		start = time.time()
		self._apply_network_delay_and_rotate_if_needed()
		headers = self._build_headers()
		self._session.headers.clear()
		self._session.headers.update(headers)
		resp = self._session.get(url, timeout=self.timeout)
		elapsed = time.time() - start
		# If blocked by common anti-bot statuses, attempt dynamic fallback inline
		if resp.status_code in {403, 429, 503}:
			from loguru import logger as _logger
			_logger.warning(f"Static fetch received status={resp.status_code}, retrying with rotated headers")
			# Try a second attempt with rotated fingerprint headers (desktop)
			try:
				self._apply_network_delay_and_rotate_if_needed()
				headers2 = self._build_headers()
				self._session.headers.clear()
				self._session.headers.update(headers2)
				resp2 = self._session.get(url, timeout=self.timeout)
				if resp2.status_code not in {403, 429, 503}:
					resp = resp2
			except Exception:
				pass
			# If still blocked, attempt a mobile fingerprint as a last static attempt
			if resp.status_code in {403, 429, 503}:
				try:
					self._apply_network_delay_and_rotate_if_needed()
					stealth_mobile = self.anti_detection._generate_stealth_headers(is_mobile=True)
					stealth_mobile["Accept-Encoding"] = "gzip, deflate"
					mobile_headers = dict(self.headers)
					mobile_headers.update(stealth_mobile)
					self._session.headers.clear()
					self._session.headers.update(mobile_headers)
					resp3 = self._session.get(url, timeout=self.timeout)
					if resp3.status_code not in {403, 429, 503}:
						resp = resp3
				except Exception:
					pass
			# As a final static attempt, warm up cookies via base domain, then retry
			if resp.status_code in {403, 429, 503}:
				try:
					from urllib.parse import urlparse
					parsed = urlparse(url)
					base = f"{parsed.scheme}://{parsed.netloc}/"
					self._apply_network_delay_and_rotate_if_needed()
					self._session.headers.clear()
					self._session.headers.update(self._build_headers())
					self._session.get(base, timeout=self.timeout)
					self._apply_network_delay_and_rotate_if_needed()
					resp4 = self._session.get(url, timeout=self.timeout)
					if resp4.status_code not in {403, 429, 503}:
						resp = resp4
				except Exception:
					pass
			# Proceed to raise for status if still blocked so tenacity can retry
		resp.raise_for_status()

		raw = resp.content
		encoding = None

		# Respect provided encoding first
		if resp.encoding:
			encoding = resp.encoding
			text = resp.text
		else:
			# Detect encoding
			detected = chardet.detect(raw)
			encoding = detected.get("encoding") or "utf-8"
			text = raw.decode(encoding, errors="replace")

		ct = (resp.headers.get("Content-Type") or "").lower()
		html = text

		# Normalize text (light cleaning for Phase 1)
		soup = BeautifulSoup(html, "lxml")
		# Attempt to remove common modal/overlay elements that hide content
		for modal_selector in [
			".cookie-banner", ".cookie-consent", ".cookies", ".gdpr",
			".consent", "#onetrust-consent-sdk", ".ot-sdk-container",
			".modal", ".modal-backdrop", ".newsletter", ".signup", ".overlay"
		]:
			for el in soup.select(modal_selector):
				try:
					el.decompose()
				except Exception:
					pass
		for tag in soup(["script", "style"]):
			tag.decompose()
		text_only = soup.get_text(separator="\n", strip=True)

		return PageContent(
			url=url,
			status_code=resp.status_code,
			elapsed_seconds=elapsed,
			encoding=encoding,
			content_type=ct,
			html=html,
			text=text_only,
			metadata={
				"server": resp.headers.get("Server", ""),
				"content_length": str(len(raw)),
			},
		)
