from __future__ import annotations

import re
import time
from typing import Dict, List, Tuple, Optional

import requests
from bs4 import BeautifulSoup
from loguru import logger

from web_scraper.data_models.models import ClassificationResult
from web_scraper.utils.classification_cache import ClassificationCache


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

_cache = ClassificationCache()


def _text_to_html_ratio(html: str) -> float:
	soup = BeautifulSoup(html, "lxml")
	text = soup.get_text(separator=" ", strip=True)
	return len(text) / max(1, len(html))


def _analyze_html_structure(html: str) -> Tuple[Dict[str, float], List[str]]:
	indicators: Dict[str, float] = {}
	reasons: List[str] = []

	soup = BeautifulSoup(html, "lxml")
	text = soup.get_text(separator=" ", strip=True)
	word_count = len(re.findall(r"\w+", text))
	indicators["word_count"] = float(word_count)

	all_nodes = soup.find_all(True)
	script_nodes = soup.find_all("script")
	num_nodes = max(1, len(all_nodes))
	script_density = len(script_nodes) / num_nodes
	indicators["script_density"] = script_density

	spa_signatures = 0
	if soup.find(id="root") or soup.find(id="app"):
		spa_signatures += 1
	if soup.find(attrs={"data-reactroot": True}):
		spa_signatures += 1
	if soup.find(attrs={"ng-version": True}):
		spa_signatures += 1
	if soup.find("script", string=re.compile(r"React|Vue|Angular", re.I)):
		spa_signatures += 1
	indicators["spa_signatures"] = float(spa_signatures)

	# Additional Phase 2 signals
	ratio = _text_to_html_ratio(html)
	indicators["text_to_html_ratio"] = ratio
	if ratio > 0.2:
		reasons.append("Good text-to-HTML ratio (>0.2)")

	placeholders = len(soup.select(".skeleton, .placeholder, .loading, [aria-busy='true']"))
	indicators["placeholder_count"] = float(placeholders)
	if placeholders > 0:
		reasons.append("Loading skeletons/placeholders detected")

	if soup.find("noscript"):
		indicators["noscript_present"] = 1.0
		reasons.append("<noscript> present")

	if word_count > 500:
		reasons.append("Rich initial content (>500 words)")
	if script_density < 0.2:
		reasons.append("Low script density (<20%)")
	if spa_signatures > 0:
		reasons.append("SPA framework signatures present")
	if word_count < 100:
		reasons.append("Minimal initial content (<100 words)")

	return indicators, reasons


def classify_url(url: str, timeout: int = 20, override_classification: Optional[str] = None, override_confidence: Optional[float] = None) -> ClassificationResult:
	# Manual override
	if override_classification is not None and override_confidence is not None:
		_cache.override(url, override_classification, override_confidence)
		logger.info(f"Manual override applied for {url}: {override_classification} ({override_confidence:.2f})")

	# Try cache first
	cached = _cache.get(url)
	if cached:
		logger.info(f"Cache hit for {url}")
		rec = cached["result"]
		return ClassificationResult(**rec)

	# Try similar URLs
	similar = _cache.get_similar(url)
	if similar:
		logger.info(f"Similar URL cache hint used for {url}")
		rec = similar["result"]
		return ClassificationResult(**rec)

	logger.info(f"Classifying URL: {url}")

	status_code = None
	indicators: Dict[str, float] = {}
	reasons: List[str] = []

	headers = dict(_DEFAULT_HEADERS)

	# HEAD analysis
	try:
		start = time.time()
		h = requests.head(url, headers=headers, allow_redirects=True, timeout=timeout)
		elapsed = time.time() - start
		status_code = h.status_code
		content_type = h.headers.get("Content-Type", "").lower()
		server = h.headers.get("Server")
		x_powered = h.headers.get("X-Powered-By")

		indicators["head_elapsed_s"] = elapsed
		if content_type:
			if "application/json" in content_type:
				indicators["content_type_json"] = 1.0
				reasons.append("HEAD indicates JSON endpoint")
			elif "text/html" in content_type:
				indicators["content_type_html"] = 1.0

		if server:
			indicators["server_header_present"] = 1.0
		if x_powered:
			indicators["x_powered_by_present"] = 1.0
	except Exception as e:
		logger.debug(f"HEAD failed: {e}")

	# GET initial HTML
	html = ""
	try:
		start = time.time()
		r = requests.get(url, headers=headers, allow_redirects=True, timeout=timeout)
		elapsed_get = time.time() - start
		status_code = r.status_code
		indicators["get_elapsed_s"] = elapsed_get
		ct = (r.headers.get("Content-Type") or "").lower()
		if "text/html" in ct or r.text:
			html = r.text or ""
	except Exception as e:
		logger.warning(f"GET failed for {url}: {e}")

	if html:
		struct_ind, struct_reasons = _analyze_html_structure(html)
		indicators.update(struct_ind)
		reasons.extend(struct_reasons)

	# Decision logic
	confidence = 0.5
	classification = "dynamic"

	word_count = indicators.get("word_count", 0)
	script_density = indicators.get("script_density", 1.0)
	spa_signatures = indicators.get("spa_signatures", 0)
	ratio = indicators.get("text_to_html_ratio", 0.0)
	placeholders = indicators.get("placeholder_count", 0.0)
	noscript = indicators.get("noscript_present", 0.0)

	static_votes = 0
	dynamic_votes = 0

	if word_count > 500:
		static_votes += 1
	if script_density < 0.2:
		static_votes += 1
	if ratio > 0.2:
		static_votes += 1

	if word_count < 100:
		dynamic_votes += 1
	if script_density > 0.5:
		dynamic_votes += 1
	if spa_signatures > 0:
		dynamic_votes += 1
	if placeholders > 0:
		dynamic_votes += 1
	if noscript > 0:
		dynamic_votes += 1
	if indicators.get("content_type_json"):
		dynamic_votes += 1

	total_votes = max(1, static_votes + dynamic_votes)
	if static_votes > dynamic_votes:
		classification = "static"
		confidence = static_votes / total_votes
	else:
		classification = "dynamic"
		confidence = dynamic_votes / total_votes

	result = ClassificationResult(
		url=url,
		classification=classification,
		confidence=confidence,
		indicators=indicators,
		reasons=reasons,
		status_code=status_code,
	)

	# Save to cache
	_cache.set(url, result.model_dump(mode="json"))
	return result
