from __future__ import annotations

import json
import re
from typing import Dict, List, Tuple

from bs4 import BeautifulSoup, Comment
from loguru import logger

EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_REGEX = re.compile(r"(?:(?:\+\d{1,3}[\s.-]?)?(?:\(\d{2,4}\)[\s.-]?|\d{2,4}[\s.-])?\d{3}[\s.-]?\d{3,4}(?:[\s.-]?\d{3,4})?)")


def clean_html(html: str) -> Tuple[BeautifulSoup, str]:
	"""Remove scripts/styles/comments and normalize whitespace, return soup and text."""
	soup = BeautifulSoup(html, "lxml")
	for element in soup(["script", "style", "noscript"]):
		element.decompose()
	# Remove HTML comments (avoid deprecated 'text' kwarg)
	for comment in soup.find_all(string=lambda s: isinstance(s, Comment)):
		comment.extract()
	text = soup.get_text(separator="\n", strip=True)
	# Normalize excessive blank lines
	text = re.sub(r"\n{3,}", "\n\n", text)
	return soup, text


def parse_jsonld_scripts(soup: BeautifulSoup) -> List[Dict]:
	structured: List[Dict] = []
	for tag in soup.find_all("script", type=lambda t: t and "ld+json" in t.lower()):
		try:
			data = json.loads(tag.string or "{}")
			if isinstance(data, list):
				structured.extend([d for d in data if isinstance(d, dict)])
			elif isinstance(data, dict):
				structured.append(data)
		except Exception:
			continue
	return structured


def extract_links_and_images(soup: BeautifulSoup) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
	links: List[Dict[str, str]] = []
	for a in soup.find_all("a", href=True):
		text = (a.get_text(strip=True) or "")[:200]
		links.append({"href": a["href"], "text": text})
	images: List[Dict[str, str]] = []
	for img in soup.find_all("img"):
		images.append({
			"src": img.get("src", ""),
			"alt": img.get("alt", "")
		})
	return links, images


def section_content(soup: BeautifulSoup) -> List[Dict[str, str]]:
	"""Segment content into sections using specific tags and return as List[Dict]."""
	sections = []
	section_tags = ["h1", "h2", "h3", "h4", "h5", "h6", "p", "li", 'td', 'th', 'dd', 'dt', "a", "span", "footer"]
	
	for node in soup.find_all(section_tags):
		text = node.get_text(strip=True)
		if text and len(text) > 7:  # Filter out very short content
			section_data = {
				"tag": node.name,
				"text": text,
				"class": " ".join(node.get("class", [])),
				"id": node.get("id", ""),
				"parent_tag": node.parent.name if node.parent else ""
			}
			sections.append(section_data)
	
	return sections


def extract_contact_patterns(text: str, links: List[Dict[str, str]]) -> Tuple[List[str], List[str]]:
	emails = sorted(set(EMAIL_REGEX.findall(text)))
	phones = sorted(set([p.strip() for p in PHONE_REGEX.findall(text) if len(p.strip()) >= 7]))
	# Also check mailto links
	for link in links:
		href = link.get("href", "")
		if href.startswith("mailto:"):
			emails.append(href[len("mailto:"):])
	emails = sorted(set(emails))
	return emails, phones


def process_content(html: str) -> Dict:
	try:
		raw_soup = BeautifulSoup(html, "lxml")
		extract_jsonld = parse_jsonld_scripts(raw_soup)

		soup, cleaned_text = clean_html(html)
		links, images = extract_links_and_images(soup)
		sections = section_content(soup)
		emails, phones = extract_contact_patterns(cleaned_text, links)
		logger.debug(f"Content processed successfully inside process_content")
		
		return {
			"cleaned_text": cleaned_text,
			"structured_data": extract_jsonld,
			"links": links,
			"images": images,
			"sections": sections,
			"emails": emails,
			"phones": phones,
		}
	except Exception as e:
		logger.error(f"Failed to process content in process_content: {e}")
		return None
