"""
Universal Scraper - Fully Dynamic Lead Generation System
==========================================================

A completely dynamic web scraper that extracts complete business profiles from ANY business directory website.
This single file contains all necessary components for universal scraping - no hardcoded site configurations.

Features:
- AI-powered data extraction using Google Gemini
- Comprehensive lead profiles (beyond just contacts)
- Anti-detection browser automation
- Contact validation and filtering
- Works with ANY business directory website dynamically
- Modular architecture for easy extension

Usage:
python universal_scraper_complete.py [SERVICE]  # REQUIRED argument

Examples:
python universal_scraper_complete.py "ac services"
python universal_scraper_complete.py "software development"
python universal_scraper_complete.py "travel services"

Author: GitHub Copilot
Date: September 24, 2025
"""

import asyncio
import time
import random
import re
import json
import os
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any, Tuple
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from loguru import logger
from playwright.async_api import Page, BrowserContext, Browser, async_playwright
from pydantic import BaseModel, Field
import google.generativeai as genai

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not installed, will use system env vars

# =============================================================================
# CONFIGURATION
# =============================================================================

# AI Configuration
_AI_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
_AI_DISABLED = False
_AI_MAX_CALLS = 5
_AI_CALLS = 0
_AI_LAST_CALL_TIME = 0
_AI_RATE_LIMIT = 1.0  # seconds between calls
_HAS_GEMINI = False

# Scraper Configurations - REMOVED: Now fully dynamic, works with any website
# SCRAPER_CONFIGS = {
#     "kompass": {...},
#     "opencorporates": {...},
#     etc.
# }

# =============================================================================
# DATA MODELS
# =============================================================================

class ContactInfo(BaseModel):
    """Contact information structure"""
    email: Optional[str] = None
    phone: Optional[str] = None
    linkedin: Optional[str] = None
    twitter: Optional[str] = None
    website: Optional[str] = None
    others: Optional[str] = None
    socialmedialinks: List[str] = Field(default_factory=list)

class Lead(BaseModel):
    """Complete lead profile structure"""
    name: Optional[str] = None
    contact_info: ContactInfo = Field(default_factory=ContactInfo)
    company_name: Optional[str] = None
    time: Optional[str] = None
    link_details: Optional[str] = None
    type: str = "lead"  # "lead" or "competitor"
    what_we_can_offer: Optional[str] = None
    source_url: Optional[str] = None
    source_platform: Optional[str] = None
    location: Optional[str] = None
    industry: Optional[str] = None
    company_type: Optional[str] = None
    bio: Optional[str] = None
    address: Optional[str] = None

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def extract_emails(lead_data: Dict[str, Any]) -> List[str]:
    """Extract email addresses from lead data."""
    emails = []

    # Check direct email field
    if lead_data.get('email'):
        if isinstance(lead_data['email'], list):
            emails.extend(lead_data['email'])
        else:
            emails.append(lead_data['email'])

    # Extract from notes or other text fields
    text_fields = ['notes', 'bio', 'description', 'address', 'organization']
    for field in text_fields:
        if lead_data.get(field):
            # Comprehensive email regex patterns
            email_patterns = [
                r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
                r'mailto:\s*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})',
                r'email["\']?\s*:\s*["\']?([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})',
                r'contact["\']?\s*:\s*["\']?([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})',
                r'info@\S+\.\S+',
                r'sales@\S+\.\S+',
                r'support@\S+\.\S+',
                r'admin@\S+\.\S+',
                r'contact@\S+\.\S+',
            ]
            for pattern in email_patterns:
                found_emails = re.findall(pattern, lead_data[field], re.IGNORECASE)
                emails.extend(found_emails)

    # Clean and validate emails
    valid_emails = []
    for email in emails:
        email = email.strip().lower()
        if '@' in email and '.' in email and len(email) > 5:
            # Basic validation
            if not email.startswith(('mailto:', 'email:', 'contact:')):
                valid_emails.append(email)

    return list(set(valid_emails))  # Remove duplicates

def extract_phones(lead_data: Dict[str, Any]) -> List[str]:
    """Extract phone numbers from lead data."""
    phones = []

    # Check direct phone field
    if lead_data.get('phone'):
        if isinstance(lead_data['phone'], list):
            phones.extend(lead_data['phone'])
        else:
            phones.append(lead_data['phone'])

    # Extract from notes or other text fields
    text_fields = ['notes', 'bio', 'description', 'address', 'organization']
    for field in text_fields:
        if lead_data.get(field):
            # Comprehensive phone regex patterns for business directories
            phone_patterns = [
                r'\+\d{1,3}[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{0,4}',  # International formats
                r'\b\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b',  # (123) 456-7890 or 123-456-7890
                r'\b\d{3}[-.\s]\d{3}[-.\s]\d{4}\b',          # 123-456-7890
                r'\b\d{3}\.\d{3}\.\d{4}\b',                   # 123.456.7890
                r'\b\d{3}\s\d{3}\s\d{4}\b',                   # 123 456 7890
                r'tel:\s*(\+?\d[\d\s\-\(\)]+)',              # tel: +1-234-567-8900
                r'phone["\']?\s*:\s*["\']?(\+?\d[\d\s\-\(\)]+)',  # phone: "+1-234-567-8900"
                r'contact["\']?\s*:\s*["\']?(\+?\d[\d\s\-\(\)]+)', # contact: "+1-234-567-8900"
                r'\b\d{10,11}\b',                             # 1234567890 or 11234567890
                r'\b1?\d{10}\b',                              # 11234567890 or 1234567890
                r'\(\d{3}\)\s*\d{3}[-.\s]?\d{4}',            # (123) 456-7890
                r'\d{3}[-.\s]\d{4}',                          # 456-7890 (extensions)
            ]
            for pattern in phone_patterns:
                found_phones = re.findall(pattern, lead_data[field])
                phones.extend(found_phones)

    # Validate and filter phone numbers
    valid_phones = []
    for phone in phones:
        phone = phone.strip()
        if is_valid_phone_number(phone):
            valid_phones.append(phone)

    return list(set(valid_phones))  # Remove duplicates

def is_valid_phone_number(phone: str) -> bool:
    """Validate if a phone number is in a valid format"""
    if not phone or len(phone.strip()) < 10:
        return False

    # Remove all non-digit characters
    digits_only = re.sub(r'\D', '', phone)

    # Must have 10 or 11 digits (with optional country code)
    if not (len(digits_only) == 10 or (len(digits_only) == 11 and digits_only.startswith('1'))):
        return False

    # Reject obviously invalid patterns
    invalid_patterns = [
        r'^0+$',      # All zeros
        r'^1+$',      # All ones
        r'^2+$',      # All twos
        r'^123456',   # Sequential numbers
        r'^987654',   # Reverse sequential
        r'(\d)\1{9}', # Same digit repeated 10+ times
        r'^202\d{7}', # Date-like patterns starting with 202 (2025, 2026, etc.)
        r'^201\d{7}', # Date-like patterns starting with 201
        r'^203\d{7}', # Date-like patterns starting with 203
    ]

    for pattern in invalid_patterns:
        if re.search(pattern, digits_only):
            return False

    return True

def _extract_json(text: str) -> Dict[str, Any]:
    """Extract first valid JSON object or array from text; return empty dict on failure."""
    try:
        # Try to find JSON in code blocks first
        json_match = re.search(r'```json\s*(\{.*?\})\s*```', text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group(1))

        # Try to find JSON in the text directly
        start = text.find("{")
        alt_start = text.find("[") if start == -1 else start
        if alt_start == -1 and start == -1:
            return {}
        if start == -1 or (alt_start != -1 and alt_start < start):
            start = alt_start

        # naive brace matching
        stack = []
        for i in range(start, len(text)):
            ch = text[i]
            if ch in "[{":
                stack.append(ch)
            elif ch in "]}":
                if not stack:
                    break
                open_ch = stack.pop()
                if (open_ch == "[" and ch != "]") or (open_ch == "{" and ch != "}"):
                    return {}
                if not stack:
                    snippet = text[start:i + 1]
                    return json.loads(snippet)
        return {}
    except Exception as e:
        logger.warning(f"JSON extraction failed: {e}")
        return {}

def _configure_gemini() -> bool:
    """Configure Gemini AI client"""
    global _HAS_GEMINI
    if _AI_DISABLED or not _AI_KEY:
        logger.warning(f"Gemini unavailable - Disabled: {_AI_DISABLED}, Has Key: {bool(_AI_KEY)}")
        return False

    try:
        genai.configure(api_key=_AI_KEY)
        _HAS_GEMINI = True
        return True
    except Exception as e:
        logger.warning(f"Gemini configuration failed: {e}")
        return False

def _should_call_ai() -> bool:
    """Check if AI call should be made based on rate limits and budget"""
    global _AI_CALLS, _AI_LAST_CALL_TIME

    if _AI_DISABLED:
        return False

    if _AI_CALLS >= _AI_MAX_CALLS:
        return False

    # Rate limiting
    current_time = time.time()
    if current_time - _AI_LAST_CALL_TIME < _AI_RATE_LIMIT:
        time.sleep(_AI_RATE_LIMIT - (current_time - _AI_LAST_CALL_TIME))

    return True

def _respect_rate_limits():
    """Ensure rate limits are respected"""
    global _AI_LAST_CALL_TIME
    current_time = time.time()
    if current_time - _AI_LAST_CALL_TIME < _AI_RATE_LIMIT:
        time.sleep(_AI_RATE_LIMIT - (current_time - _AI_LAST_CALL_TIME))

def _ai_generate_json(prompt: str, model_name: str = "gemini-2.0-flash") -> Optional[Dict[str, Any]]:
    """Safely invoke Gemini with rate limiting and error handling. Returns parsed JSON or None."""
    global _AI_CALLS, _AI_LAST_CALL_TIME

    if not _should_call_ai():
        logger.warning(f"AI call skipped. Disabled: {_AI_DISABLED}, Calls: {_AI_CALLS}/{_AI_MAX_CALLS}")
        return None

    if not _configure_gemini():
        logger.warning("Gemini configuration failed")
        return None

    try:
        _respect_rate_limits()
        model = genai.GenerativeModel(model_name)

        logger.info(f"Making AI call #{_AI_CALLS + 1}/{_AI_MAX_CALLS}")
        resp = model.generate_content(
            prompt,
            generation_config=genai.types.GenerationConfig(
                temperature=0.1,
                top_p=0.8,
                top_k=40,
                max_output_tokens=2048,
            )
        )

        _AI_CALLS += 1
        _AI_LAST_CALL_TIME = time.time()

        # Check if response was generated successfully
        if not resp or not resp.candidates:
            logger.warning("No response or candidates from AI")
            return None

        candidate = resp.candidates[0]

        # Check finish reason
        finish_reason = candidate.finish_reason
        if finish_reason == 1:  # STOP - normal completion
            if not resp.text:
                logger.warning("Empty response text from AI")
                return None

            logger.debug(f"AI response (first 500 chars): {resp.text[:500]}")

            result = _extract_json(resp.text)
            if result:
                logger.info(f"Successfully extracted JSON from AI response")
                return result
            else:
                logger.warning("Failed to extract JSON from AI response")
                logger.debug(f"Raw AI response: {resp.text}")
                return None

        elif finish_reason == 2:  # SAFETY
            logger.warning("AI response blocked by safety filters")
            logger.debug(f"Prompt that was blocked: {prompt[:200]}...")
            return None

        elif finish_reason == 3:  # RECITATION
            logger.warning("AI response was too similar to training data")
            return None

        else:
            logger.warning(f"AI response finished with unexpected reason: {finish_reason}")
            return None

    except Exception as e:
        logger.warning(f"AI call failed: {e}")
        return None

# =============================================================================
# ANTI-DETECTION SYSTEM
# =============================================================================

class AntiDetectionManager:
    """Manages anti-detection measures for web scraping"""

    def __init__(self):
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        ]
        self.locales = ['en-US', 'en-GB', 'en-CA', 'en-AU']
        self.timezones = ['America/New_York', 'Europe/London', 'America/Los_Angeles', 'Australia/Sydney']

    def get_random_user_agent(self) -> str:
        return random.choice(self.user_agents)

    def get_random_locale(self) -> str:
        return random.choice(self.locales)

    def get_random_timezone(self) -> str:
        return random.choice(self.timezones)

async def create_stealth_browser_context(browser: Browser, anti_detection: AntiDetectionManager) -> BrowserContext:
    """Create a browser context with stealth settings"""
    context = await browser.new_context(
        user_agent=anti_detection.get_random_user_agent(),
        locale=anti_detection.get_random_locale(),
        timezone_id=anti_detection.get_random_timezone(),
        viewport={'width': random.randint(1200, 1920), 'height': random.randint(800, 1080)},
        device_scale_factor=random.choice([1, 1.25, 1.5]),
        has_touch=random.choice([True, False]),
        is_mobile=random.choice([True, False]),
    )

    # Additional stealth settings
    await context.add_init_script("""
        // Override navigator properties
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});

        // Mock hardware concurrency
        Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 4});

        // Mock device memory
        Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});

        // Remove automation indicators
        delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
        delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
        delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
        delete window.cdc_adoQpoasnfa76pfcZLmcfl_JSON;
        delete window.cdc_adoQpoasnfa76pfcZLmcfl_Object;
        delete window.cdc_adoQpoasnfa76pfcZLmcfl_Proxy;
    """)

    return context

async def execute_human_behavior(page: Page, anti_detection: AntiDetectionManager, behavior_type: str, position: Tuple[int, int] = None):
    """Execute human-like behavior on the page"""
    try:
        if behavior_type == 'click':
            if position:
                x, y = position
                await page.mouse.move(x, y)
                await asyncio.sleep(random.uniform(0.1, 0.3))
                await page.mouse.click(x, y)
        elif behavior_type == 'scroll':
            await page.evaluate("""
                window.scrollTo({
                    top: Math.random() * document.body.scrollHeight,
                    behavior: 'smooth'
                });
            """)
        elif behavior_type == 'wait':
            await asyncio.sleep(random.uniform(1, 3))

        # Random additional human behavior
        if random.random() < 0.3:
            await page.mouse.move(
                random.randint(100, 800),
                random.randint(100, 600)
            )

    except Exception as e:
        logger.warning(f"Human behavior execution failed: {e}")

# =============================================================================
# AI INTEGRATION FUNCTIONS
# =============================================================================

def _has_meaningful_contact_info(lead: Dict) -> bool:
    """Check if a lead has meaningful contact information (email, phone, or address)."""
    return bool(
        lead.get('email') or
        lead.get('phone') or
        lead.get('address') or
        lead.get('social_media', {}).get('linkedin') or
        lead.get('social_media', {}).get('twitter') or
        lead.get('websites')
    )

def extract_client_info_from_sections(ai_input_data: Dict[str, Any], url: str = "") -> Dict[str, Any]:
    """Extract client information from filtered sections and structured data using Gemini AI LLM."""

    # Handle both old format (List) and new format (Dict)
    filtered_sections = ai_input_data.get("sections")
    structured_data = ai_input_data.get("structured_data")

    logger.info(f"Processing sections and structured data items for {url}")

    if not filtered_sections and not structured_data:
        logger.warning(f"No sections or structured data found for {url}")
        return {
            "leads": [],
            "organization_info": {},
            "addresses": [],
            "overall_confidence": 0.0,
            "summary": "No data to process"
        }

    # Prepare sections text for AI analysis (more concise)
    sections_text = ""
    for i, section_data in enumerate(filtered_sections[:5]):  # Limit to top 5 sections
        section = section_data.get("section", {})
        text = section.get("text", "")
        tag = section.get("tag", "")
        priority = section_data.get("priority_score", 0)

        if text.strip():  # Only include non-empty sections
            # Truncate very long sections
            truncated_text = text.strip()[:1000] + ("..." if len(text.strip()) > 1000 else "")
            sections_text += f"\nSection {i+1} ({tag}, Priority: {priority:.1f}):\n{truncated_text}\n"

    # Prepare structured data (more focused)
    structured_text = ""
    if structured_data:
        if isinstance(structured_data, list):
            for i, item in enumerate(structured_data[:3]):  # Limit to first 3 items
                item_str = json.dumps(item, indent=1)
                structured_text += f"\nStructured Item {i+1}:\n{item_str}\n"
        else:
            structured_str = json.dumps(structured_data, indent=1)
            structured_text = f"\nStructured Data:\n{structured_str}\n"

    prompt = f"""You are a business research assistant. Analyze the following company directory website content and extract available business information into a structured JSON format.

Website: {url}

Content Sections:
{sections_text}

Structured Data:
{structured_text}

Extract information for each company/business found. Include all available details in the exact format specified below.

IMPORTANT: Return the data in a "leads" array format as specified below. Do not return individual objects.

Guidelines:
- Extract all available information from the content
- Use null for missing information (not empty strings)
- Be accurate and only include information that is actually present
- Combine related information from multiple sections
- IMPORTANT: For "company_name", extract the ACTUAL business/company name, NOT the platform name (e.g., not "ThomasNet", "IndiaMart", "YellowPages", etc.)
- Look for specific company names, manufacturer names, supplier names, or business entity names
- For "type" field, classify as "lead" (potential customer) or "competitor" based on context
- For "what_we_can_offer" field, suggest relevant services/products we could provide based on the ICP data provided
- For "link_details" field, provide a short description of what the link/page contains
- If this appears to be a platform page (ThomasNet, etc.) rather than a specific company page, return empty leads array
- PRIORITIZE extracting leads that match the Ideal Customer Profile (ICP) data provided in the structured data section

Return valid JSON format with "leads" array:
{{
  "leads": [
    {{
      "name": "Contact person name or primary contact",
      "contact_info": {{
        "email": "primary email address",
        "phone": "primary phone number",
        "linkedin": "LinkedIn profile URL",
        "twitter": "Twitter handle or URL",
        "website": "company website URL",
        "others": "other contact methods",
        "socialmedialinks": ["array of all social media URLs"]
      }},
      "company_name": "Full company/organization name",
      "time": "current timestamp in ISO format",
      "link_details": "short description of the source link/page content",
      "type": "lead or competitor",
      "what_we_can_offer": "suggested services/products we can provide",
      "source_url": "{url}",
      "source_platform": "platform name (ThomasNet, IndiaMart, YellowPages, etc.)",
      "location": "city, state, country or geographic location",
      "industry": "industry sector or business category",
      "company_type": "company type (Private, Public, LLC, Corporation, etc.)",
      "bio": "company description or business summary",
      "address": "complete business address"
    }}
  ],
  "summary": "Summary of extracted business information",
  "total_leads": 0
}}"""

    ai_result = _ai_generate_json(prompt)
    if ai_result:
        logger.info(f"AI extraction successful for {url} - found {len(ai_result.get('leads', []))} potential clients")
        return ai_result

    # AI failed, try fallback extraction from structured data
    logger.warning(f"AI extraction failed for {url}, attempting fallback extraction")
    fallback_result = _extract_from_structured_data(filtered_sections, structured_data, url)
    if fallback_result and fallback_result.get('leads') and fallback_result['leads']:
        # Check if the fallback leads actually contain meaningful contact information
        meaningful_leads = [lead for lead in fallback_result['leads'] if _has_meaningful_contact_info(lead)]
        if meaningful_leads:
            logger.info(f"Fallback extraction successful for {url} - found {len(meaningful_leads)} potential clients with contact info")
            fallback_result['leads'] = meaningful_leads
            return fallback_result
        else:
            logger.warning(f"Fallback extraction found leads but no meaningful contact info for {url}, triggering page scraping")
            # Return result indicating page scraping is needed
            return {
                "leads": [],
                "organization_info": {},
                "addresses": [],
                "overall_confidence": 0.0,
                "summary": f"Fallback found leads but no contact details - page scraping needed for {url}",
                "needs_page_scraping": True
            }

    # Check if structured data fallback indicates page scraping is needed
    if fallback_result and fallback_result.get('needs_page_scraping'):
        logger.info(f"Structured data fallback indicates page scraping needed for {url}")
        return {
            "leads": [],
            "organization_info": {},
            "addresses": [],
            "overall_confidence": 0.0,
            "summary": f"Structured data extraction failed - page scraping needed for {url}",
            "needs_page_scraping": True
        }

    # Structured data fallback also failed, try comprehensive page scraping
    logger.warning(f"Structured data fallback failed for {url}, attempting comprehensive page scraping")
    try:
        # Import here to avoid circular imports
        from playwright.async_api import Page
        # We need access to the page object, but this function doesn't have it
        # So we'll return a special marker that the caller can handle
        return {
            "leads": [],
            "organization_info": {},
            "addresses": [],
            "overall_confidence": 0.0,
            "summary": f"All extraction methods failed - page scraping needed for {url}",
            "needs_page_scraping": True
        }
    except Exception as e:
        logger.warning(f"Comprehensive page scraping setup failed: {e}")

    # Both AI and fallback failed
    logger.warning(f"Both AI and fallback extraction failed for {url}")
    return {
        "leads": [],
        "organization_info": {
            "primary_name": None,
            "industry": None,
            "services": [],
            "location": None,
            "organization_type": None
        },
        "addresses": [],
        "overall_confidence": 0.0,
        "summary": f"AI extraction failed - processed {len(filtered_sections)} sections and structured data from {url}"
    }

def _extract_from_structured_data(filtered_sections: List[Dict], structured_data: List[Dict], url: str) -> Optional[Dict[str, Any]]:
    """Fallback extraction method that parses structured data and HTML without using AI."""
    try:
        contacts = []

        # Extract from structured data (JSON-LD, microdata, etc.)
        if structured_data:
            for item in structured_data:
                if isinstance(item, dict):
                    contact = _extract_company_from_structured_item(item, url)
                    if contact:
                        contacts.append(contact)

        # Extract from HTML sections if no structured data contacts found
        if not contacts and filtered_sections:
            for section in filtered_sections:
                contact = _extract_company_from_section(section, url)
                if contact:
                    contacts.append(contact)

        if contacts:
            return {
                "leads": contacts,
                "organization_info": {
                    "primary_name": contacts[0].get("name") if contacts else None,
                    "industry": contacts[0].get("industry") if contacts else None,
                    "services": [],
                    "location": contacts[0].get("location") if contacts else None,
                    "organization_type": contacts[0].get("company_type") if contacts else None
                },
                "addresses": [],
                "overall_confidence": 0.6,
                "summary": f"Fallback extraction found {len(contacts)} companies from structured data and HTML"
            }

        # No meaningful contacts found, signal that page scraping is needed
        return {
            "leads": [],
            "organization_info": {},
            "addresses": [],
            "overall_confidence": 0.0,
            "summary": "No contacts found in structured data - page scraping needed",
            "needs_page_scraping": True
        }

    except Exception as e:
        logger.warning(f"Fallback extraction failed: {e}")
        return None

def _extract_company_from_structured_item(item: Dict, url: str) -> Optional[Dict]:
    """Extract company info from a single structured data item."""
    try:
        contact = {
            "name": None,
            "email": None,
            "phone": None,
            "organization": None,
            "role": "Company/Organization",
            "confidence": 0.7,
            "source": f"structured_data_{url}",
            "notes": "",
            "lead_category": "B2B",
            "lead_sub_category": "",
            "bio": "",
            "industry": None,
            "company_type": None,
            "location": None,
            "employee_count": None,
            "revenue": None,
            "websites": [],
            "social_media": {
                "linkedin": None,
                "twitter": None,
                "facebook": None,
                "instagram": None,
                "youtube": None,
                "other": []
            },
            "address": None,
            "status": "active",
            "incorporation_date": None,
            "jurisdiction": None
        }

        # Extract basic info
        if item.get("@type") == "Organization" or item.get("type") == "Organization":
            contact["organization"] = item.get("name")
            contact["bio"] = item.get("description", "")
            contact["websites"] = [item.get("url")] if item.get("url") else []

            # Extract contact info
            if item.get("contactPoint"):
                contact_point = item["contactPoint"]
                if isinstance(contact_point, list):
                    contact_point = contact_point[0]
                contact["email"] = contact_point.get("email")
                contact["phone"] = contact_point.get("telephone")

            # Extract address
            if item.get("address"):
                address = item["address"]
                if isinstance(address, dict):
                    street = address.get("streetAddress", "")
                    city = address.get("addressLocality", "")
                    state = address.get("addressRegion", "")
                    zip_code = address.get("postalCode", "")
                    country = address.get("addressCountry", "")
                    contact["address"] = f"{street}, {city}, {state} {zip_code}, {country}".strip(", ")

        return contact

    except Exception as e:
        logger.warning(f"Failed to extract from structured item: {e}")
        return None

def _extract_company_from_section(section: Dict, url: str) -> Optional[Dict]:
    """Extract company info from HTML section."""
    try:
        section_data = section.get("section", {})
        text = section_data.get("text", "")

        # Determine source platform from URL
        from urllib.parse import urlparse
        domain = urlparse(url).netloc.replace('www.', '')
        if 'indiamart.com' in domain:
            source_platform = "IndiaMart"
        elif 'thomasnet.com' in domain:
            source_platform = "ThomasNet"
        elif 'linkedin.com' in domain:
            source_platform = "LinkedIn"
        elif 'kompass.com' in domain:
            source_platform = "Kompass"
        elif 'yellowpages.com' in domain:
            source_platform = "YellowPages"
        else:
            source_platform = domain.split('.')[0].title()

        contact = {
            "name": None,
            "contact_info": {
                "email": None,
                "phone": None,
                "website": None,
                "linkedin": None,
                "address": None
            },
            "company_name": None,
            "industry": None,
            "location": None,
            "company_type": None,
            "bio": None,
            "services": [],
            "source_url": url,
            "source_platform": source_platform,
            "confidence": 0.5,
            "time": datetime.now(timezone.utc).isoformat(),
            "type": "lead"
        }

        # Extract emails and phones
        extracted_emails = extract_emails({"notes": text})
        extracted_phones = extract_phones({"notes": text})

        if extracted_emails:
            contact["contact_info"]["email"] = extracted_emails[0] if len(extracted_emails) == 1 else extracted_emails
        if extracted_phones:
            contact["contact_info"]["phone"] = extracted_phones[0] if len(extracted_phones) == 1 else extracted_phones

        # Extract additional information from text content (without HTML)
        soup = BeautifulSoup(text, 'html.parser')
        text_content = soup.get_text(separator=' ', strip=True)

        # Try to extract company name from title or headings
        title = soup.find('title')
        if title:
            title_text = title.get_text(strip=True)
            # Remove common platform suffixes
            title_text = re.sub(r'\s*-\s*IndiaMart.*', '', title_text, flags=re.IGNORECASE)
            title_text = re.sub(r'\s*-\s*ThomasNet.*', '', title_text, flags=re.IGNORECASE)
            title_text = re.sub(r'\s*-\s*LinkedIn.*', '', title_text, flags=re.IGNORECASE)
            if len(title_text) > 3 and len(title_text) < 100:
                contact["company_name"] = title_text

        # Extract location/address information
        address_patterns = [
            r'(\d+\s+[^,]+,\s*[^,]+,\s*\w{2}\s*\d{5})',  # Street, City, State ZIP
            r'([^,]+,\s*\w{2}\s*\d{5})',  # City, State ZIP
            r'([^,]+,\s*[^,]+)',  # City, Country
        ]
        for pattern in address_patterns:
            addresses = re.findall(pattern, text_content, re.IGNORECASE)
            if addresses:
                contact["contact_info"]["address"] = addresses[0].strip()
                contact["location"] = addresses[0].strip()
                break

        # Extract bio/description (first meaningful paragraph)
        paragraphs = soup.find_all('p')
        for p in paragraphs[:3]:  # Check first 3 paragraphs
            p_text = p.get_text(strip=True)
            if len(p_text) > 50 and len(p_text) < 500:  # Reasonable length
                contact["bio"] = p_text
                break

        # Extract industry/service information
        industry_keywords = ['manufacturing', 'services', 'consulting', 'technology', 'engineering',
                           'construction', 'retail', 'wholesale', 'healthcare', 'finance', 'education']
        for keyword in industry_keywords:
            if keyword.lower() in text_content.lower():
                contact["industry"] = keyword.title()
                break

        # Extract LinkedIn profile if available
        linkedin_match = re.search(r'linkedin\.com/(?:company|in)/([^"\'>\s]+)', text_content, re.IGNORECASE)
        if linkedin_match:
            contact["contact_info"]["linkedin"] = f"https://linkedin.com/company/{linkedin_match.group(1)}"

        # Extract website URL
        website_match = re.search(r'https?://(?:www\.)?([^"\'>\s]+\.(?:com|net|org|co|in|biz))', text_content)
        if website_match and 'linkedin.com' not in website_match.group(0).lower():
            contact["contact_info"]["website"] = website_match.group(0)

        return contact if (extracted_emails or extracted_phones or contact["company_name"] or contact["bio"]) else None

    except Exception as e:
        logger.warning(f"Failed to extract from section: {e}")
        return None

# =============================================================================
# CONTACT ENHANCEMENT FUNCTIONS
# =============================================================================

async def enhance_contact_details(page: Page, lead_data: Dict[str, Any]) -> Dict[str, Any]:
    """Enhance contact details by extracting additional information from the page"""
    try:
        # Get all text content from the page
        page_text = await page.inner_text('body')

        # Extract additional emails and phones
        additional_emails = extract_emails({"notes": page_text})
        additional_phones = extract_phones({"notes": page_text})

        # Update lead data with enhanced information
        enhanced_lead = lead_data.copy()

        # Handle both old format (direct fields) and new format (contact_info structure)
        existing_phones = []
        if enhanced_lead.get("contact_info", {}).get("phone"):
            phone = enhanced_lead["contact_info"]["phone"]
            if phone:
                existing_phones = [phone] if isinstance(phone, str) else phone
        elif enhanced_lead.get("phone"):
            phone = enhanced_lead["phone"]
            if phone:
                existing_phones = [phone] if isinstance(phone, str) else phone

        # Add phone numbers if not already present
        if additional_phones:
            # Filter out phones that are already present
            new_phones = [p for p in additional_phones if p not in existing_phones]
            existing_phones.extend(new_phones)
            existing_phones = list(set(existing_phones))  # Remove duplicates

        # Update the lead data with phones
        if existing_phones:
            if len(existing_phones) == 1:
                if "contact_info" in enhanced_lead:
                    enhanced_lead["contact_info"]["phone"] = existing_phones[0]
                else:
                    enhanced_lead["phone"] = existing_phones[0]
            else:
                if "contact_info" in enhanced_lead:
                    enhanced_lead["contact_info"]["phone"] = existing_phones
                else:
                    enhanced_lead["phone"] = existing_phones

        # Handle both old format and new format for emails
        existing_emails = []
        if enhanced_lead.get("contact_info", {}).get("email"):
            email = enhanced_lead["contact_info"]["email"]
            if email:
                existing_emails = [email] if isinstance(email, str) else email
        elif enhanced_lead.get("email"):
            email = enhanced_lead["email"]
            if email:
                existing_emails = [email] if isinstance(email, str) else email

        # Add email addresses if not already present
        if additional_emails:
            # Filter out emails that are already present
            new_emails = [e for e in additional_emails if e not in existing_emails]
            existing_emails.extend(new_emails)
            existing_emails = list(set(existing_emails))  # Remove duplicates

        # Update the lead data with emails
        if existing_emails:
            if len(existing_emails) == 1:
                if "contact_info" in enhanced_lead:
                    enhanced_lead["contact_info"]["email"] = existing_emails[0]
                else:
                    enhanced_lead["email"] = existing_emails[0]
            else:
                if "contact_info" in enhanced_lead:
                    enhanced_lead["contact_info"]["email"] = existing_emails
                else:
                    enhanced_lead["email"] = existing_emails

        logger.info(f"Enhanced contact extraction found: {len(additional_emails)} emails, {len(additional_phones)} phones")
        return enhanced_lead

    except Exception as e:
        logger.warning(f"Contact enhancement failed: {e}")
        return lead_data

# =============================================================================
# PAGE CLASSIFICATION
# =============================================================================

def classify_page_type(url: str, html_content: str) -> str:
    """Classify the type of page based on URL and content"""
    url_lower = url.lower()
    content_lower = html_content.lower()

    # Search results page - check for common search result indicators
    if ('search' in url_lower or 'find' in url_lower or 'query' in url_lower or
        any(param in url_lower for param in ['q=', 'query=', 'search=', 'keyword=', 'find_desc=', 'ss=', 'search_terms='])):
        if any(keyword in content_lower for keyword in ['result', 'company', 'business', 'supplier', 'manufacturer', 'listing', 'search', 'found', 'matches']):
            return "search_results"

    # Company directory patterns - broader detection
    if any(keyword in url_lower for keyword in ['directory', 'companies', 'business', 'suppliers']):
        if any(keyword in content_lower for keyword in ['company', 'business', 'supplier', 'manufacturer', 'directory']):
            return "company_directory"

    # Individual company page patterns
    if any(keyword in url_lower for keyword in ['profile', 'company', 'business', 'supplier', 'product']):
        if any(keyword in content_lower for keyword in ['contact', 'about', 'services', 'products', 'phone', 'email']):
            return "company_profile"

    # Generic business directory - if it has multiple business listings
    if any(keyword in content_lower for keyword in ['companies', 'businesses', 'suppliers', 'manufacturers']):
        return "company_directory"

    return "unknown"

# =============================================================================
# MAIN UNIVERSAL SCRAPER CLASS
# =============================================================================

class UniversalScraper:
    """Universal web scraper that works with any business directory"""

    def __init__(self, url: Optional[str] = None, search_query: Optional[str] = None, max_pages: int = 5,
                 base_url: Optional[str] = None, company_name: Optional[str] = None,
                 search_input_selector: Optional[str] = None, search_button_selector: Optional[str] = None,
                 next_page_selector: Optional[str] = None, max_pages_to_scrape: Optional[int] = None):
        # Handle both old and new API parameters
        if url is not None:
            # Old API
            self.url = url
            self.search_query = search_query
            self.max_pages = max_pages
            self.base_url = None
            self.company_name = search_query
            self.search_input_selector = None
            self.search_button_selector = None
            self.next_page_selector = None
            self.max_pages_to_scrape = max_pages
        else:
            # New API
            self.url = base_url
            self.search_query = company_name
            self.max_pages = max_pages_to_scrape or 5
            self.base_url = base_url
            self.company_name = company_name
            self.search_input_selector = search_input_selector
            self.search_button_selector = search_button_selector
            self.next_page_selector = next_page_selector
            self.max_pages_to_scrape = max_pages_to_scrape

        # Initialize anti-detection
        self.anti_detection_manager = AntiDetectionManager()

        # Browser instances
        self.browser = None
        self.context = None
        self.page = None

    def get_hardcoded_icp(self) -> Dict[str, Any]:
        """
        Get hardcoded ICP (Ideal Customer Profile) data
        In future versions, this will come from user forms
        """
        return {
            "product_details": {
                "product_name": "Premium Bus Travel & Group Tour Services",
                "product_category": "Travel & Tourism/Transportation Services",
                "usps": [
                    "Luxury bus fleet with premium amenities",
                    "Custom corporate group travel packages",
                    "Exclusive high-end travel experiences",
                    "Professional tour planning and coordination",
                    "Cost-effective group travel solutions",
                    "24/7 customer support during travel"
                ],
                "pain_points_solved": [
                    "Complicated group travel logistics",
                    "Expensive individual travel arrangements",
                    "Lack of customized corporate travel options",
                    "Poor coordination for large group events",
                    "Safety concerns in group transportation",
                    "Time-consuming travel planning process"
                ]
            },
            "icp_information": {
                "target_industry": [
                    "Corporate Companies",
                    "Educational Institutions",
                    "Wedding Planners",
                    "Event Management",
                    "Religious Organizations",
                    "Sports Teams/Clubs",
                    "Family Reunion Organizers",
                    "Travel Influencers"
                ],
                "competitor_companies": [
                    "RedBus",
                    "MakeMyTrip",
                    "Yatra",
                    "Local tour operators",
                    "Private bus operators",
                    "Luxury Bus Company", 
                    "Premium Tour Operator", 
                    "Corporate Travel Agency"
                ],
                "company_size": "10-1000+ employees/members",
                "decision_maker_persona": [
                    "HR Manager",
                    "Event Coordinator",
                    "Travel Manager",
                    "Family Head/Organizer",
                    "Wedding Planner",
                    "School/College Administrator",
                    "Corporate Executive",
                    "Travel Influencer",
                    "Religious Leader/Organizer"
                ],
                "region": ["India", "Major Cities", "Tourist Destinations"],
                "budget_range": "$5,000-$50,000 annually",
                "occasions": [
                    "Corporate offsites",
                    "Wedding functions",
                    "Family vacations",
                    "Educational tours",
                    "Religious pilgrimages",
                    "Adventure trips",
                    "Destination weddings",
                    "Sports events"
                ]
            }
        }

    async def __aenter__(self):
        """Async context manager entry"""
        await self._initialize_browser()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self._close_browser()

    async def _initialize_browser(self):
        """Initialize browser with stealth settings"""
        try:
            playwright = await async_playwright().start()
            self.browser = await playwright.chromium.launch(
                headless=True,  # Run in headless mode (no browser window)
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-accelerated-2d-canvas',
                    '--no-first-run',
                    '--no-zygote',
                    '--disable-gpu',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor'
                ]
            )

            self.context = await create_stealth_browser_context(self.browser, self.anti_detection_manager)
            self.page = await self.context.new_page()

            logger.info("Browser initialized successfully with stealth settings")

        except Exception as e:
            logger.error(f"Failed to initialize browser: {e}")
            raise

    async def _close_browser(self):
        """Close browser and cleanup"""
        try:
            if self.page:
                await self.page.close()
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            logger.info("Browser closed successfully")
        except Exception as e:
            logger.warning(f"Error closing browser: {e}")

    async def _navigate_to_url(self, url: str) -> bool:
        """Navigate to URL with error handling and retries"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                logger.info(f"Navigating to URL: {url}")
                await self.page.goto(url, wait_until="domcontentloaded", timeout=30000)

                # Wait for page to stabilize
                await asyncio.sleep(2)

                # Execute human-like behavior
                await execute_human_behavior(self.page, self.anti_detection_manager, 'scroll')
                await asyncio.sleep(1)

                return True

            except Exception as e:
                logger.warning(f"Navigation attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Failed to navigate to {url} after {max_retries} attempts")
                    return False

        return False

    async def scrape_any_website(self) -> Dict[str, Any]:
        """Main scraping method that works with any business directory"""
        try:
            # Initialize browser if not already done
            if not self.browser:
                await self._initialize_browser()

            # Navigate to the target URL only if we're not already on a page
            if not self.page or self.page.url == 'about:blank':
                if not await self._navigate_to_url(self.url):
                    return {"error": "Failed to navigate to URL", "extracted_data": []}

            # Detect page type
            html_content = await self.page.content()
            current_url = self.page.url
            page_type = classify_page_type(current_url, html_content)
            logger.info(f"📋 Current URL: {current_url}")
            logger.info(f"📋 Detected page type: {page_type}")
            
            # Debug: log some content indicators
            content_preview = html_content[:500].lower()
            has_companies = 'company' in content_preview or 'business' in content_preview
            has_results = 'result' in content_preview or 'listing' in content_preview
            logger.info(f"📋 Content indicators - Has companies/business: {has_companies}, Has results/listings: {has_results}")

            # Route to appropriate scraping method
            if page_type == "company_directory":
                return await self._scrape_company_directory()
            elif page_type == "company_profile":
                return await self._scrape_company_profile()
            elif page_type == "search_results":
                return await self._scrape_search_results()
            else:
                logger.warning(f"Unknown page type: {page_type}, attempting generic scraping")
                # Check if this might be a search results page based on URL
                url_lower = current_url.lower()
                if ('search' in url_lower or 'find' in url_lower or 'query' in url_lower or
                    any(param in url_lower for param in ['q=', 'query=', 'search=', 'keyword=', 'find_desc=', 'ss=', 'search_terms='])):
                    logger.info("Detected search parameters in URL, treating as search results")
                    return await self._scrape_universal_search_results()
                # Only attempt generic scraping if it looks like search results
                elif any(keyword in html_content.lower() for keyword in ['results', 'companies', 'businesses', 'listings']):
                    return await self._scrape_universal_search_results()
                else:
                    return {
                        "page_type": "unknown",
                        "extracted_data": [],
                        "message": "Page doesn't appear to be search results - skipping extraction"
                    }

        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            return {"error": str(e), "extracted_data": []}

    async def _scrape_search_results(self) -> Dict[str, Any]:
        """Scrape search results page"""
        return await self._scrape_universal_search_results()

    # REMOVED: _detect_directory_type - No longer needed, fully dynamic now

    # REMOVED: All hardcoded site-specific scraping methods
    # Now uses only universal AI-powered scraping

    # REMOVED: All hardcoded site-specific scraping methods (ThomasNet, Kompass, Crunchbase, etc.)
    # Now uses only universal AI-powered scraping that works with ANY business directory

    async def _scrape_universal_directory(self) -> Dict[str, Any]:
        """Universal directory scraping that works with any business directory"""
        logger.info("🌍 Scraping universal directory - works with any business directory")

        # Detect if this is a search results page
        html_content = await self.page.content()
        current_url = self.page.url

        # Check if we're actually on a search results page
        is_search_results = (
            "search" in current_url.lower() or
            any(param in current_url.lower() for param in ['q=', 'query=', 'search=', 'keyword=', 'find_desc=']) or
            any(keyword in html_content.lower() for keyword in ['results', 'companies found', 'business listings', 'search results'])
        )

        if is_search_results:
            logger.info("✅ Confirmed: On search results page, extracting company URLs")
            return await self._scrape_universal_search_results()
        else:
            logger.warning("❌ Not on search results page, skipping extraction from main website")
            return {
                "page_type": "main_website",
                "extracted_data": [],
                "total_companies": 0,
                "source": "main_website_skipped",
                "message": "Not on search results page - main website extraction skipped as requested"
            }

    async def _extract_contacts_from_search_listings(self) -> List[Dict[str, Any]]:
        """Extract contact information directly from search results listings on any business directory."""
        try:
            # Get the full page content
            html_content = await self.page.content()
            current_url = self.page.url

            contacts = []

            # Use BeautifulSoup for better HTML parsing
            soup = BeautifulSoup(html_content, 'html.parser')

            # More targeted selectors for actual business listings (not navigation/ads)
            result_selectors = [
                # Common business directory patterns
                '.business-result', '.company-result', '.listing-item', '.search-result-item',
                '.business-card', '.company-card', '.listing-card', '.result-card',
                '.business-listing', '.company-listing', '.directory-listing',
                '.supplier-item', '.vendor-item', '.business-item',

                # Specific website patterns
                '.result', '.listing',  # Generic but filtered below
                '[class*="result"]', '[class*="listing"]', '[class*="business"]',
                '[class*="company"]', '[class*="supplier"]',

                # ThomasNet specific
                '.supplier-result', '.product-result',

                # YellowPages specific
                '.business-result-item', '.listing-item',

                # Yelp specific
                '.business-container', '.search-result',

                # Generic containers that might hold business data
                '.card', '.item', '.entry'
            ]

            result_elements = []
            for selector in result_selectors:
                elements = soup.select(selector)
                if elements:
                    result_elements.extend(elements)

            # Remove duplicates while preserving order
            seen = set()
            unique_elements = []
            for elem in result_elements:
                elem_str = str(elem)[:200]  # Use first 200 chars as identifier
                if elem_str not in seen:
                    seen.add(elem_str)
                    unique_elements.append(elem)

            result_elements = unique_elements

            # Filter out non-business elements (navigation, ads, etc.)
            filtered_elements = []
            exclude_keywords = [
                'about us', 'contact us', 'advertise', 'claim your business',
                'manage listing', 'login', 'register', 'sign up', 'subscribe',
                'privacy policy', 'terms of service', 'help', 'support',
                'navigation', 'menu', 'footer', 'header', 'sidebar',
                'popular cities', 'top categories', 'related searches',
                'sponsored', 'advertisement', 'promo', 'featured',
                'company news', 'news', 'latest news', 'industry news',
                'press release', 'announcement', 'update', 'blog',
                'article', 'story', 'post', 'feed', 'newsletter',
                'search results', 'search', 'find', 'browse',
                'categories', 'directory', 'listings', 'results',
                'home', 'main', 'index', 'welcome', 'overview'
            ]

            for element in result_elements:
                element_text = element.get_text().strip().lower()

                # Skip if it contains exclusion keywords
                if any(exclude in element_text for exclude in exclude_keywords):
                    continue

                # Skip if it's too short (likely navigation/ads)
                if len(element_text) < 30:  # Increased minimum length
                    continue

                # Skip if it looks like a link list or menu
                links = element.find_all('a')
                if len(links) > 5:  # Reduced threshold for links
                    continue

                # Must have some business-like content AND contact information
                business_indicators = [
                    'phone', 'tel', 'call', 'contact', 'address', 'street',
                    'email', 'mail', 'website', 'web', 'site',
                    'inc', 'llc', 'corp', 'ltd', 'limited', 'incorporated',
                    'professional', 'center', 'clinic', 'hospital', 'office',
                    'services', 'repair', 'maintenance', 'installation'
                ]

                has_business_indicator = any(indicator in element_text for indicator in business_indicators)

                # Must have contact information (phone, email, or address)
                has_contact_info = (
                    'phone' in element_text or 'tel' in element_text or
                    'email' in element_text or 'mail' in element_text or
                    'address' in element_text or 'street' in element_text or
                    '@' in element_text or  # Email symbol
                    any(char.isdigit() for char in element_text)  # Numbers for phone/address
                )

                # Check for structured data that indicates a business
                has_structured_data = bool(
                    element.find(attrs={"itemtype": re.compile(r'Organization|LocalBusiness|Place')}) or
                    element.find(attrs={"itemprop": re.compile(r'name|telephone|email|address')}) or
                    element.find('span', class_=re.compile(r'phone|address|name|email'))
                )

                # Must have both business indicators AND contact info, OR structured data
                if (has_business_indicator and has_contact_info) or has_structured_data:
                    filtered_elements.append(element)

            result_elements = filtered_elements[:20]  # Limit to top 20 most relevant

            logger.info(f"Found {len(result_elements)} filtered business result containers")

            for i, element in enumerate(result_elements[:15]):  # Limit to first 15 results
                try:
                    # Extract text content for processing
                    element_html = str(element)
                    element_text = element.get_text().strip()

                    # Create contact structure
                    contact = {
                        "name": "",
                        "email": [],
                        "phone": [],
                        "organization": "",
                        "role": None,
                        "confidence": 0.7,  # Higher confidence for filtered results
                        "source": f"Search Results ({urlparse(current_url).netloc})",
                        "notes": element_text[:300],  # Shorter notes for cleaner data
                        "lead_category": "Business Directory",
                        "lead_sub_category": "",
                        "bio": None,
                        "industry": "",
                        "company_type": "Business",
                        "location": "",
                        "employee_count": None,
                        "revenue": None,
                        "websites": [],
                        "social_media": {
                            "linkedin": None,
                            "twitter": None,
                            "facebook": None,
                            "instagram": None,
                            "youtube": None
                        },
                        "address": "",
                        "status": "active",
                        "incorporation_date": None,
                        "jurisdiction": None
                    }

                    # Extract business/company name - more targeted approach
                    business_name = ""

                    # First try structured data (schema.org)
                    name_elem = element.find(attrs={"itemprop": "name"}) or element.find(attrs={"itemtype": re.compile(r'Organization|LocalBusiness')})
                    if name_elem:
                        if name_elem.get('itemprop') == 'name':
                            business_name = name_elem.get_text().strip()
                        else:
                            name_sub = name_elem.find(attrs={"itemprop": "name"})
                            if name_sub:
                                business_name = name_sub.get_text().strip()

                    # Try specific selectors for business names
                    if not business_name:
                        name_selectors = [
                            'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
                            '.business-name', '.company-name', '.listing-title',
                            '.result-title', '.item-title', '.card-title',
                            '.business-title', '.company-title',
                            'a.business-name', 'a.company-name',
                            '.name', '.title', '.heading'
                        ]

                        for selector in name_selectors:
                            name_elem = element.select_one(selector)
                            if name_elem:
                                business_name = name_elem.get_text().strip()
                                # Clean up common artifacts
                                business_name = re.sub(r'^[•·*]\s*', '', business_name)  # Remove bullets
                                business_name = re.sub(r'\s+', ' ', business_name)  # Normalize spaces
                                if len(business_name) > 2 and len(business_name) < 100:  # Reasonable length
                                    break

                    # Extract from text patterns if still no name - more strict filtering
                    if not business_name:
                        # Look for capitalized business names with stricter filtering
                        name_patterns = [
                            r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+(?:\s+(?:Inc|LLC|Corp|Co|Ltd|Plumbing|Services|Company|Group|Enterprises|Center|Professional|Associates|Partners|Repair|Maintenance|Installation|Heating|Cooling|Air|Conditioning))?)',
                            r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+(?:\s+[A-Z][a-z]+))',  # Three+ word names
                            r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)',  # Two+ word names
                        ]

                        for pattern in name_patterns:
                            matches = re.findall(pattern, element_text)
                            for match in matches:
                                match = match.strip()
                                # Filter out common non-business names and generic terms
                                skip_terms = [
                                    'about', 'contact', 'home', 'services', 'business', 'company',
                                    'search', 'find', 'browse', 'directory', 'listings', 'results',
                                    'news', 'article', 'story', 'post', 'blog', 'update',
                                    'press release', 'announcement', 'featured', 'popular',
                                    'top', 'best', 'new', 'latest', 'recent', 'trending'
                                ]
                                if not any(skip in match.lower() for skip in skip_terms):
                                    if 5 < len(match) < 80:  # Longer minimum length
                                        business_name = match
                                        break
                            if business_name:
                                break

                    if business_name:
                        contact["name"] = business_name
                        contact["organization"] = business_name

                    # Extract phone numbers - more comprehensive
                    phone_patterns = [
                        r'\b\(\d{3}\)\s*\d{3}[-.\s]?\d{4}\b',  # (123) 456-7890
                        r'\b\d{3}[-.\s]\d{3}[-.\s]\d{4}\b',    # 123-456-7890
                        r'\b\d{3}\.\d{3}\.\d{4}\b',             # 123.456.7890
                        r'\b\d{3}\s\d{3}\s\d{4}\b',             # 123 456 7890
                        r'\+\d{1,3}\s*\(\d{3}\)\s*\d{3}[-.\s]\d{4}',  # +1 (123) 456-7890
                        r'\b\d{10,11}\b',                        # 1234567890
                        r'tel:\s*([+\d\s\-\(\)]{10,})',         # tel: +1-234-567-8900
                        r'phone:\s*([+\d\s\-\(\)]{10,})',       # phone: (123) 456-7890
                        r'call:\s*([+\d\s\-\(\)]{10,})',        # call: 123-456-7890
                    ]

                    phones_found = []
                    for pattern in phone_patterns:
                        matches = re.findall(pattern, element_html, re.IGNORECASE)
                        phones_found.extend(matches)

                    # Clean and validate phones
                    valid_phones = []
                    for phone in phones_found:
                        phone = phone.strip()
                        # Remove prefixes
                        phone = re.sub(r'^(tel|phone|call)[:.\s]*', '', phone, flags=re.IGNORECASE)
                        if is_valid_phone_number(phone):
                            valid_phones.append(phone)

                    contact["phone"] = list(set(valid_phones))

                    # Extract email addresses - more comprehensive
                    email_patterns = [
                        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
                        r'mailto:\s*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})',
                        r'email[:.\s]*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})',
                        r'contact[:.\s]*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})',
                        r'info@\S+\.\S+',
                        r'sales@\S+\.\S+',
                        r'support@\S+\.\S+',
                    ]

                    emails_found = []
                    for pattern in email_patterns:
                        matches = re.findall(pattern, element_html, re.IGNORECASE)
                        emails_found.extend(matches)

                    # Clean emails
                    valid_emails = []
                    for email in emails_found:
                        email = email.strip().lower()
                        # Remove prefixes
                        email = re.sub(r'^(mailto|email|contact)[:.\s]*', '', email, flags=re.IGNORECASE)
                        if '@' in email and '.' in email and len(email) > 5:
                            # Basic validation
                            if not email.startswith(('http://', 'https://', 'www.')):
                                valid_emails.append(email)

                    contact["email"] = list(set(valid_emails))

                    # Extract address/location - more targeted
                    address_text = ""

                    # Try structured data first
                    address_elem = element.find(attrs={"itemprop": "address"})
                    if address_elem:
                        address_text = address_elem.get_text().strip()
                    else:
                        # Try specific selectors
                        address_selectors = [
                            '.address', '.location', '.street-address', '.business-address',
                            '[class*="address"]', '[class*="location"]', '.adr', '.street'
                        ]
                        for selector in address_selectors:
                            addr_elem = element.select_one(selector)
                            if addr_elem:
                                address_text = addr_elem.get_text().strip()
                                break

                    # Extract from text patterns
                    if not address_text:
                        address_patterns = [
                            r'\d+\s+[A-Za-z0-9\s,.-]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Way|Place|Pl|Court|Ct|Circle|Cir|Parkway|Pkwy|Highway|Hwy)\s*,?\s*[A-Za-z\s]+,?\s*\d{5}',
                            r'\d+\s+[A-Za-z\s,.-]+,\s*[A-Za-z\s]+,?\s*\d{5}',
                            r'[A-Za-z\s]+,\s*[A-Z]{2}\s*\d{5}',  # City, State ZIP
                        ]
                        for pattern in address_patterns:
                            matches = re.findall(pattern, element_text, re.IGNORECASE)
                            if matches:
                                address_text = matches[0].strip()
                                break

                    contact["address"] = address_text
                    contact["location"] = address_text

                    # Extract website URLs
                    website_patterns = [
                        r'https?://[^\s<>"\']+(?:\.[a-zA-Z]{2,})+(?:/[^\s<>"\']*)?',
                        r'www\.[^\s<>"\']+(?:\.[a-zA-Z]{2,})+(?:/[^\s<>"\']*)?',
                        r'http://[^\s<>"\']+(?:\.[a-zA-Z]{2,})+(?:/[^\s<>"\']*)?',
                    ]

                    websites_found = []
                    for pattern in website_patterns:
                        matches = re.findall(pattern, element_html, re.IGNORECASE)
                        websites_found.extend(matches)

                    # Filter out unwanted URLs
                    filtered_websites = []
                    for url in websites_found:
                        url_lower = url.lower()
                        if not any(skip in url_lower for skip in ['facebook.com/l/', 'linkedin.com/company/', 'twitter.com/', 'instagram.com/', 'yelp.com/', 'google.com/maps', 'maps.google.com']):
                            filtered_websites.append(url)

                    contact["websites"] = list(set(filtered_websites))

                    # Extract industry/service type from context
                    if 'ac services' in element_text.lower():
                        contact["industry"] = "Air Conditioning Services"
                        contact["lead_sub_category"] = "HVAC"

                    # Only add contact if we have meaningful business data AND contact information
                    has_business_name = bool(contact["name"] and len(contact["name"]) > 5)
                    has_contact_info = bool(
                        contact["phone"] or
                        contact["email"] or
                        (contact["address"] and len(contact["address"]) > 15)
                    )

                    # Must have both a business name AND contact information
                    if has_business_name and has_contact_info:
                        contacts.append(contact)
                        logger.info(f"✅ Extracted business contact: {contact['name']} - Phones: {len(contact['phone'])}, Emails: {len(contact['email'])}, Address: {bool(contact['address'])}")
                    else:
                        logger.debug(f"Skipping contact - Business name: {has_business_name}, Contact info: {has_contact_info}")

                except Exception as e:
                    logger.debug(f"Error processing result element {i}: {e}")
                    continue

            logger.info(f"Successfully extracted {len(contacts)} contacts directly from search results")
            return contacts

        except Exception as e:
            logger.warning(f"Failed to extract contacts from search listings: {e}")
            return []

    async def _scrape_universal_search_results(self) -> Dict[str, Any]:
        """Scrape search results from any business directory - extract from all pages with pagination"""
        logger.info("📋 Detected search results page, extracting company data from all pages")

        all_contacts = []
        page_count = 0
        max_pages = 10  # Limit to prevent infinite loops

        while page_count < max_pages:
            page_count += 1
            logger.info(f"🔄 Processing page {page_count}")

            # First try to extract contact info directly from search results listings on current page
            direct_extraction = await self._extract_contacts_from_search_listings()
            if direct_extraction and len(direct_extraction) > 0:
                all_contacts.extend(direct_extraction)
                logger.info(f"✅ Extracted {len(direct_extraction)} leads directly from page {page_count}")

            # Also try the fallback approach for company URLs on current page
            company_urls = await self._extract_universal_company_urls()
            if company_urls:
                logger.info(f"📋 Found {len(company_urls)} company URLs on page {page_count}")

                # Limit companies per page to avoid overwhelming
                max_companies_per_page = min(len(company_urls), 5)

                for i in range(max_companies_per_page):
                    company_url = company_urls[i]
                    logger.info(f"Visiting company page {i + 1}/{max_companies_per_page} on page {page_count}: {company_url}")

                    try:
                        # Navigate to company page
                        await self.page.goto(company_url, wait_until="domcontentloaded")
                        await asyncio.sleep(random.uniform(2, 4))

                        # Extract contact data using AI
                        contact_data = await self._extract_universal_company_contact()
                        if contact_data:
                            all_contacts.append(contact_data)
                            logger.info(f"✅ Extracted contact data from company page")
                        else:
                            logger.debug(f"❌ No contact data found on company page: {company_url}")

                        # Go back to search results page
                        await self.page.go_back()
                        await asyncio.sleep(random.uniform(1, 3))

                        # Respectful delay
                        if i < max_companies_per_page - 1:
                            await asyncio.sleep(random.uniform(2, 4))

                    except Exception as e:
                        logger.warning(f"Failed to process company page {company_url}: {e}")
                        # Try to go back to search results
                        try:
                            await self.page.go_back()
                            await asyncio.sleep(random.uniform(1, 2))
                        except:
                            pass
                        continue

            # Look for next page link
            next_page_found = await self._navigate_to_next_page()

            if not next_page_found:
                logger.info(f"📄 No more pages found after page {page_count}")
                break

            # Wait for next page to load
            await asyncio.sleep(random.uniform(2, 4))

        logger.info(f"✅ Completed universal directory extraction: {len(all_contacts)} total contacts from {page_count} pages")
        return {
            "page_type": "search_results_paginated",
            "extracted_data": all_contacts,
            "total_companies": len(all_contacts),
            "pages_processed": page_count,
            "source": "universal_directory_paginated"
        }

    async def _extract_universal_company_urls(self) -> List[str]:
        """Extract company URLs using universal selectors"""
        try:
            # More comprehensive selectors for company profile pages
            selectors = [
                # ThomasNet specific
                'a[href*="/suppliers/"]',
                'a[href*="/profile/"]',
                'a[href*="/company/"]',
                'a[href*="supplier-profile"]',
                '.supplier-name a',
                '.company-name a',
                '.result-item a[href*="/suppliers/"]',
                # IndiaMart specific
                'a[href*="/proddetail/"]',
                'a[href*="/company/"]',
                'a[href*="company-profile"]',
                '.company-info a',
                '.supplier-link',
                # YellowPages specific
                'a[href*="/business/"]',
                '.business-name a',
                '.listing-title a',
                # Yelp specific
                'a[href*="/biz/"]',
                '.business-name a',
                # Crunchbase specific
                'a[href*="/organization/"]',
                # OpenCorporates specific
                'a[href*="/companies/"]',
                # General selectors for search results
                '.search-result a',
                '.result a',
                '.listing a',
                '.company-link',
                '.business-link',
                'h2 a[href]',
                'h3 a[href]',
                'h4 a[href]',
                '.result-title a',
                '.company-title a',
                '.supplier-title a',
                # LinkedIn company pages
                'a[href*="linkedin.com/company/"]',
                # Additional common patterns
                'a[href*="company-profile"]',
                'a[href*="business-profile"]',
                'a[href*="supplier-profile"]'
            ]

            all_urls = set()

            # First, try to get all links and filter them
            try:
                all_links = await self.page.query_selector_all('a[href]')
                logger.info(f"Found {len(all_links)} total links on page")

                for link in all_links:
                    try:
                        href = await link.get_attribute('href')
                        text = await link.inner_text()
                        text = text.strip().lower() if text else ""

                        if href and self._is_company_url(href):
                            # Additional filtering based on link text
                            if any(keyword in text for keyword in ['company', 'business', 'supplier', 'manufacturer', 'firm', 'corp', 'ltd', 'inc', 'llc', 'services', 'solutions', 'group', 'enterprises']):
                                if not href.startswith('http'):
                                    base_url = self.page.url.rstrip('/')
                                    if href.startswith('/'):
                                        href = f"{base_url}{href}"
                                    else:
                                        href = f"{base_url}/{href}"
                                all_urls.add(href)
                                logger.info(f"Added company URL: {href} (text: {text[:50]})")
                    except:
                        continue
            except Exception as e:
                logger.warning(f"Error extracting all links: {e}")

            # Then try specific selectors
            for selector in selectors:
                try:
                    links = await self.page.query_selector_all(selector)
                    for link in links[:20]:  # Limit per selector
                        href = await link.get_attribute('href')
                        if href and self._is_company_url(href):
                            if not href.startswith('http'):
                                base_url = self.page.url.rstrip('/')
                                if href.startswith('/'):
                                    href = f"{base_url}{href}"
                                else:
                                    href = f"{base_url}/{href}"
                            all_urls.add(href)
                            logger.info(f"Added company URL from selector {selector}: {href}")
                except:
                    continue

            # Also try to extract URLs from structured data
            try:
                structured_data = await self.page.evaluate("""
                    () => {
                        const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                        const data = [];
                        scripts.forEach(script => {
                            try {
                                const json = JSON.parse(script.textContent);
                                if (json['@type'] === 'Organization' || json.type === 'Organization') {
                                    data.push(json);
                                }
                            } catch (e) {}
                        });
                        return data;
                    }
                """)

                for item in structured_data:
                    if item.get('url') and self._is_company_url(item['url']):
                        all_urls.add(item['url'])
                    if item.get('sameAs'):
                        same_as = item['sameAs']
                        if isinstance(same_as, list):
                            for url in same_as:
                                if url and self._is_company_url(url):
                                    all_urls.add(url)
                        elif isinstance(same_as, str) and self._is_company_url(same_as):
                            all_urls.add(same_as)
            except:
                pass

            # Filter out duplicates and invalid URLs
            valid_urls = []
            for url in all_urls:
                if url and len(url) > 10 and not url.endswith('.pdf') and not url.endswith('.jpg') and not url.endswith('.png'):
                    # Avoid internal pages
                    if not any(skip in url.lower() for skip in ['login', 'register', 'about', 'contact', 'privacy', 'terms', 'help', 'search', 'category']):
                        valid_urls.append(url)

            logger.info(f"Extracted {len(valid_urls)} valid company URLs from search results")
            return valid_urls[:15]  # Limit to 15 URLs

            urls = list(all_urls)[:15]  # Limit to 15 URLs
            logger.info(f"Extracted {len(urls)} company URLs from universal search results")
            for url in urls[:5]:  # Log first 5 for debugging
                logger.debug(f"Sample URL: {url}")
            return urls

        except Exception as e:
            logger.warning(f"Failed to extract universal company URLs: {e}")
            return []

    def _is_company_url(self, url: str) -> bool:
        """Check if URL looks like a company profile page"""
        url_lower = url.lower()

        # Company profile indicators
        company_keywords = ['company', 'profile', 'business', 'supplier', 'manufacturer', 'firm', 'corp', 'ltd', 'inc', 'llc']

        # Avoid platform internal pages, search, categories, marketing pages
        avoid_keywords = [
            'search', 'category', 'tag', 'page', 'sort', 'filter', 'login', 'register',
            'business.thomasnet.com', 'thomasnet.com/ads', 'thomasnet.com/news',
            'about', 'contact', 'privacy', 'terms', 'help', 'support', 'blog',
            'marketing', 'advertising', 'seo', 'analytics', 'sponsorship',
            'article', 'news', 'story', 'press', 'media', 'event', 'webinar'
        ]

        # For ThomasNet specifically - only allow actual supplier profile URLs
        if 'thomasnet.com' in url_lower:
            # Must contain supplier profile patterns AND not be search/category pages
            if ('/suppliers/' in url_lower or '/profile/' in url_lower or '/company/' in url_lower) and not any(avoid in url_lower for avoid in avoid_keywords):
                return True
            # Avoid ThomasNet's own business/marketing pages and search results
            if any(avoid in url_lower for avoid in ['business.thomasnet.com', 'thomasnet.com/ads', 'thomasnet.com/news', 'thomasnet.com/search.html', 'thomasnet.com/articles', 'thomasnet.com/companyhistory']):
                return False
            # Allow LinkedIn company pages
            if 'linkedin.com/company/' in url_lower:
                return True

        # For IndiaMart specifically - allow product detail pages
        if 'indiamart.com' in url_lower:
            if '/proddetail/' in url_lower and not any(avoid in url_lower for avoid in avoid_keywords):
                return True
            # Allow LinkedIn company pages
            if 'linkedin.com/company/' in url_lower:
                return True

        # For YellowPages specifically
        if 'yellowpages.com' in url_lower:
            if '/business/' in url_lower and not any(avoid in url_lower for avoid in avoid_keywords):
                return True

        # For Yelp specifically
        if 'yelp.com' in url_lower:
            if '/biz/' in url_lower and not any(avoid in url_lower for avoid in avoid_keywords):
                return True

        # For Crunchbase specifically
        if 'crunchbase.com' in url_lower:
            if '/organization/' in url_lower and not any(avoid in url_lower for avoid in avoid_keywords):
                return True

        # For OpenCorporates specifically
        if 'opencorporates.com' in url_lower:
            if '/companies/' in url_lower and not any(avoid in url_lower for avoid in avoid_keywords):
                return True

        # General validation - has company keywords and no avoid keywords
        has_company_keyword = any(keyword in url_lower for keyword in company_keywords)
        has_avoid_keyword = any(keyword in url_lower for keyword in avoid_keywords)

        return has_company_keyword and not has_avoid_keyword

    async def _navigate_to_next_page(self) -> bool:
        """Navigate to the next page of search results. Returns True if successful, False if no more pages."""
        try:
            logger.info("🔍 Looking for next page link...")

            # Common pagination selectors and patterns
            pagination_selectors = [
                # Text-based next buttons
                'a[aria-label*="next" i]',
                'a[title*="next" i]',
                'a:has-text("next")',
                'a:has-text("Next")',
                'a:has-text("NEXT")',
                'a:has-text(">")',
                'a:has-text("»")',
                'a:has-text("›")',

                # Class/ID based selectors
                '.pagination a.next',
                '.pagination .next',
                '.pager a.next',
                '.pager .next',
                'a.next',
                '.next a',
                'a[rel="next"]',
                'link[rel="next"]',

                # Specific website patterns
                '.pagination-next',
                '.pagination__next',
                '.page-next',
                '.next-page',
                'a[data-page*="next"]',

                # Numbered pagination - look for current page + 1
                '.pagination a.active + a',  # Next after active
                '.pager a.active + a',

                # Button-based pagination
                'button[aria-label*="next" i]',
                'button:has-text("next")',
                'button:has-text("Next")',
                'button:has-text(">")',
                'button:has-text("»")',
            ]

            # Try each selector
            for selector in pagination_selectors:
                try:
                    # Look for the element
                    next_link = await self.page.query_selector(selector)
                    if next_link:
                        # Check if it's visible and enabled
                        is_visible = await next_link.is_visible()
                        if not is_visible:
                            continue

                        # Check if it's disabled (some sites disable next when no more pages)
                        disabled = await next_link.get_attribute('disabled')
                        if disabled:
                            logger.info("Next page link is disabled - no more pages")
                            return False

                        # Check for disabled class
                        class_attr = await next_link.get_attribute('class')
                        if class_attr and ('disabled' in class_attr.lower() or 'inactive' in class_attr.lower()):
                            logger.info("Next page link is disabled/inactive - no more pages")
                            return False

                        logger.info(f"Found next page link with selector: {selector}")

                        # Click the next page link
                        await next_link.click()

                        # Wait for navigation or content change
                        await self.page.wait_for_load_state('domcontentloaded')
                        await asyncio.sleep(random.uniform(2, 4))

                        # Verify we actually moved to next page
                        current_url = self.page.url
                        logger.info(f"Navigated to next page: {current_url}")

                        return True

                except Exception as e:
                    logger.debug(f"Selector {selector} failed: {e}")
                    continue

            # Try JavaScript-based pagination (some sites use onclick handlers)
            try:
                # Look for elements with onclick containing pagination logic
                next_elements = await self.page.query_selector_all('[onclick*="page"], [onclick*="next"], [onclick*="pagination"]')
                for element in next_elements:
                    try:
                        onclick_attr = await element.get_attribute('onclick')
                        if onclick_attr and ('next' in onclick_attr.lower() or 'page' in onclick_attr.lower()):
                            is_visible = await element.is_visible()
                            if is_visible:
                                logger.info("Found JavaScript-based next page element")
                                await element.click()
                                await self.page.wait_for_load_state('domcontentloaded')
                                await asyncio.sleep(random.uniform(2, 4))
                                return True
                    except:
                        continue
            except Exception as e:
                logger.debug(f"JavaScript pagination check failed: {e}")

            # Check for "Load More" buttons (infinite scroll alternative)
            try:
                load_more_selectors = [
                    'button:has-text("load more")',
                    'button:has-text("Load More")',
                    'button:has-text("show more")',
                    'button:has-text("Show More")',
                    'a:has-text("load more")',
                    'a:has-text("Load More")',
                    '.load-more',
                    '.show-more',
                    '#load-more'
                ]

                for selector in load_more_selectors:
                    load_more_btn = await self.page.query_selector(selector)
                    if load_more_btn:
                        is_visible = await load_more_btn.is_visible()
                        if is_visible:
                            logger.info("Found 'Load More' button - clicking to load additional results")
                            await load_more_btn.click()
                            await asyncio.sleep(random.uniform(3, 5))  # Wait for content to load
                            return True
            except Exception as e:
                logger.debug(f"Load more button check failed: {e}")

            logger.info("❌ No next page link found")
            return False

        except Exception as e:
            logger.warning(f"Error navigating to next page: {e}")
            return False

    async def _extract_universal_company_contact(self) -> Optional[Dict[str, Any]]:
        """Extract contact information using AI from any company page with ICP data"""
        try:
            # Get page content
            html_content = await self.page.content()

            # Get ICP data
            icp_data = self.get_hardcoded_icp()

            # Prepare input for AI extraction with both page content and ICP data
            ai_input_data = {
                "sections": [
                    {"section": {"text": html_content, "tag": "body"}, "priority_score": 1.0},
                    {"section": {"text": json.dumps(icp_data), "tag": "icp"}, "priority_score": 0.8}
                ],
                "structured_data": [icp_data]
            }

            # Extract using AI
            extracted_data = extract_client_info_from_sections(ai_input_data, self.page.url)

            if extracted_data and extracted_data.get("leads") and extracted_data["leads"]:
                # Take the first lead and validate/clean phone numbers
                lead_data = extracted_data["leads"][0]
                
                # Validate and clean phone numbers from AI extraction
                if lead_data.get("contact_info", {}).get("phone"):
                    phone = lead_data["contact_info"]["phone"]
                    if not is_valid_phone_number(phone):
                        logger.warning(f"AI extracted invalid phone number: {phone} - removing")
                        lead_data["contact_info"]["phone"] = None
                
                enhanced_result = await enhance_contact_details(self.page, lead_data)
                return enhanced_result

            # Check if page scraping is needed
            if extracted_data and extracted_data.get("needs_page_scraping"):
                logger.info(f"AI and structured data failed for {self.page.url}, trying comprehensive page scraping")
                fallback_contact = await self._extract_contact_from_page_scraping()
                if fallback_contact:
                    logger.info(f"Comprehensive page scraping successful for {self.page.url}")
                    return fallback_contact

        except Exception as e:
            logger.warning(f"Failed to extract universal company contact: {e}")

        return None

    async def _extract_contact_from_page_scraping(self) -> Optional[Dict[str, Any]]:
        """Comprehensive page scraping fallback for contact extraction"""
        try:
            # Get page content
            page_content = await self.page.content()
            soup = BeautifulSoup(page_content, 'html.parser')

            # Extract all text content
            text_content = soup.get_text(separator=' ', strip=True)

            # Extract contact information using multiple methods
            contact_info = {
                "name": None,
                "email": None,
                "phone": None,
                "organization": None,
                "role": "Company/Organization",
                "confidence": 0.7,
                "source": f"page_scraping_{self.page.url}",
                "notes": "",
                "lead_category": "B2B",
                "lead_sub_category": "",
                "bio": "",
                "industry": None,
                "company_type": None,
                "location": None,
                "employee_count": None,
                "revenue": None,
                "websites": [],
                "social_media": {
                    "linkedin": None,
                    "twitter": None,
                    "facebook": None,
                    "instagram": None,
                    "youtube": None,
                    "other": []
                },
                "address": None,
                "status": "active",
                "incorporation_date": None,
                "jurisdiction": None
            }

            # Extract company name from title or headings
            title = soup.find('title')
            if title:
                title_text = title.get_text(strip=True)
                # Remove common suffixes
                title_text = re.sub(r'\s*-\s*ThomasNet|\s*-\s*IndiaMart|\s*-\s*Yellow Pages|\s*-\s*Yelp|\s*\|.*$', '', title_text)
                if len(title_text) > 3 and not any(skip in title_text.lower() for skip in ['search', 'login', 'register', 'directory']):
                    contact_info["organization"] = title_text.strip()

            # Extract emails from page using multiple methods
            all_emails = extract_emails({"notes": text_content})
            if not all_emails:
                # Try more specific patterns for business directories
                email_selectors = [
                    '.email', '.contact-email', '.mail',
                    '[class*="email"]', '[class*="contact"]',
                    'a[href^="mailto:"]', 'a[href*="email"]'
                ]
                for selector in email_selectors:
                    email_elem = soup.select_one(selector)
                    if email_elem:
                        if email_elem.get('href', '').startswith('mailto:'):
                            email = email_elem['href'].replace('mailto:', '').strip()
                        else:
                            email = email_elem.get_text(strip=True)
                        if email and '@' in email and '.' in email:
                            all_emails.append(email)
                            break

            if all_emails:
                # Filter out generic emails
                valid_emails = [e for e in all_emails if not any(generic in e.lower() for generic in ['noreply', 'no-reply', 'info@', 'contact@', 'sales@', 'support@'])]
                if valid_emails:
                    contact_info["email"] = valid_emails[0] if len(valid_emails) == 1 else valid_emails

            # Extract phones from page using multiple methods
            all_phones = extract_phones({"notes": text_content})
            if not all_phones:
                # Try more specific patterns for business directories
                phone_selectors = [
                    '.phone', '.contact-phone', '.telephone', '.tel',
                    '[class*="phone"]', '[class*="contact"]', '[class*="tel"]',
                    'a[href^="tel:"]'
                ]
                for selector in phone_selectors:
                    phone_elem = soup.select_one(selector)
                    if phone_elem:
                        if phone_elem.get('href', '').startswith('tel:'):
                            phone = phone_elem['href'].replace('tel:', '').strip()
                        else:
                            phone = phone_elem.get_text(strip=True)
                        if phone:
                            # Clean phone number
                            phone = re.sub(r'[^\d\+\-\(\)\s]', '', phone).strip()
                            if len(phone) >= 7:  # Minimum length for a valid phone
                                all_phones.append(phone)
                                break

            if all_phones:
                contact_info["phone"] = all_phones[0] if len(all_phones) == 1 else all_phones

            # Extract address information
            address_selectors = [
                '.address', '.location', '.contact-address',
                '[class*="address"]', '[class*="location"]'
            ]
            for selector in address_selectors:
                addr_elem = soup.select_one(selector)
                if addr_elem:
                    address_text = addr_elem.get_text(strip=True)
                    if address_text and len(address_text) > 10:  # Minimum address length
                        contact_info["address"] = address_text
                        break

            # Extract bio/description
            bio_selectors = [
                '.description', '.about', '.company-description',
                '[class*="description"]', '[class*="about"]',
                'meta[name="description"]'
            ]
            for selector in bio_selectors:
                bio_elem = soup.select_one(selector)
                if bio_elem:
                    if selector == 'meta[name="description"]':
                        bio_text = bio_elem.get('content', '')
                    else:
                        bio_text = bio_elem.get_text(strip=True)
                    if bio_text and len(bio_text) > 20:  # Minimum bio length
                        contact_info["bio"] = bio_text
                        break

            # Extract website
            website_selectors = [
                'a[href^="http"]', '.website', '.url',
                '[class*="website"]', '[class*="url"]'
            ]
            for selector in website_selectors:
                site_elem = soup.select_one(selector)
                if site_elem:
                    href = site_elem.get('href')
                    if href and 'http' in href and not any(domain in href for domain in ['thomasnet.com', 'indiamart.com', 'linkedin.com', 'facebook.com', 'twitter.com']):
                        contact_info["websites"].append(href)
                        break

            # Only return if we have meaningful data
            has_meaningful_data = (
                contact_info["organization"] or
                contact_info["email"] or
                contact_info["phone"] or
                contact_info["address"] or
                (contact_info["bio"] and len(contact_info["bio"]) > 50)
            )

            if has_meaningful_data:
                logger.info(f"Extracted meaningful contact data: org={contact_info['organization']}, email={contact_info['email']}, phone={contact_info['phone']}")
                return contact_info
            else:
                logger.info(f"No meaningful contact data found on page: {self.page.url}")
                return None

        except Exception as e:
            logger.warning(f"Failed to extract contact from page scraping: {e}")
            return None

    async def _scrape_universal_company_profile(self) -> Dict[str, Any]:
        """Scrape a single company profile page"""
        logger.info("🏢 Scraping single company profile")

        contact_data = await self._extract_universal_company_contact()

        return {
            "page_type": "company_profile",
            "extracted_data": [contact_data] if contact_data else [],
            "total_companies": 1 if contact_data else 0,
            "source": "universal_profile"
        }

    async def _scrape_generic(self) -> Dict[str, Any]:
        """Generic scraping for unknown page types"""
        logger.info("🔍 Performing generic scraping")

        # Try to extract any contact information
        contact_data = await self._extract_universal_company_contact()

        return {
            "page_type": "generic",
            "extracted_data": [contact_data] if contact_data else [],
            "total_companies": 1 if contact_data else 0,
            "source": "generic_scraping"
        }

    async def _scrape_company_profile(self) -> Dict[str, Any]:
        """Single company profile scraping"""
        return await self._scrape_universal_company_profile()

    async def perform_search_on_directory(self, search_term: str) -> Dict[str, Any]:
        """Navigate to directory homepage and perform search for the given term"""
        try:
            # Initialize browser if not already done
            if not self.browser:
                await self._initialize_browser()

            # Navigate to directory homepage
            if not await self._navigate_to_url(self.url):
                return {"error": "Failed to navigate to directory homepage", "extracted_data": []}

            # Wait for page to load
            await asyncio.sleep(2)

            # Try to find and fill search form
            search_performed = await self._perform_search_on_page(search_term)
            
            if not search_performed:
                logger.warning(f"Could not perform search on {self.url}, trying direct navigation")
                # Try constructing search URL manually
                search_url = self._construct_search_url(search_term)
                if search_url and search_url != self.url:
                    await self.page.goto(search_url, wait_until="domcontentloaded")
                    await asyncio.sleep(2)

            # Now scrape the results (we should be on search results page)
            return await self.scrape_any_website()

       

        except Exception as e:
            logger.error(f"Search failed on directory {self.url}: {e}")
            return {"error": str(e), "extracted_data": []}

    async def _perform_search_on_page(self, search_term: str) -> bool:
        """Try to find search input and perform search on current page"""
        try:
            # Common search input selectors - expanded list
            search_selectors = [
                'input[type="search"]',
                'input[name="q"]',
                'input[name="query"]',
                'input[name="search"]',
                'input[name="keyword"]',
                'input[name="term"]',
                'input[name="find"]',
                'input[placeholder*="search" i]',
                'input[placeholder*="find" i]',
                'input[placeholder*="company" i]',
                'input[placeholder*="business" i]',
                'input[placeholder*="supplier" i]',
                '.search-input',
                '#search-input',
                '.search-box input',
                '.search-form input',
                'input.search-field',
                'input.query',
                'input.search-query',
                'input[name="search_terms"]',
                'input[name="ss"]',
                'input[name="find_desc"]'
            ]

            search_input = None
            for selector in search_selectors:
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        # Check if it's actually an input element
                        tag_name = await element.evaluate("el => el.tagName.toLowerCase()")
                        if tag_name == 'input':
                            # Check if it's visible
                            is_visible = await element.is_visible()
                            if is_visible:
                                search_input = element
                                logger.info(f"Found search input with selector: {selector}")
                                break
                except:
                    continue

            if not search_input:
                logger.warning("No search input found on page")
                return False

            # Clear and fill search input
            await search_input.click()  # Focus first
            await search_input.fill("")  # Clear by filling empty string
            await search_input.fill(search_term)
            await asyncio.sleep(1)

            # Find and click search button - expanded list
            button_selectors = [
                'button[type="submit"]',
                'input[type="submit"]',
                '.search-button',
                '#search-button',
                'button.search',
                'button[aria-label*="search" i]',
                'button:contains("Search")',
                'button:contains("Find")',
                'button:contains("Go")',
                'input[value*="search" i]',
                'input[value*="find" i]',
                '.search-submit',
                '#search-submit',
                'button[name="search"]',
                'button[name="submit"]'
            ]

            search_button = None
            for selector in button_selectors:
                try:
                    if ':contains' in selector:
                        # Handle text-based selectors
                        text = selector.split('"')[1]
                        search_button = await self.page.query_selector(f'button:has-text("{text}")')
                    else:
                        search_button = await self.page.query_selector(selector)
                    if search_button:
                        is_visible = await search_button.is_visible()
                        if is_visible:
                            logger.info(f"Found search button with selector: {selector}")
                            break
                except:
                    continue

            if search_button:
                await search_button.click()
            else:
                # Try pressing Enter in the search input
                logger.info("No search button found, trying Enter key")
                await search_input.press('Enter')

            # Wait for search results to load - increased wait time
            await asyncio.sleep(8)
            
            # Check if we got redirected or if the page changed
            current_url_after = self.page.url
            logger.info(f"URL after search: {current_url_after}")
            
            # Check if search results loaded
            content = await self.page.content()
            if 'result' in content.lower() or 'found' in content.lower() or 'companies' in content.lower():
                logger.info(f"Successfully performed search for '{search_term}' on {self.url}")
                return True
            else:
                logger.warning(f"Search may not have worked - no results indicators found")
                return False

        except Exception as e:
            logger.warning(f"Failed to perform search on page: {e}")
            return False

    def _construct_search_url(self, search_term: str) -> Optional[str]:
        """Try to construct a search URL for known directories"""
        base_url = self.url.rstrip('/')
        term_encoded = search_term.replace(' ', '+')

        # Known directory search URL patterns
        if 'thomasnet.com' in base_url:
            return f"{base_url}/search.html?q={term_encoded}"
        elif 'indiamart.com' in base_url:
            return f"{base_url}/search.html?keyword={term_encoded}"
        elif 'yellowpages.com' in base_url:
            return f"{base_url}/search?search_terms={term_encoded}"
        elif 'yelp.com' in base_url:
            return f"{base_url}/search?find_desc={term_encoded}"
        elif 'crunchbase.com' in base_url:
            return f"{base_url}/search/organizations?query={term_encoded}"
        elif 'opencorporates.com' in base_url:
            return f"{base_url}/companies?q={term_encoded}"

        return None

async def main():
    """Main execution function - Now uses Google search to find directory websites"""
    logger.info("Starting Universal Scraper execution...")

    # Dynamic configuration - uses Google search to find directories
    print("🌍 Universal Scraper - Uses Google to find and scrape business directories!")
    print("Usage: python universal_scraper_complete.py [SERVICE]")
    print("Example: python universal_scraper_complete.py 'ac services'")
    print("Example: python universal_scraper_complete.py 'software development'")
    print()

    # Get service from command line arguments - REQUIRED
    import sys
    if len(sys.argv) < 2:
        print("❌ Error: Service name is required!")
        print("Usage: python universal_scraper_complete.py [SERVICE]")
        print("Example: python universal_scraper_complete.py 'ac services'")
        sys.exit(1)

    service_name = sys.argv[1]

    print(f"🎯 Searching for directories for service: {service_name}")
    print()

    # Use Google search to find directory websites
    directory_urls = await search_google_for_directories(service_name)

    if not directory_urls:
        print("❌ No directory websites found via Google search")
        sys.exit(1)

    print(f"📋 Found {len(directory_urls)} directory websites:")
    for i, url in enumerate(directory_urls, 1):
        print(f"  {i}. {url}")
    print()

    all_extracted_leads = []

    # Scrape each directory website
    for i, directory_url in enumerate(directory_urls, 1):
        print(f"🔍 Scraping directory {i}/{len(directory_urls)}: {directory_url}")
        print("-" * 60)

        try:
            # Create scraper for this directory
            scraper = UniversalScraper(url=directory_url)

            # Perform search on this directory and scrape results
            async with scraper:
                results = await scraper.perform_search_on_directory(service_name)

            extracted_leads = results.get("extracted_data", [])
            all_extracted_leads.extend(extracted_leads)

            print(f"✅ Extracted {len(extracted_leads)} leads from {directory_url}")
            print(f"📋 Page type: {results.get('page_type', 'unknown')}")
            print()

            # Respectful delay between directories
            if i < len(directory_urls):
                await asyncio.sleep(random.uniform(5, 10))

        except Exception as e:
            print(f"❌ Failed to scrape {directory_url}: {e}")
            continue

    # Save all results
    if all_extracted_leads:
        output_filename = f"leads_{service_name.replace(' ', '_').lower()}_google_search.json"
        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(all_extracted_leads, f, ensure_ascii=False, indent=4)

        print(f"🎉 Total leads extracted: {len(all_extracted_leads)}")
        print(f"💾 Results saved to {output_filename}")
    else:
        print("❌ No leads extracted from any directory")

async def search_google_for_directories(service_name: str) -> List[str]:
    """Use Google search to find business directory websites for a service"""
    try:
        # Initialize browser for Google search
        playwright = await async_playwright().start()
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = await context.new_page()

        # Search query for business directories - expanded to find more directories
        search_query = f'{service_name} directory OR companies OR businesses OR suppliers OR manufacturers site:.com OR site:.net OR site:.org OR site:.in OR site:.co.uk'
        google_url = f"https://www.google.com/search?q={search_query}&num=30"

        print(f"🔍 Searching Google: {search_query}")

        await page.goto(google_url, wait_until="domcontentloaded")
        await asyncio.sleep(3)

        # Extract search results
        directory_urls = []

        # Expanded list of known business directory domains
        known_directories = [
            'thomasnet.com', 'indiamart.com', 'kompass.com', 'yellowpages.com',
            'crunchbase.com', 'linkedin.com', 'opencorporates.com', 'bizdir.com',
            'businessdirectory.com', 'local.com', 'yelp.com', 'bbb.org',
            'manta.com', 'dexknows.com', 'superpages.com', 'angieslist.com',
            'houzz.com', 'thumbtack.com', 'homeadvisor.com', 'angi.com',
            'cylex.net', 'tuugo.us', 'hotfrog.com', 'brownbook.net',
            'citysearch.com', 'insiderpages.com', 'showmelocal.com',
            'getthedata.co', 'companycheck.co.uk', 'duedil.com', 'thesunbusinessdirectory.com',
            'yell.com', 'touchlocal.com', 'cylex-uk.co.uk', 'ukindex.co.uk',
            'findopen.co.uk', 'thesun.co.uk/business-directory', 'scotsman.com/business-directory',
            'telegraph.co.uk/business-directory', 'independent.co.uk/business-directory'
        ]

        # Get all search result links
        links = await page.query_selector_all('a[href*="http"]')
        print(f"Found {len(links)} total links on Google results page")

        for link in links[:30]:  # Check first 30 links
            try:
                href = await link.get_attribute('href')
                print(f"Checking link: {href}")
                if href and 'google.com' not in href and 'youtube.com' not in href:
                    # Extract actual URL from Google redirect
                    if 'url?q=' in href:
                        start = href.find('url?q=') + 6
                        end = href.find('&', start)
                        if end == -1:
                            end = len(href)
                        actual_url = href[start:end]
                        actual_url = actual_url.replace('%3A', ':').replace('%2F', '/')
                    else:
                        actual_url = href

                    # Check if it's a known directory or looks like a business directory
                    from urllib.parse import urlparse
                    domain = urlparse(actual_url).netloc.replace('www.', '')

                    # Skip non-business sites
                    skip_domains = ['facebook.com', 'twitter.com', 'instagram.com', 'youtube.com',
                                  'wikipedia.org', 'amazon.com', 'ebay.com', 'craigslist.org',
                                  'reddit.com', 'pinterest.com', 'tiktok.com', 'snapchat.com']

                    if any(skip in domain for skip in skip_domains):
                        continue

                    # Include known directories or sites that look like directories
                    if (domain in known_directories or
                        any(keyword in actual_url.lower() for keyword in ['directory', 'companies', 'business', 'suppliers', 'manufacturers', 'search', 'yellowpages', 'thomasnet', 'indiamart', 'kompass', 'crunchbase', 'opencorporates', 'yelp', 'bbb', 'manta', 'dexknows', 'superpages', 'angieslist', 'houzz', 'thumbtack', 'homeadvisor', 'angi', 'cylex', 'tuugo', 'hotfrog', 'brownbook', 'citysearch', 'insiderpages', 'showmelocal'])):
                        if actual_url not in directory_urls:
                            directory_urls.append(actual_url)
                            print(f"Added directory: {actual_url}")

            except:
                continue

        await browser.close()

        # If no directories found, use some known directory homepages
        if not directory_urls:
            print("No directories found from Google, using known directory homepages...")
            fallback_directories = [
                "https://www.thomasnet.com/",
                "https://www.indiamart.com/",
                "https://www.yellowpages.com/",
                "https://www.yelp.com/",
                "https://www.crunchbase.com/",
                "https://opencorporates.com/",
                "https://www.kompass.com/",
                "https://www.manta.com/",
                "https://www.dexknows.com/",
                "https://www.superpages.com/"
            ]
            directory_urls = fallback_directories[:10]  # Use first 10

        # Limit to top 10 directories to avoid overwhelming
        return directory_urls[:10]

    except Exception as e:
        logger.error(f"Google search failed: {e}")
        return []

if __name__ == "__main__":
    asyncio.run(main())