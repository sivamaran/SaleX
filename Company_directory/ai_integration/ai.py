from __future__ import annotations

import json
import os
import re
import time
from typing import Any, Dict, List, Optional

from loguru import logger
from dotenv import load_dotenv

# Add this right after the imports, before any other code
load_dotenv()

try:
    import google.generativeai as genai  # type: ignore
    _HAS_GEMINI = True
    logger.info("Google Generative AI imported successfully")
except ImportError as e:
    _HAS_GEMINI = False
    logger.error(f"Failed to import google.generativeai: {e}")
except Exception as e:
    _HAS_GEMINI = False
    logger.error(f"Unexpected error importing google.generativeai: {e}")

# More reasonable safeguards - increased limits for better functionality
_AI_DISABLED: bool = False
_AI_CALLS: int = 0
_AI_MAX_CALLS: int = int(os.environ.get("GEMINI_MAX_CALLS_PER_RUN", "5"))  # Increased from 3
_AI_COOLDOWN_SECONDS: float = float(os.environ.get("GEMINI_MIN_INTERVAL_SECONDS", "2.0"))  # Reduced from 2.0
_AI_LAST_CALL_TIME: float = 0.0


def _first_n_words(text: str, n: int = 500) -> str:
    words = re.findall(r"\S+", text)
    return " ".join(words[:n])


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
    global _AI_DISABLED
    if _AI_DISABLED or not _HAS_GEMINI:
        logger.warning(f"Gemini unavailable - Disabled: {_AI_DISABLED}, Has Gemini: {_HAS_GEMINI}")
        return False
    
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        logger.warning("GEMINI_API_KEY environment variable not set; AI features disabled")
        _AI_DISABLED = True
        return False
    
    # Validate API key format
    if not api_key.startswith(('AIza', 'ya29')):
        logger.warning(f"Invalid GEMINI_API_KEY format (should start with 'AIza' or 'ya29'): {api_key[:10]}...")
        _AI_DISABLED = True
        return False
    
    try:
        genai.configure(api_key=api_key)
        logger.info("Gemini configured successfully")
        return True
    except Exception as e:
        logger.error(f"Gemini configure failed with error: {type(e).__name__}: {e}")
        logger.error(f"API key length: {len(api_key)}, starts with: {api_key[:10]}...")
        _AI_DISABLED = True
        return False


def _respect_rate_limits() -> None:
    global _AI_LAST_CALL_TIME
    if _AI_COOLDOWN_SECONDS <= 0:
        return
    elapsed = time.time() - _AI_LAST_CALL_TIME
    if elapsed < _AI_COOLDOWN_SECONDS:
        time.sleep(_AI_COOLDOWN_SECONDS - elapsed)


def _should_call_ai() -> bool:
    if _AI_DISABLED:
        logger.warning("AI is disabled")
        return False
    if _AI_CALLS >= _AI_MAX_CALLS:
        logger.info(f"AI call budget exhausted for this run ({_AI_CALLS}/{_AI_MAX_CALLS}); skipping further calls")
        return False
    return True

def disambiguate_business_entities(url: str, html: str, cleaned_text: str) -> Dict[str, Any]:
    """Identify business entities; prefer AI, fallback to heuristics; if both fail, return {} so caller can skip."""
    prompt = f"""
Role: Business intelligence analyst
Task: Identify and disambiguate business entities
Context URL: {url}
Context (first 500 words):
{_first_n_words(cleaned_text, 500)}

Return ONLY valid JSON following this schema:
{{
    "entities": [
        {{"name": "string", "type": "company|person|org", "is_primary": true, "owns_contact": true, "confidence": 0.8, "notes": "string"}}
    ],
    "primary_entity": "string or null",
    "ambiguities": ["string"]
}}
"""
    data = _ai_generate_json(prompt)
    if data:
        return data
    # Heuristics fallback
    try:
        title_match = re.search(r"<title>(.*?)</title>", html, re.I | re.S)
        title = (title_match.group(1).strip() if title_match else "")
        name = re.split(r"[-|:\u2013\u2014]", title)[0].strip() or "Unknown"
        return {
            "entities": [
                {"name": name, "type": "company", "is_primary": True, "owns_contact": True, "confidence": 0.5, "notes": "heuristic-title"}
            ],
            "primary_entity": name,
            "ambiguities": [],
        }
    except Exception:
        return {}


def generate_extraction_strategy(html: str) -> Dict[str, Any]:
    """Ask AI for CSS/XPath suggestions; fallback to common selectors; if both fail, return {} to skip."""
    prompt = f"""
Analyze the following HTML snapshot and propose extraction strategies.
Return ONLY valid JSON with fields: selectors (map of area->list of CSS selectors), priority (list of areas by priority), notes (array of strings).
HTML snippet (truncated):
{html[:10000]}
"""
    data = _ai_generate_json(prompt)
    if data:
        return data
    # Fallback suggestions
    try:
        return {
            "selectors": {
                "contact": ["a[href^='mailto:']", "a[href*='contact']", "section#contact", "footer"],
                "phone": ["a[href^='tel:']", "*[class*='phone']", "*[class*='tel']"],
                "email": ["a[href^='mailto:']", "*[class*='email']"],
                "about": ["a[href*='about']", "section#about", "main"]
            },
            "priority": ["contact", "email", "phone", "about"],
            "notes": ["Heuristic fallback"]
        }
    except Exception:
        return {}


def validate_and_enhance(emails: List[str], phones: List[str]) -> Dict[str, Any]:
    """Validate formats and add confidence; uses libraries if present, else lightweight checks."""
    validated_emails: List[Dict[str, Any]] = []
    validated_phones: List[Dict[str, Any]] = []

    # Email validation
    try:
        from email_validator import validate_email, EmailNotValidError  # type: ignore
        for e in set(emails):
            try:
                _ = validate_email(e)
                validated_emails.append({"value": e, "valid": True, "reason": "email-validator"})
            except EmailNotValidError as ex:
                validated_emails.append({"value": e, "valid": False, "reason": str(ex)})
    except Exception:
        for e in set(emails):
            valid = bool(re.match(r"^[^@]+@[^@]+\.[^@]+$", e))
            validated_emails.append({"value": e, "valid": valid, "reason": "regex"})

    # Phone validation
    try:
        import phonenumbers  # type: ignore
        for p in set(phones):
            try:
                parsed = phonenumbers.parse(p, None)
                is_valid = phonenumbers.is_valid_number(parsed)
                validated_phones.append({"value": p, "valid": bool(is_valid), "reason": "phonenumbers"})
            except Exception as ex:
                validated_phones.append({"value": p, "valid": False, "reason": str(ex)})
    except Exception:
        for p in set(phones):
            digits = re.sub(r"\D", "", p)
            validated_phones.append({"value": p, "valid": len(digits) >= 7, "reason": "length>=7"})

    return {"emails": validated_emails, "phones": validated_phones}


def _handle_ai_error(err: Exception) -> None:
    """Detect quota/rate-limit errors and disable AI for the rest of the run."""
    global _AI_DISABLED
    message = str(err).lower()
    
    # More specific quota/rate limit detection
    quota_keywords = ["quota", "exceed", "429", "resourceexhausted"]
    rate_limit_keywords = ["rate limit", "too many requests"]
    
    if any(k in message for k in quota_keywords) or any(k in message for k in rate_limit_keywords):
        logger.warning(f"AI quota/rate limit error detected; disabling AI for this run: {err}")
        _AI_DISABLED = True
    else:
        logger.warning(f"AI call failed (not disabling): {err}")


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
            logger.warning("AI response blocked due to recitation")
            return None
            
        elif finish_reason == 4:  # OTHER
            logger.warning("AI response blocked for other reasons")
            return None
            
        else:
            logger.warning(f"Unknown finish reason: {finish_reason}")
            return None
        
    except Exception as e:
        _handle_ai_error(e)
        return None


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
        
        return None
        
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
        
        # Extract name
        if "name" in item:
            contact["name"] = item["name"]
            contact["organization"] = item["name"]
        
        # Extract description/bio
        if "description" in item:
            contact["bio"] = item["description"]
            contact["notes"] = item["description"][:200] + "..." if len(item["description"]) > 200 else item["description"]
        
        # Extract address/location
        if "address" in item:
            addr = item["address"]
            if isinstance(addr, dict):
                location_parts = []
                if "streetAddress" in addr:
                    location_parts.append(addr["streetAddress"])
                if "addressLocality" in addr:
                    location_parts.append(addr["addressLocality"])
                if "addressRegion" in addr:
                    location_parts.append(addr["addressRegion"])
                if "postalCode" in addr:
                    location_parts.append(addr["postalCode"])
                if "addressCountry" in addr:
                    location_parts.append(addr["addressCountry"])
                
                contact["location"] = ", ".join(location_parts)
                contact["address"] = contact["location"]
        
        # Extract contact info
        if "telephone" in item:
            contact["phone"] = item["telephone"]
        if "email" in item:
            contact["email"] = item["email"]
        
        # Extract website
        if "url" in item:
            contact["websites"] = [item["url"]]
        
        # Extract industry/type info
        if "@type" in item:
            item_type = item["@type"]
            if "Organization" in item_type:
                contact["company_type"] = "Organization"
            elif "Corporation" in item_type:
                contact["company_type"] = "Corporation"
            elif "Company" in item_type:
                contact["company_type"] = "Company"
        
        # Only return if we have at least a name
        if contact["name"]:
            return contact
        
        return None
        
    except Exception as e:
        logger.debug(f"Failed to extract from structured item: {e}")
        return None


def _extract_company_from_section(section: Dict, url: str) -> Optional[Dict]:
    """Extract company info from HTML section data."""
    try:
        section_data = section.get("section", {})
        text = section_data.get("text", "")
        
        # Look for company-like patterns in text
        # This is a basic implementation - could be enhanced with better patterns
        if len(text) < 10:  # Too short to be meaningful
            return None
            
        # Check if text contains company-like keywords
        company_keywords = ["ltd", "limited", "inc", "incorporated", "corp", "corporation", "llc", "company", "co."]
        has_company_indicator = any(keyword.lower() in text.lower() for keyword in company_keywords)
        
        if not has_company_indicator and len(text.split()) < 3:
            return None
        
        return {
            "name": text[:100] if len(text) > 100 else text,  # Use first part as name
            "email": None,
            "phone": None,
            "organization": text[:100] if len(text) > 100 else text,
            "role": "Company/Organization",
            "confidence": 0.4,
            "source": f"html_section_{url}",
            "notes": text,
            "lead_category": "B2B",
            "lead_sub_category": "",
            "bio": text,
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
        
    except Exception as e:
        logger.debug(f"Failed to extract from section: {e}")
        return None


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

    logger.debug(f"Structured data for {url}")

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
- For "type" field, classify as "lead" (potential customer) or "competitor" based on context
- For "what_we_can_offer" field, suggest relevant services/products we could provide
- For "link_details" field, provide a short description of what the link/page contains

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
    if fallback_result and fallback_result.get('leads'):
        logger.info(f"Fallback extraction successful for {url} - found {len(fallback_result.get('leads', []))} potential clients")
        return fallback_result
    
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

if __name__ == "__main__":
    pass