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


def extract_client_info_from_sections(ai_input_data: Dict[str, Any], url: str = "") -> Dict[str, Any]:
    """Extract client information from filtered sections and structured data using Gemini AI LLM."""
    
    # Handle both old format (List) and new format (Dict)
    filtered_sections = ai_input_data.get("sections")
    structured_data = ai_input_data.get("structured_data")
    
    logger.info(f"Processing sections and structured data items for {url}")
    
    if not filtered_sections and not structured_data:
        logger.warning(f"No sections or structured data found for {url}")
        return {
            "contacts": [],
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

    prompt = f"""You are an information identifier assistant. This is for study purpose. Analyze the following website data and organize any available person, business, contact details, and addresses into a structured JSON format.

Website: {url}

Content Sections:
{sections_text}

Structured Data:
{structured_text}

Identify:
- Business names, organizations and contact information
- Email addresses and phone numbers  
- Personnel details like names, job titles, and contact information
- Services industries, or locations mentioned
- Physical addresses, cities, regions, or location information

Guidelines:
- Capture any available names, roles, and contact details (including authors, contributors, or organizations).
- Extract any address information, city names, regions, or location details mentioned in the content.
- If nothing is found, still return valid JSON with empty/null fields.
- Keep descriptions concise and factual.
- Focus only on information explicitly present in the content or structured data.

Return valid JSON format:
{{
  "contacts": [
    {{
      "name": "Person or organization name",
      "email": "email@example.com or null",
      "phone": "phone number or null", 
      "organization": "Company/brand name or null",
      "role": "Job title, author role, or null",
      "confidence": 0.8,
      "source": "section or data source",
      "notes": "context if available",
      "lead_category": "Lead Category",
      "lead_sub_category": "Lead Sub Category"
    }}
  ],
  "organization_info": {{
    "primary_name": "Main company/brand name or null",
    "industry": "Industry/sector if identifiable, otherwise null",
    "services": ["service1", "service2"],
    "location": "Address, city, or region if available, otherwise null",
    "organization_type": "Organization Type"
  }},
  "addresses": [
    {{
      "address": "Full address or location description",
      "type": "street|city|region|country",
      "confidence": 0.8,
      "source": "section or data source",
      "notes": "context if available"
    }}
  ],
  "overall_confidence": 0.7,
  "summary": "Short summary of extracted details"
}}"""
    
    ai_result = _ai_generate_json(prompt)
    if ai_result:
        logger.info(f"AI extraction successful for {url} - found {len(ai_result.get('contacts', []))} potential clients")
        return ai_result
    else:
        logger.warning(f"AI extraction failed for {url}")
        # Return a more informative fallback
        return {
            "contacts": [],
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

    filtered_ai_lead_info = [{'section': {'tag': 'p', 'text': 'It also just has way more going on, even bars staying open much longer than in Toronto. In the 6ix, the last call at a bar on the weekend is typically around 1:45 a.m., as they have to stop serving alcohol at 2 a.m.', 'class': '', 'id': '', 'parent_tag': 'div'}, 'confidence': 0.4, 'contact_info': {'phones': [], 'emails': [], 'addresses': [{'value': '45 a.m., as they have to stop serving alcohol at', 'confidence': 0.6, 'type': 'street', 'source': 'text_pattern'}], 'websites_social': {'websites': [], 'social_media': []}}, 'keyword_matches': 0, 'priority_score': 0.6000000000000001}, {'section': {'tag': 'p', 'text': 'In NYC, I left a bar at 2:30 a.m., and the place was jammed with people.', 'class': '', 'id': '', 'parent_tag': 'div'}, 'confidence': 0.4, 'contact_info': {'phones': [], 'emails': [], 'addresses': [{'value': '30 a.m., and the place was jammed with people', 'confidence': 0.6, 'type': 'street', 'source': 'text_pattern'}], 'websites_social': {'websites': [], 'social_media': []}}, 'keyword_matches': 0, 'priority_score': 0.6000000000000001}]
    url = "https://www.narcity.com/i-recently-travelled-from-canada-to-us"
    structured_data = [{'@context': 'https://schema.org', '@type': 'BreadcrumbList', 'itemListElement': [{'@type': 'ListItem', 'item': {'@id': 'https://www.narcity.com', 'name': 'Canada'}, 'position': 1}, {'@type': 'ListItem', 'item': {'@id': 'https://www.narcity.com/travel', 'name': 'Travel'}, 'position': 2}, {'@type': 'ListItem', 'item': {'@id': 'https://www.narcity.com/tag/canada-us-travel', 'name': 'Canada Us Travel'}, 'position': 3}, {'@type': 'ListItem', 'item': {'@id': 'https://www.narcity.com/i-recently-travelled-from-canada-to-us', '@type': 'WebPage', 'name': 'I recently travelled from Canada to the US â€" Here are the 6 differences I noticed immediately'}, 'position': 3}]}, {'@context': 'https://schema.org', '@type': 'ItemList', 'itemListElement': [{'@type': 'ListItem', 'item': {'@type': 'Thing', 'description': 'Even Canada&#39;s biggest cities kind of seem low-key compared to American ones. I mean, I live in Toronto which is supposedly Canada&#39;s N.Y.C., and still found a visit to the Big Apple wild.It is absolutely gigantic and moves at lightning speed. Everyone there is in a rush to get where they need to be, whereas I generally find Toronto to be more laid back.There are many similarities between the two cities â€" NYC has Times Square, and Toronto has Sankofa Square (formerly Yonge-Dundas Square) and both have Flatiron Buildings â€" but the Big Apple&#39;s versions are far bigger.It also just has way more going on, even bars staying open much longer than in Toronto. In the 6ix, the last call at a bar on the weekend is typically around 1:45 a.m., as they have to stop serving alcohol at 2 a.m.In NYC, I left a bar at 2:30 a.m., and the place was jammed with people.', 'name': 'U.S. cities are built different\xa0'}, 'name': 'U.S. cities are built different\xa0', 'position': 1}, {'@type': 'ListItem', 'item': {'@type': 'Thing', 'description': 'I know that I&#39;ve complained in the past about Canadian tipping culture, but at least the process here is pretty straightforward.When you settle your bill here, the server presents you with the total bill and comes with a card machine. On the screen, there are percentages, such as 18%, 20%, or 22%. You select one, and you pay for it.When I paid for drinks in New York, the bartender handed me a receipt, on which I had to write down the tip I wanted to give. This meant I had to do the math! As someone who&#39;s not the best at math, I found it tough to know what to tip.With all its flaws, I prefer the Canadian way of doing things.', 'name': 'Tipping is way easier in Canada'}, 'name': 'Tipping is way easier in Canada', 'position': 2}, {'@type': 'ListItem', 'item': {'@type': 'Thing', 'description': 'When we crossed the border in Buffalo, our Google Maps automatically converted from the metric system to imperial. That was handy, but still our car is in km/h, not mp/h â€" making it super difficult to adjust our speed accordingly. For two countries so close, it&#39;s somewhat shocking that they follow a different system.', 'name': 'The\xa0metric system is superior'}, 'name': 'The\xa0metric system is superior', 'position': 3}, {'@type': 'ListItem', 'item': {'@type': 'Thing', 'description': 'While there are many Tim Hortons in the U.S., there just aren&#39;t as many as there are in Canada.Instead, there are so many Dunkin&#39; Donuts and Starbucks in the U.S. While there are lovely stores in their own right, you just can&#39;t beat Tim&#39;s.', 'name': 'There was a severe lack of Timmies'}, 'name': 'There was a severe lack of Timmies', 'position': 4}, {'@type': 'ListItem', 'item': {'@type': 'Thing', 'description': 'While eating out, I found the food portion sizes in the U.S. were bigger than in Canada. You get more bang for your buck, but I was surprised when the server put my plate down sometimes. Also I found that overall Canadian store groceries had generally better quality food. I noticed it especially with bacon from the store â€" Canadian bacon is superior.', 'name': 'U.S. portion sizes are massive'}, 'name': 'U.S. portion sizes are massive', 'position': 5}, {'@type': 'ListItem', 'item': {'@type': 'Thing', 'description': 'Americans love The Stars and Stripes.This was something I especially noticed driving for hours in upstate NY.The number of American flags in people&#39;s front yards or on their porches is not something you see too often in Canada with the Maple Leaf flag.Here, you might see a massive Canadian flag outside a government building or a shopping centre, but in the U.S., they are everywhere.', 'name': 'There are U.S. flags EVERYWHERE'}, 'name': 'There are U.S. flags EVERYWHERE', 'position': 6}], 'name': 'I recently travelled from Canada to the US â€" Here are the 6 differences I noticed immediately', 'url': 'https://www.narcity.com/i-recently-travelled-from-canada-to-us'}]
    ai_input_data = {
                        "sections": filtered_ai_lead_info,
                        "structured_data": structured_data
                    }
    ai_extracted_data = extract_client_info_from_sections(ai_input_data, url)  # Modified to accept dict

    # Phase 5: Store AI leads under "ai_leads" key
    if ai_extracted_data and ai_extracted_data.get("contacts"):
        lead_info["ai_leads"] = ai_extracted_data.get("contacts", [])
        logger.info(f"AI extracted {len(lead_info['ai_leads'])} potential client leads from {url}")
    else:
        lead_info["ai_leads"] = []