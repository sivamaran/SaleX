from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
import json

from loguru import logger

from web_scraper.extractors.json_ld_filter import JSONLDFilter, filter_jsonld


class ContactExtractor:
    """Extract and validate contact information with confidence scoring."""
    
    # Phone number patterns for different formats
    PHONE_PATTERNS = [
        r'\+\d{1,3}[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,9}',  # International
        r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',  # US format
        r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}',  # Simple US
        r'\d{10,15}',  # Raw digits
    ]
    
    # Email pattern
    EMAIL_PATTERN = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    
    # Address patterns
    ADDRESS_PATTERNS = [
        r'\b\d+[-\s]+[A-Za-z0-9\s,.-]{10,}(?:Street|St\.?|Avenue|Ave\.?|Road|Rd\.?|Boulevard|Blvd\.?|Lane|Ln\.?|Drive|Dr\.?|Court|Ct\.?|Place|Pl\.?)(?:\s*,?\s*[A-Za-z\s]+)?(?:\s*,?\s*\d{5}(?:-\d{4})?)?',
        r'P\.?O\.?\s*Box\s+\d+(?:\s*,?\s*[A-Za-z\s]+)?(?:\s*,?\s*\d{5}(?:-\d{4})?)?',
        r'\b\d+[A-Za-z]?\s+[A-Z][a-zA-Z\s]{5,30}(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln|Drive|Dr|Court|Ct|Place|Pl)\b'
    ]
    
    def extract_phones(self, text: str, html: str = "") -> List[Dict[str, Any]]:
        """Extract phone numbers with context and confidence scoring."""
        # Type safety checks
        if not isinstance(text, str):
            text = str(text) if text is not None else ""
        if not isinstance(html, str):
            html = str(html) if html is not None else ""
        
        phones = []
        seen = set()
        
        # Extract from tel: links first (highest confidence)
        tel_links = re.findall(r'href=["\']tel:([^"\']+)["\']', html, re.I)
        for tel in tel_links:
            clean_tel = re.sub(r'[^\d+]', '', tel)
            if clean_tel and clean_tel not in seen:
                phones.append({
                    "value": tel,
                    "clean_value": clean_tel,
                    "confidence": 0.95,
                    "source": "tel_link",
                    "context": "HTML tel: link"
                })
                seen.add(clean_tel)
        
        # Extract from text patterns
        for pattern in self.PHONE_PATTERNS:
            matches = re.finditer(pattern, text)
            for match in matches:
                phone = match.group().strip()
                clean_phone = re.sub(r'[^\d+]', '', phone)
                
                if not self._is_valid_phone(clean_phone) or self._is_duplicate_phone(clean_phone, seen):
                    continue
                
                # Skip numbers that are clearly not phone numbers
                if self._is_false_positive_phone(match, text):
                    continue

                # Context analysis for confidence
                context_start = max(0, match.start() - 50)
                context_end = min(len(text), match.end() + 50)
                context = text[context_start:context_end].lower()
                
                confidence = 0.7
                if any(word in context for word in ['phone', 'call', 'tel', 'contact']):
                    confidence += 0.15
                if any(word in context for word in ['mobile', 'cell', 'direct']):
                    confidence += 0.1
                
                phones.append({
                    "value": phone,
                    "clean_value": clean_phone,
                    "confidence": min(confidence, 1.0),
                    "source": "text_pattern",
                    "context": context.strip()
                })
                seen.add(clean_phone)
        
        return sorted(phones, key=lambda x: x["confidence"], reverse=True)
    
    def _is_valid_phone(self, phone: str) -> bool:
        """Enhanced phone validation."""
        if not phone or len(phone) < 7:
            return False
        
        # Remove country code prefix for length check
        digits_only = phone.lstrip('+').lstrip('0')
        if len(digits_only) < 7 or len(digits_only) > 15:
            return False
        
        # Check for obvious non-phone patterns
        if phone.count('0') > len(phone) * 0.7:  # Too many zeros
            return False
        if phone.count('1') > len(phone) * 0.7:  # Too many ones
            return False
        
        return True

    def _is_duplicate_phone(self, phone: str, seen: set) -> bool:
        """Check if phone is a duplicate or substring of existing phones."""
        for existing in seen:
            if phone in existing or existing in phone:
                if abs(len(phone) - len(existing)) <= 2:  # Similar length
                    return True
        return phone in seen

    def _is_false_positive_phone(self, match, text: str) -> bool:
        """Check if the matched number is likely not a phone number."""
        phone = match.group()
        context_start = max(0, match.start() - 100)
        context_end = min(len(text), match.end() + 100)
        context = text[context_start:context_end].lower()
        
        # Skip if in navigation/menu context
        nav_indicators = ['dashboard', 'sign in', 'home', 'menu', 'navigation', 'footer']
        if any(indicator in context for indicator in nav_indicators):
            return True
        
        # Skip if it's clearly a year, ID, or other non-phone number
        if re.match(r'^\d{4}$', phone) or re.match(r'^\d{8,}$', phone) and 'id' in context:
            return True
        
        return False

    def extract_emails(self, text: str, html: str = "") -> List[Dict[str, Any]]:
        """Extract email addresses with role classification and confidence."""
        # Type safety checks
        if not isinstance(text, str):
            text = str(text) if text is not None else ""
        if not isinstance(html, str):
            html = str(html) if html is not None else ""
        
        emails = []
        seen = set()
        
        # Extract from mailto: links first
        mailto_links = re.findall(r'href=["\']mailto:([^"\']+)["\']', html, re.I)
        for email in mailto_links:
            email = email.split('?')[0].strip()  # Remove query params
            if email and email not in seen and '@' in email:
                role = self._classify_email_role(email)
                emails.append({
                    "value": email,
                    "confidence": 0.95,
                    "role": role,
                    "source": "mailto_link",
                    "is_personal": role not in ['info', 'contact', 'sales', 'support']
                })
                seen.add(email)
        
        # Extract from text
        matches = re.finditer(self.EMAIL_PATTERN, text)
        for match in matches:
            email = match.group().lower().strip()
            if email in seen:
                continue
            
            # Context analysis
            context_start = max(0, match.start() - 30)
            context_end = min(len(text), match.end() + 30)
            context = text[context_start:context_end].lower()
            
            role = self._classify_email_role(email)
            confidence = 0.8
            
            # Boost confidence based on context
            if any(word in context for word in ['email', 'contact', 'reach']):
                confidence += 0.1
            
            emails.append({
                "value": email,
                "confidence": min(confidence, 1.0),
                "role": role,
                "source": "text_pattern",
                "is_personal": role not in ['info', 'contact', 'sales', 'support'],
                "context": context.strip()
            })
            seen.add(email)
        
        return sorted(emails, key=lambda x: x["confidence"], reverse=True)
    
    def _classify_email_role(self, email: str) -> str:
        """Classify email by role based on local part."""
        local_part = email.split('@')[0].lower()
        
        role_patterns = {
            'info': ['info', 'information'],
            'contact': ['contact', 'hello', 'hi'],
            'sales': ['sales', 'business', 'inquiries', 'quote'],
            'support': ['support', 'help', 'service'],
            'admin': ['admin', 'administrator'],
            'marketing': ['marketing', 'promo', 'newsletter'],
            'personal': []  # Default for names
        }
        
        for role, patterns in role_patterns.items():
            if any(pattern in local_part for pattern in patterns):
                return role
        
        return 'personal'
    
    def extract_addresses(self, text: str) -> List[Dict[str, Any]]:
        """Extract physical addresses with validation."""
        if not isinstance(text, str):
            text = str(text) if text is not None else ""
        
        addresses = []
        seen = set()
        
        for pattern in self.ADDRESS_PATTERNS:
            matches = re.finditer(pattern, text, re.I)
            for match in matches:
                address = match.group().strip()
                if address in seen:
                    continue
                
                # Skip addresses that are clearly false positives
                if self._is_false_positive_address(address, match, text):
                    continue

                # Basic validation
                confidence = 0.6
                if re.search(r'\d{5}(?:-\d{4})?', address):  # Has ZIP code
                    confidence += 0.2
                if any(word in address.lower() for word in ['street', 'avenue', 'road', 'boulevard']):
                    confidence += 0.1
                
                addresses.append({
                    "value": address,
                    "confidence": confidence,
                    "type": "po_box" if "box" in address.lower() else "street",
                    "source": "text_pattern"
                })
                seen.add(address)
        
        return sorted(addresses, key=lambda x: x["confidence"], reverse=True)
    
    def _is_false_positive_address(self, address: str, match, text: str) -> bool:
        """Filter out false positive addresses."""
        address_lower = address.lower()
        
        # Skip navigation/menu items
        if any(word in address_lower for word in ['dashboard', 'sign in', 'home', 'about', 'contact', 'blog', 'news']):
            return True
        
        # Skip course/education content
        if any(word in address_lower for word in ['course', 'certification', 'students', 'class', 'participants']):
            return True
        
        # Skip if it contains too many capital letters (likely not an address)
        capital_ratio = sum(1 for c in address if c.isupper()) / len(address)
        if capital_ratio > 0.3:
            return True
        
        # Skip if it's clearly part of a sentence structure
        context_start = max(0, match.start() - 20)
        context_end = min(len(text), match.end() + 20)
        context = text[context_start:context_end]
        
        if any(phrase in context.lower() for phrase in ['he took', 'when he', 'you could', 'here you']):
            return True
        
        return False

    def extract_websites_social(self, text: str, html: str = "", current_url: str = "") -> Dict[str, List[Dict[str, Any]]]:
        """Extract website URLs and social media profiles."""
        websites = []
        social_media = []
        seen_urls = set()
        
        current_domain = urlparse(current_url).netloc if current_url else ""
        
        # Extract from HTML links
        html_links = re.findall(r'href=["\']([^"\']+)["\']', html, re.I)
        
        # Extract from text URLs
        url_pattern = r'https?://[^\s<>"\']+|www\.[^\s<>"\']+|[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?:/[^\s]*)?'
        text_urls = re.findall(url_pattern, text)
        
        all_urls = html_links + text_urls
        
        for url in all_urls:
            if not url or url in seen_urls:
                continue
            
            # Normalize URL
            if not url.startswith(('http://', 'https://')):
                if url.startswith('www.'):
                    url = 'https://' + url
                elif '.' in url and not url.startswith('/'):
                    url = 'https://' + url
                else:
                    continue
            
            try:
                parsed = urlparse(url)
                domain = parsed.netloc.lower()
                
                # Skip current domain, common CDNs, and utility domains
                if self._should_skip_domain(domain, current_domain):
                    continue
                
                # Classify as social media or website
                if self._is_social_media(domain):
                    platform = self._get_social_platform(domain)
                    social_media.append({
                        "platform": platform,
                        "url": url,
                        "confidence": 0.9,
                        "source": "link_extraction"
                    })
                else:
                    # Only include business-relevant websites
                    if self._is_business_relevant_website(domain):
                        websites.append({
                            "url": url,
                            "domain": domain,
                            "confidence": 0.8,
                            "source": "link_extraction"
                        })
                
                seen_urls.add(url)
            except Exception:
                continue
        
        return {
            "websites": sorted(websites, key=lambda x: x["confidence"], reverse=True),
            "social_media": sorted(social_media, key=lambda x: x["confidence"], reverse=True)
        }
    
    def _should_skip_domain(self, domain: str, current_domain: str) -> bool:
        """Check if domain should be skipped."""
        skip_domains = [
            'example.com', 'localhost', 'fonts.googleapis.com', 'cdnjs.cloudflare.com',
            'fonts.gstatic.com', 'ajax.googleapis.com', 'code.jquery.com',
            'stackpath.bootstrapcdn.com', 'unpkg.com', 'cdn.jsdelivr.net'
        ]
        
        # Skip current domain
        if domain == current_domain:
            return True
        
        # Skip CDN and utility domains
        if any(skip in domain for skip in skip_domains):
            return True
        
        # Skip generic TLDs that are clearly not business domains
        if domain.endswith(('.tech', '.dev')) and not domain.count('.') > 1:
            return True
        
        return False
    
    def _is_business_relevant_website(self, domain: str) -> bool:
        """Check if website is business-relevant."""
        # Skip app stores and download sites
        if any(store in domain for store in ['play.google.com', 'apps.apple.com', 'microsoft.com']):
            return False
        
        # Only include domains that seem business-related
        business_indicators = ['course', 'institute', 'academy', 'company', 'corp', 'inc', 'ltd']
        return any(indicator in domain for indicator in business_indicators)

    def _is_social_media(self, domain: str) -> bool:
        """Check if domain is a social media platform."""
        social_domains = [
            'facebook.com', 'twitter.com', 'linkedin.com', 'instagram.com',
            'youtube.com', 'tiktok.com', 'pinterest.com', 'snapchat.com',
            'whatsapp.com', 'telegram.org', 'discord.com'
        ]
        return any(social in domain for social in social_domains)
    
    def _get_social_platform(self, domain: str) -> str:
        """Get social media platform name from domain."""
        platforms = {
            'facebook': 'Facebook',
            'twitter': 'Twitter',
            'linkedin': 'LinkedIn',
            'instagram': 'Instagram',
            'youtube': 'YouTube',
            'tiktok': 'TikTok',
            'pinterest': 'Pinterest',
            'snapchat': 'Snapchat',
            'whatsapp': 'WhatsApp',
            'telegram': 'Telegram',
            'discord': 'Discord'
        }
        
        for key, name in platforms.items():
            if key in domain:
                return name
        return 'Unknown'


class BusinessInfoExtractor:
    """Extract business information and decision maker details."""
    
    TRAVEL_KEYWORDS = [
        'travel', 'tour', 'trip', 'vacation', 'holiday', 'journey', 'bus', 'transport',
        'charter', 'excursion', 'sightseeing', 'cruise', 'flight', 'hotel', 'booking',
        'itinerary', 'destination', 'package', 'group travel', 'corporate travel'
    ]
    
    DECISION_MAKER_TITLES = [
        'ceo', 'president', 'director', 'manager', 'owner', 'founder', 'principal',
        'head', 'chief', 'vp', 'vice president', 'coordinator', 'organizer', 'planner'
    ]
    
    def extract_company_details(self, text: str, html: str = "", url: str = "") -> Dict[str, Any]:
        """Extract company name, industry, and services."""
        # Type safety for text parameter
        if not isinstance(text, str):
            if isinstance(text, dict):
                # Extract text content from dict
                text_parts = []
                def extract_text_recursive(obj):
                    if isinstance(obj, str):
                        text_parts.append(obj)
                    elif isinstance(obj, dict):
                        for value in obj.values():
                            extract_text_recursive(value)
                    elif isinstance(obj, list):
                        for item in obj:
                            extract_text_recursive(item)
                
                extract_text_recursive(text)
                text = " ".join(text_parts)
            else:
                text = str(text) if text is not None else ""
            
        # Extract company name from title tag
        title_match = re.search(r'<title>(.*?)</title>', html, re.I | re.S)
        title = title_match.group(1).strip() if title_match else ""
        
        # Clean title to get company name
        company_name = self._extract_company_name(title, url)
        
        # Industry classification
        industry = self._classify_industry(text)
        
        # Service extraction
        services = self._extract_services(text)
        
        # Company size estimation
        size_estimate = self._estimate_company_size(text)
        
        return {
            "company_name": company_name,
            "industry": industry,
            "services": services,
            "size_estimate": size_estimate,
            "travel_relevance": self._calculate_travel_relevance(text)
        }
    
    def _extract_company_name(self, title: str, url: str) -> str:
        """Extract company name from title and URL."""
        if not title:
            # Fallback to domain name
            try:
                domain = urlparse(url).netloc
                return domain.replace('www.', '').split('.')[0].title()
            except Exception:
                return "Unknown"
        
        # Remove common question/article patterns first
        title = re.sub(r'^[iI]\s+am\s+.*?[.?]\s*', '', title)  # Remove "I am..." questions
        title = re.sub(r'^\w+\s+(am|is|are|was|were)\s+.*$', '', title)  # Remove statement patterns
        
        # Clean title more intelligently
        title = re.sub(r'\s*[-|:\u2013\u2014]\s*(?:home|welcome|official site|website).*$', '', title, flags=re.I)
        
        # If title is still a question or statement, extract from URL
        if len(title.split()) > 8 or any(word in title.lower() for word in ['what', 'how', 'which', 'keeping', 'mind']):
            try:
                domain = urlparse(url).netloc
                domain_name = domain.replace('www.', '').split('.')[0]
                return domain_name.title()
            except Exception:
                return "Unknown"

        # Split on common separators and take the first meaningful part
        parts = re.split(r'[-|:\u2013\u2014]', title)
        name = parts[0].strip()
        
        # Remove common business descriptors that might be in the title
        descriptors = r'\b(?:career guidance|career counseling|education|courses|training|services|solutions|consulting|academy|institute|center|centre)\b'
        name = re.sub(descriptors, '', name, flags=re.I).strip()
        
        # Remove common suffixes
        suffixes = ['inc', 'llc', 'ltd', 'corp', 'company', 'co', 'pvt']
        words = name.split()
        while words and words[-1].lower().rstrip('.') in suffixes:
            words.pop()
        
        name = ' '.join(words) if words else title.split()[0] if title else "Unknown"
        
        # Fallback to domain if name is too generic or empty
        generic_names = ['home', 'welcome', 'index', 'main', 'page', '']
        if not name or len(name) < 2 or name.lower() in generic_names:
            try:
                domain = urlparse(url).netloc
                return domain.replace('www.', '').split('.')[0].title()
            except Exception:
                return "Unknown"
        
        return name or "Unknown"
    
    def _classify_industry(self, text: str) -> str:
        """Classify business industry based on content."""
        if text is None:
            return 'general'

        if isinstance(text, dict):
            # Extract text content from dict if possible
            text_content = ""
            for key, value in text.items():
                if isinstance(value, str):
                    text_content += f" {value}"
                elif isinstance(value, (list, tuple)):
                    for item in value:
                        if isinstance(item, str):
                            text_content += f" {item}"
            text = text_content.strip()
        elif isinstance(text, (list, tuple)):
            # Handle list/tuple inputs
            text_parts = []
            for item in text:
                if isinstance(item, str):
                    text_parts.append(item)
                elif isinstance(item, dict):
                    # Extract string values from dict items
                    for v in item.values():
                        if isinstance(v, str):
                            text_parts.append(v)
            text = " ".join(text_parts)
        elif not isinstance(text, str):
            # Convert other types to string
            try:
                text = str(text)
            except Exception:
                logger.warning(f"Could not convert {type(text)} to string for industry classification")
                return 'general'
        
        if not text or len(text.strip()) == 0:
            return 'general'
        
        text_lower = text.lower()
            
        industry_keywords = {
                'education_services': [
                    'career guidance', 'career counseling', 'career test', 'psychometric test',
                    'educational counseling', 'academic guidance', 'student counseling',
                    'career development', 'skill assessment', 'aptitude test', 'career planning'
                ],
                'travel_agency': ['travel agency', 'travel agent', 'tour operator', 'travel service'],
                'transportation': ['bus company', 'transportation', 'charter bus', 'shuttle service'],
                'hospitality': ['hotel', 'resort', 'accommodation', 'hospitality'],
                'event_planning': ['event planning', 'event management', 'wedding planner'],
                'education': ['school', 'university', 'college', 'student', 'education'],
                'corporate': ['corporate', 'business', 'company', 'enterprise'],
                'nonprofit': ['nonprofit', 'charity', 'foundation', 'ngo'],
                'government': ['government', 'municipal', 'city', 'county', 'state'],
                'healthcare': ['hospital', 'medical', 'healthcare', 'clinic'],
                'sports': ['sports', 'athletic', 'team', 'league'],
                'religious': ['church', 'religious', 'faith', 'ministry']
            }
        
        # Score each industry based on keyword matches
        industry_scores = {}
        for industry, keywords in industry_keywords.items():
            score = 0
            for keyword in keywords:
                # Count occurrences and weight by keyword importance
                count = text_lower.count(keyword)
                if count > 0:
                    # Longer, more specific keywords get higher weight
                    weight = len(keyword.split()) * 2
                    score += count * weight
            industry_scores[industry] = score
        
        # Return the industry with the highest score
        if industry_scores and max(industry_scores.values()) > 0:
            return max(industry_scores, key=industry_scores.get)
        
        return 'general'
    
    def _extract_services(self, text: str) -> List[str]:
        """Extract services offered by the business."""
        services = []
        text_lower = text.lower()
        
        service_patterns = {
            'group_travel': ['group travel', 'group tour', 'group trip'],
            'corporate_travel': ['corporate travel', 'business travel'],
            'event_transportation': ['event transportation', 'wedding transportation'],
            'sightseeing': ['sightseeing', 'city tour', 'guided tour'],
            'long_distance': ['long distance', 'interstate', 'cross country'],
            'local_transport': ['local transport', 'city bus', 'commuter'],
            'consulting': ['consulting', 'advisory', 'advice', 'strategy'],
            'training': ['training', 'workshop', 'course', 'bootcamp'],
            'software': ['software', 'SaaS', 'platform', 'application', 'cloud'],
            'marketing': ['marketing', 'advertising', 'SEO', 'branding', 'promotion'],
            'ecommerce': ['shop', 'store', 'retail', 'catalog', 'ecommerce'],
            'finance': ['loan', 'insurance', 'investment', 'mortgage', 'credit'],
            'real_estate': ['property', 'real estate', 'broker', 'realtor', 'lease'],
            'event_services': ['event', 'conference', 'wedding', 'seminar', 'expo'],
            'healthcare': ['clinic', 'hospital', 'doctor', 'pharmacy', 'healthcare']
        }
        
        for service, patterns in service_patterns.items():
            if any(pattern in text_lower for pattern in patterns):
                services.append(service)
        
        return services
    
    def _estimate_company_size(self, text: str) -> str:
        """Estimate company size based on content indicators."""
        text_lower = text.lower()
        
        # Look for size indicators
        if any(word in text_lower for word in ['enterprise', 'corporation', 'nationwide', 'international']):
            return 'large'
        elif any(word in text_lower for word in ['team of', 'staff of', 'employees']):
            # Try to extract numbers
            numbers = re.findall(r'(\d+)\s*(?:employees|staff|team members)', text_lower)
            if numbers:
                count = int(numbers[0])
                if count > 100:
                    return 'large'
                elif count > 20:
                    return 'medium'
                else:
                    return 'small'
        elif any(word in text_lower for word in ['family owned', 'local', 'small business']):
            return 'small'
        
        return 'unknown'
    
    def _calculate_travel_relevance(self, text: str) -> float:
        """Calculate relevance to travel industry (0-1 score)."""
        # Type safety
        if not isinstance(text, str):
            if isinstance(text, dict):
                text_parts = [str(v) for v in text.values() if isinstance(v, (str, int, float))]
                text = " ".join(text_parts)
            else:
                try:
                    text = str(text) if text is not None else ""
                except Exception:
                    return 0.0
        
        if not text:
            return 0.0

        text_lower = text.lower()
        matches = sum(1 for keyword in self.TRAVEL_KEYWORDS if keyword in text_lower)
        return min(matches / 10.0, 1.0)  # Normalize to 0-1
    
    def identify_decision_makers(self, text: str, html: str = "") -> List[Dict[str, Any]]:
        """Identify potential decision makers with improved accuracy."""
        
        #Type safety for text parameter
        if not isinstance(text, str):
            if isinstance(text, dict):
                text_parts = []
                def extract_text_recursive(obj):
                    if isinstance(obj, str):
                        text_parts.append(obj)
                    elif isinstance(obj, dict):
                        for value in obj.values():
                            extract_text_recursive(value)
                    elif isinstance(obj, list):
                        for item in obj:
                            extract_text_recursive(item)
                
                extract_text_recursive(text)
                text = " ".join(text_parts)
            else:
                text = str(text) if text is not None else ""
        
        decision_makers = []
        
        # First, try to find structured team/about sections
        team_sections = self._extract_team_sections(html, text)
        
        for section in team_sections:
            people = self._extract_people_from_section(section)
            decision_makers.extend(people)
        
        # If no structured sections found, look for individual mentions
        if not decision_makers:
            people = self._extract_people_from_unstructured_text(text)
            decision_makers.extend(people)
        
        # Remove duplicates and filter out obvious false positives
        decision_makers = self._filter_and_deduplicate_people(decision_makers)
        
        # Sort by authority level
        return sorted(decision_makers, key=lambda x: x.get('authority_score', 0), reverse=True)

    def _extract_people_from_unstructured_text(self, text: str) -> List[Dict[str, Any]]:
        """Extract people from unstructured text using context clues."""
        people = []
        
        # Look for sentences that mention someone with a title
        sentences = re.split(r'[.!?]+', text)
        
        for sentence in sentences:
            # Look for patterns like "founded by John Smith" or "John Smith is the CEO"
            founder_pattern = r'(?:founded|started|established|owned)\s+by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]*)*\s+[A-Z][a-z]+)'
            title_pattern = r'([A-Z][a-z]+(?:\s+[A-Z][a-z]*)*\s+[A-Z][a-z]+)\s+(?:is|was)\s+(?:the\s+)?([A-Z][^.]{2,30})'
            
            for pattern, title_pos in [(founder_pattern, "Founder"), (title_pattern, 2)]:
                matches = re.finditer(pattern, sentence, re.I)
                for match in matches:
                    name = match.group(1).strip()
                    title = title_pos if isinstance(title_pos, str) else match.group(title_pos).strip()
                    
                    if self._is_valid_person_name(name):
                        people.append({
                            "name": name,
                            "title": title,
                            "authority_score": self._calculate_authority_score(title),
                            "contact_type": "decision_maker" if self._calculate_authority_score(title) > 0.5 else "staff"
                        })
        
        return people

    def _filter_and_deduplicate_people(self, people: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicates and false positives."""
        seen_names = set()
        filtered_people = []
        
        for person in people:
            name = person['name']
            name_key = name.lower().replace(' ', '')
            
            # Skip duplicates
            if name_key in seen_names:
                continue
            
            # Enhanced false positive detection
            if self._is_definitely_not_person(name, person.get('title', '')):
                continue

            # Skip if authority score is too low (likely false positive)
            if person.get('authority_score', 0) < 0.2:
                continue
            
            seen_names.add(name_key)
            filtered_people.append(person)
        
        return filtered_people
    
    def _is_definitely_not_person(self, name: str, title: str) -> bool:
        """Enhanced detection of non-person entities."""
        name_lower = name.lower()
        title_lower = title.lower() if title else ""
        
        # Industry/sector names
        if any(word in name_lower for word in [
            'industry', 'indutry', 'shipping', 'transport', 'college', 'bank', 
            'staff', 'state', 'government', 'department', 'organization',
            'company', 'corporation', 'institute', 'academy', 'university'
        ]):
            return True
        
        # Course/education content
        if any(word in name_lower for word in [
            'course', 'class', 'test', 'exam', 'certification', 'degree',
            'bachelor', 'master', 'diploma', 'science', 'arts', 'commerce'
        ]):
            return True
        
        # Generic titles that aren't person-specific
        if any(phrase in title_lower for phrase in [
            'here you could', 'when he took', 'you could opt', 'keeping this'
        ]):
            return True
        
        # Names that are too generic or descriptive
        if len(name.split()) > 4 or any(word in name_lower for word in [
            'option', 'suitable', 'keeping', 'mind', 'career', 'guidance'
        ]):
            return True
        
        return False

    def _extract_team_sections(self, html: str, text: str) -> List[str]:
        """Extract team/about sections from content."""
        sections = []
        
        # Look for specific HTML structures that typically contain team info
        team_patterns = [
            # Look for sections with team-related classes/ids
            r'<(?:section|div)[^>]*(?:class|id)="[^"]*(?:team|about|staff|management|founder|leadership)[^"]*"[^>]*>(.*?)</(?:section|div)>',
            # Look for headers followed by content
            r'<h[1-6][^>]*>(?:[^<]*(?:team|about|staff|management|founder|leadership)[^<]*)</h[1-6]>(.*?)(?=<h[1-6]|</(?:section|div|body)|$)',
            # Look for specific bio sections
            r'<(?:section|div)[^>]*(?:class|id)="[^"]*(?:bio|profile|member)[^"]*"[^>]*>(.*?)</(?:section|div)>'
        ]
        
        for pattern in team_patterns:
            matches = re.findall(pattern, html, re.I | re.S | re.DOTALL)
            sections.extend(matches)
        
        # Text-based extraction for common patterns
        text_patterns = [
            r'(?:about us|our team|meet the team|leadership|management|founded by|started by|owned by|directors?)[:\n](.*?)(?=\n\s*\n|\Z)',
            r'(?:founder|ceo|president|director|manager)[:\s]+(.*?)(?=\n\s*\n|\Z)'
        ]
        
        for pattern in text_patterns:
            matches = re.findall(pattern, text, re.I | re.S | re.DOTALL)
            sections.extend(matches)
        
        return [section.strip() for section in sections if len(section.strip()) > 20]
        
    def _extract_people_from_section(self, section: str) -> List[Dict[str, Any]]:
        """Extract people information from a text section."""
        people = []
        
        #Remove HTML tags for cleaner processing
        clean_section = re.sub(r'<[^>]+>', ' ', section)
        clean_section = re.sub(r'\s+', ' ', clean_section).strip()
        
        # Pattern 1: Name followed by title/description
        # Look for patterns like "John Smith, CEO" or "John Smith - Director"
        pattern1 = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]*)*\s+[A-Z][a-z]+)\s*[,\-\u2013\u2014:]\s*([^,\n\r.]{3,50}?)(?=\s*[,\n\r.]|$)'
        matches1 = re.finditer(pattern1, clean_section)
        
        for match in matches1:
            name = match.group(1).strip()
            title = match.group(2).strip()
            
            if self._is_valid_person_name(name) and self._is_valid_title(title):
                people.append({
                    "name": name,
                    "title": title,
                    "authority_score": self._calculate_authority_score(title),
                    "contact_type": "decision_maker" if self._calculate_authority_score(title) > 0.5 else "staff"
                })
        
        # Pattern 2: Title followed by name
        # Look for patterns like "CEO: John Smith" or "Director - John Smith"
        pattern2 = r'\b([A-Z][^,\n\r:]{2,30}?)\s*[:\-\u2013\u2014]\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]*)*\s+[A-Z][a-z]+)'
        matches2 = re.finditer(pattern2, clean_section)
        
        for match in matches2:
            title = match.group(1).strip()
            name = match.group(2).strip()
            
            if self._is_valid_person_name(name) and self._is_valid_title(title):
                people.append({
                    "name": name,
                    "title": title,
                    "authority_score": self._calculate_authority_score(title),
                    "contact_type": "decision_maker" if self._calculate_authority_score(title) > 0.5 else "staff"
                })
        
        return people
    
    def _is_valid_person_name(self, name: str) -> bool:
        """Validate if a string looks like a real person name."""
        if not name or len(name) < 3:
            return False
        
        # Must have at least first and last name
        parts = name.split()
        if len(parts) < 2:
            return False
        
        # Exclude obvious non-names (course names, services, etc.)
        excluded_words = [
            'plan', 'test', 'course', 'management', 'development', 'science',
            'design', 'arts', 'communication', 'business', 'professional',
            'career', 'skill', 'index', 'basic', 'advance', 'face', 'mentorship',
            'ideal', 'humanities', 'commerce', 'engineering', 'online', 'digital',
            'back', 'end', 'web', 'explore', 'colleges', 'new', 'zealand',
            'hong', 'kong', 'sign', 'dashboard', 'questions', 'report',
            'taken', 'answers', 'post', 'reply', 'verified', 'ask',
            'university', 'hospital', 'organization', 'entrance', 'exam',
            'parents', 'psychometric', 'knowledge', 'base', 'pricing',
            'guide', 'profile', 'certification', 'community', 'jobs',
            'exclusive', 'interviews', 'impact', 'stories', 'inspirational',
            'shipping', 'industry', 'state', 'bank', 'staff', 'college',
            'hyderabad', 'retirement', 'suitable', 'option', 'keeping'
        ]
        
        name_lower = name.lower()
        for word in excluded_words:
            if word in name_lower:
                return False
        
        # Check if all parts start with capital letter (proper name format)
        for part in parts:
            if not part[0].isupper():
                return False
            if len(part) > 1 and not part[1:].islower() and not part.isupper():
                # Allow for names like McDonald, O'Connor
                if not any(sep in part for sep in ["'", "Mc", "Mac"]):
                    return False
        
        # Exclude very long "names" (likely course descriptions)
        if len(name) > 50:
            return False
        
        return True

    def _is_valid_title(self, title: str) -> bool:
        """Validate if a string looks like a job title."""
        if not title or len(title) < 2 or len(title) > 100:
            return False
    
        # Exclude obvious non-titles
        excluded_patterns = [
            r'^\|+$',  # Just pipe symbols
            r'^\s*$',  # Just whitespace
            r'^\d+\s*answers?$',  # "3 answers"
            r'^←>.*$',  # Navigation symbols
            r'^\w{1,2}$',  # Very short abbreviations without context
            r'^(?:test|for|and|the|in|of|to|at|on|is|are|was|were)$'  # Common words that aren't titles
        ]
        
        title_lower = title.lower()
        for pattern in excluded_patterns:
            if re.match(pattern, title_lower):
                return False
    
        return True

    def _calculate_authority_score(self, title: str) -> float:
        """Calculate authority score with better title recognition."""
        if not title:
            return 0.0
        
        title_lower = title.lower()
        
        # Very high authority (C-level, owners, founders)
        if any(word in title_lower for word in ['ceo', 'chief executive', 'president', 'owner', 'founder', 'principal']):
            return 1.0
        
        # High authority (directors, VPs, chiefs)
        if any(word in title_lower for word in ['director', 'vp', 'vice president', 'chief', 'head of']):
            return 0.9
        
        # Medium-high authority (managers, leads)
        if any(word in title_lower for word in ['manager', 'head', 'lead', 'senior manager']):
            return 0.7
        
        # Medium authority (coordinators, supervisors)
        if any(word in title_lower for word in ['coordinator', 'supervisor', 'team lead']):
            return 0.5
        
        # Low-medium authority (specialists, analysts)
        if any(word in title_lower for word in ['specialist', 'analyst', 'consultant', 'advisor']):
            return 0.3
        
        # Check for educational/counseling specific titles
        if any(word in title_lower for word in ['counselor', 'counsellor', 'guidance', 'dean', 'professor']):
            return 0.6
        
        # Default for any other title
        return 0.2


class LeadScorer:
    """Calculate lead scores based on multiple factors."""
    
    def calculate_lead_score(self, contact_info: Dict[str, Any], business_info: Dict[str, Any], 
                           intent_indicators: List[Dict[str, str]], data_confidence: float) -> Dict[str, Any]:
        """Calculate comprehensive lead score (0-100)."""
        
        # Factor weights (must sum to 1.0)
        weights = {
            'contact_quality': 0.30,
            'business_relevance': 0.25,
            'intent_indicators': 0.20,
            'data_confidence': 0.15,
            'opportunity_size': 0.10
        }
        
        # Calculate individual scores
        contact_score = self._score_contact_quality(contact_info)
        relevance_score = self._score_business_relevance(business_info)
        intent_score = self._score_intent_indicators(intent_indicators)
        confidence_score = data_confidence
        opportunity_score = self._score_opportunity_size(business_info)
        
        # Weighted total
        total_score = (
            contact_score * weights['contact_quality'] +
            relevance_score * weights['business_relevance'] +
            intent_score * weights['intent_indicators'] +
            confidence_score * weights['data_confidence'] +
            opportunity_score * weights['opportunity_size']
        ) * 100
        
        # Classify lead
        classification = self._classify_lead(total_score)
        
        return {
            "total_score": round(total_score, 1),
            "classification": classification,
            "factor_scores": {
                "contact_quality": round(contact_score * 100, 1),
                "business_relevance": round(relevance_score * 100, 1),
                "intent_indicators": round(intent_score * 100, 1),
                "data_confidence": round(confidence_score * 100, 1),
                "opportunity_size": round(opportunity_score * 100, 1)
            },
            "weights": weights
        }
    
    def _score_contact_quality(self, contact_info: Dict[str, Any]) -> float:
        """Score contact information quality (0-1)."""
        score = 0.0
        
        # Email presence and quality
        emails = contact_info.get('emails', [])
        if emails:
            score += 0.3
            # Bonus for decision maker emails
            if any(email.get('is_personal', False) for email in emails):
                score += 0.1
        
        # Phone presence and quality
        phones = contact_info.get('phones', [])
        if phones:
            score += 0.3
            # Bonus for high confidence phones
            if any(phone.get('confidence', 0) > 0.8 for phone in phones):
                score += 0.1
        
        # Address presence
        addresses = contact_info.get('addresses', [])
        if addresses:
            score += 0.2
        
        # Decision maker identification
        decision_makers = contact_info.get('decision_makers', [])
        if decision_makers:
            score += 0.1
            # Bonus for high authority decision makers
            if any(dm.get('authority_score', 0) > 0.8 for dm in decision_makers):
                score += 0.1
        
        return min(score, 1.0)
    
    def _score_business_relevance(self, business_info: Dict[str, Any]) -> float:
        """Score business relevance to travel industry (0-1)."""
        score = 0.0
        
        # Travel relevance
        travel_relevance = business_info.get('travel_relevance', 0)
        score += travel_relevance * 0.5
        
        # Industry alignment
        industry = business_info.get('industry', '')
        if industry in ['travel_agency', 'transportation', 'event_planning']:
            score += 0.3
        elif industry in ['education', 'corporate', 'nonprofit']:
            score += 0.2
        elif industry in ['hospitality', 'sports', 'religious']:
            score += 0.1
        
        # Service alignment
        services = business_info.get('services', [])
        travel_services = ['group_travel', 'charter_bus', 'corporate_travel', 'event_transportation']
        matching_services = sum(1 for service in services if service in travel_services)
        score += min(matching_services * 0.1, 0.2)
        
        return min(score, 1.0)
    
    def _score_intent_indicators(self, intent_indicators: List[Dict[str, str]]) -> float:
        """Score intent indicators (0-1)."""
        if not intent_indicators:
            return 0.0
        
        # High intent indicators
        high_intent = ['booking', 'quote', 'inquiry', 'planning', 'event']
        medium_intent = ['travel', 'tour', 'trip', 'group']
        
        score = 0.0
        for indicator in intent_indicators:
            # Extract both category and match values to check
            category = indicator.get('category', '').lower()
            match = indicator.get('match', '').lower()
            
            # Check category
            if any(hi in category for hi in high_intent):
                score += 0.3
            elif any(mi in category for mi in medium_intent):
                score += 0.1
            
            # Check match value
            if any(hi in match for hi in high_intent):
                score += 0.3
            elif any(mi in match for mi in medium_intent):
                score += 0.1
        
        return min(score, 1.0)
    
    def _score_opportunity_size(self, business_info: Dict[str, Any]) -> float:
        """Score potential opportunity size (0-1)."""
        score = 0.0
        
        # Company size
        size = business_info.get('size_estimate', 'unknown')
        if size == 'large':
            score += 0.5
        elif size == 'medium':
            score += 0.3
        elif size == 'small':
            score += 0.2
        
        # Industry potential
        industry = business_info.get('industry', '')
        if industry in ['corporate', 'education', 'government']:
            score += 0.3
        elif industry in ['nonprofit', 'sports', 'religious']:
            score += 0.2
        
        return min(score, 1.0)
    
    def _classify_lead(self, score: float) -> str:
        """Classify lead based on score."""
        if score >= 80:
            return 'hot'
        elif score >= 60:
            return 'warm'
        elif score >= 40:
            return 'cold'
        else:
            return 'research'


def _analyze_sections_for_client_info(sections: List[Dict[str, str]], contact_extractor: ContactExtractor) -> List[Dict[str, Any]]:
    """Analyze sections using traditional extraction approach to identify potential client information."""
    high_potential_sections = []
    
    for section in sections:
        section_text = section.get("text", "")
        section_tag = section.get("tag", "")
        section_class = section.get("class", "")
        section_id = section.get("id", "")
        
        # Skip very short sections
        if len(section_text) < 10:
            continue
            
        # Extract potential contact information from this section
        section_phones = contact_extractor.extract_phones(section_text)
        section_emails = contact_extractor.extract_emails(section_text)
        section_addresses = contact_extractor.extract_addresses(section_text)
        section_web_social = contact_extractor.extract_websites_social(section_text)
        
        # Calculate confidence score for this section
        confidence_score = 0.0
        contact_found = False
        
        # Check for contact information
        if section_phones:
            confidence_score += 0.4
            contact_found = True
        if section_emails:
            confidence_score += 0.4
            contact_found = True
        if section_addresses:
            confidence_score += 0.2
            contact_found = True
        if section_web_social:
            confidence_score += 0.2
            contact_found = True
            
        # Check for business-related keywords
        business_keywords = [
            'business', 'company', 'contact', 'phone', 'email', 'address',
            'office', 'location', 'headquarters', 'branch', 'about us',
            'team', 'staff', 'management', 'director', 'manager', 'ceo',
            'founder', 'owner', 'social media', 'facebook', 'twitter',
            'linkedin', 'instagram', 'website', 'www', 'http'
        ]
        
        keyword_matches = sum(1 for keyword in business_keywords if keyword in section_text.lower())
        if keyword_matches > 0:
            confidence_score += min(keyword_matches * 0.05, 0.2)
            
        # Boost score for certain HTML elements
        if section_tag in ['footer', 'header']:
            confidence_score += 0.1
        if 'contact' in section_class.lower() or 'contact' in section_id.lower():
            confidence_score += 0.2
        if 'about' in section_class.lower() or 'about' in section_id.lower():
            confidence_score += 0.15
            
        # Only include sections with reasonable confidence or contact information
        if confidence_score >= 0.3 or contact_found:
            high_potential_sections.append({
                "section": section,
                "confidence": min(confidence_score, 1.0),
                "contact_info": {
                    "phones": section_phones,
                    "emails": section_emails,
                    "addresses": section_addresses,
                    "websites_social": section_web_social
                },
                "keyword_matches": keyword_matches
            })
    
    # Sort by confidence score (highest first)
    return sorted(high_potential_sections, key=lambda x: x["confidence"], reverse=True)


def smart_filter_sections(ai_lead_info: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Smart filtration function to skip irrelevant sections and prioritize high-value sections."""
    if not ai_lead_info:
        return []
    
    # Define irrelevant section patterns
    irrelevant_patterns = [
        'privacy policy', 'terms', 'legal', 'pricing', 'menu', 'help', 'faq', 
        'support', 'services', 'cookie', 'disclaimer', 'copyright', 'navigation',
        'breadcrumb', 'sidebar', 'advertisement', 'ads', 'banner', 'header menu',
        'footer menu', 'social links', 'search', 'login', 'register', 'sign up',
        'sign in', 'dashboard', 'cart', 'checkout', 'payment', 'shipping',
        'return policy', 'refund', 'warranty', 'testimonial', 'review',
        'blog', 'news', 'article', 'press release', 'media', 'gallery',
        'portfolio', 'case study', 'white paper', 'download', 'resource'
    ]
    
    # Define high-value section indicators
    high_value_indicators = [
        'contact', 'about', 'team', 'staff', 'management', 'leadership',
        'founder', 'ceo', 'director', 'office', 'location', 'address',
        'phone', 'email', 'headquarters', 'branch', 'business', 'company',
        'organization', 'profile', 'biography', 'bio', 'meet', 'our team'
    ]
    
    filtered_sections = []
    
    for section_data in ai_lead_info:
        section = section_data.get("section", {})
        section_text = section.get("text", "").lower()
        section_class = section.get("class", "").lower()
        section_id = section.get("id", "").lower()
        section_tag = section.get("tag", "").lower()
        
        # Skip sections that match irrelevant patterns
        is_irrelevant = False
        for pattern in irrelevant_patterns:
            if (pattern in section_text or 
                pattern in section_class or 
                pattern in section_id):
                is_irrelevant = True
                break
        
        if is_irrelevant:
            continue
        
        # Calculate priority score
        priority_score = section_data.get("confidence", 0.0)
        
        # Boost priority for high-value indicators
        high_value_matches = sum(1 for indicator in high_value_indicators 
                               if (indicator in section_text or 
                                   indicator in section_class or 
                                   indicator in section_id))
        
        if high_value_matches > 0:
            priority_score += min(high_value_matches * 0.1, 0.3)
        
        # Boost priority for sections with actual contact information
        contact_info = section_data.get("contact_info", {})
        if (contact_info.get("phones") or 
            contact_info.get("emails") or 
            contact_info.get("addresses")):
            priority_score += 0.2
        
        # Boost priority for certain HTML tags
        if section_tag in ['footer', 'header', 'aside']:
            priority_score += 0.1
        
        # Update the section data with new priority score
        section_data_copy = section_data.copy()
        section_data_copy["priority_score"] = min(priority_score, 1.0)
        
        # Only include sections with reasonable priority
        if priority_score >= 0.4:
            filtered_sections.append(section_data_copy)
    
    # Sort by priority score (highest first) and limit to top sections
    filtered_sections = sorted(filtered_sections, key=lambda x: x["priority_score"], reverse=True)
    
    # Limit to top 10 sections to avoid overwhelming the AI
    return filtered_sections[:20]


def _extract_from_structured_data(structured_data: List[Dict]) -> tuple:
    """Extract contact and business info from JSON-LD structured data."""
    contact_info = {"phones": [], "emails": [], "addresses": []}
    business_info = {}
    
    for item in structured_data:
        item_type = item.get("@type", "")
        
        # Handle Organization/Publisher info
        if item_type in ["Organization", "LocalBusiness", "TravelAgency", "NewsMediaOrganization"]:
            if "name" in item:
                business_info["company_name"] = item["name"]
            if "legalName" in item:
                business_info["legal_name"] = item["legalName"]
            if "description" in item:
                business_info["description"] = item["description"]
            if "url" in item:
                business_info["website"] = item["url"]
            if "foundingDate" in item:
                business_info["founding_date"] = item["foundingDate"]
            
            # Extract contact info if present
            if "telephone" in item:
                contact_info["phones"].append({
                    "value": item["telephone"],
                    "clean_value": re.sub(r'[^\d+]', '', item["telephone"]),
                    "confidence": 0.9,
                    "source": "structured_data",
                    "context": f"JSON-LD {item_type}"
                })
            if "email" in item:
                contact_info["emails"].append({
                    "value": item["email"],
                    "clean_value": item["email"],
                    "confidence": 0.9,
                    "source": "structured_data",
                    "context": f"JSON-LD {item_type}"
                })
        
        # Handle nested publisher info
        if "publisher" in item and isinstance(item["publisher"], dict):
            pub = item["publisher"]
            if pub.get("name") and not business_info.get("company_name"):
                business_info["company_name"] = pub["name"]
            if pub.get("url"):
                business_info["website"] = pub["url"]
        
        # Handle Author info (for decision makers/contacts)
        if "author" in item and isinstance(item["author"], dict):
            author = item["author"]
            if author.get("@type") == "Person":
                decision_maker = {}
                if author.get("name"):
                    decision_maker["name"] = author["name"]
                if author.get("jobTitle"):
                    decision_maker["title"] = author["jobTitle"]
                if author.get("description"):
                    decision_maker["bio"] = author["description"]
                if author.get("url"):
                    decision_maker["profile_url"] = author["url"]
                
                # Extract social media from sameAs
                if author.get("sameAs"):
                    social_links = []
                    for link in author["sameAs"]:
                        if "linkedin.com" in link:
                            social_links.append({"platform": "linkedin", "url": link})
                        elif "instagram.com" in link:
                            social_links.append({"platform": "instagram", "url": link})
                        elif "twitter.com" in link or "x.com" in link:
                            social_links.append({"platform": "twitter", "url": link})
                    if social_links:
                        decision_maker["social_media"] = social_links
                
                if decision_maker:
                    if "decision_makers" not in business_info:
                        business_info["decision_makers"] = []
                    business_info["decision_makers"].append(decision_maker)
    
    return contact_info, business_info


def _filter_structured_data_for_ai(structured_data: List[Dict]) -> Dict:
    """Filter structured data to include only relevant parts for AI analysis."""
    if not structured_data:
        return {}
    
    relevant_types = [
        "Organization", "LocalBusiness", "TravelAgency", "Person", 
        "ContactPoint", "PostalAddress", "Service", "Product",
        "NewsMediaOrganization", "Article"
    ]
    
    filtered_data = []
    
    for item in structured_data:
        item_type = item.get("@type", "")
        
        if any(rel_type in str(item_type) for rel_type in relevant_types):
            filtered_item = {"@type": item_type}
            
            # Define relevant fields (excluding images, logos, identifiers, etc.)
            relevant_fields = [
                "name", "legalName", "description", "telephone", "email", 
                "address", "url", "sameAs", "contactPoint", "member", "employee",
                "serviceType", "areaServed", "jobTitle", "foundingDate",
                "publisher", "author", "worksFor", "parentOrganization",
                "headline", "articleBody", "keywords", "articleSection"
            ]
            
            for field in relevant_fields:
                if field in item:
                    # Handle nested objects
                    if field in ["publisher", "author", "worksFor", "parentOrganization"]:
                        nested_item = item[field]
                        if isinstance(nested_item, dict):
                            filtered_nested = {}
                            nested_relevant_fields = [
                                "name", "legalName", "description", "url", "jobTitle",
                                "sameAs", "@type"
                            ]
                            for nested_field in nested_relevant_fields:
                                if nested_field in nested_item:
                                    filtered_nested[nested_field] = nested_item[nested_field]
                            if filtered_nested:
                                filtered_item[field] = filtered_nested
                    else:
                        filtered_item[field] = item[field]
            
            if len(filtered_item) > 1:  # More than just @type
                filtered_data.append(filtered_item)
    
    return {"filtered_json_ld": filtered_data} if filtered_data else {}

def _filter_json_ld_for_ai(structured_data):
    """Filter JSON-LD data for AI analysis."""
    if not structured_data:
        return []
    
    # Handle case where structured_data is a dict instead of list
    if isinstance(structured_data, dict):
        structured_data = [structured_data]
    elif not isinstance(structured_data, list):
        return []
    
    structured_data_summary = []
    for json_obj in structured_data:
        try:
            if not isinstance(json_obj, dict):
                continue
                
            json_str = json.dumps(json_obj)
            wrapped_str = f"'''{json_str}'''"
            filtered_obj = filter_jsonld(wrapped_str)
            if filtered_obj:
                structured_data_summary.append(filtered_obj)
        except (TypeError, ValueError) as e:
            logger.warning(f"Error filtering JSON-LD object: {e}")
            continue

    return structured_data_summary
    

def extract_lead_information(html: str, text: str, url: str = "", 
                           sections: List[Dict[str, str]] = None,
                           structured_data: List[Dict] = None) -> Dict[str, Any]:
    """Main function to extract comprehensive lead information."""
    logger.info(f"Starting lead information extraction for {url}")
    
    # Debug logging to identify the issue
    logger.debug(f"Text type: {type(text)}, HTML type: {type(html)}")
    
    # Enhanced type safety checks
    if not isinstance(html, str):
        html = str(html) if html is not None else ""
    
    if not isinstance(text, str):
        if isinstance(text, dict):
            logger.warning(f"Text parameter is dict, extracting string content: {list(text.keys())}")
            # Extract text content from dict
            text_parts = []
            def extract_text_recursive(obj):
                if isinstance(obj, str):
                    text_parts.append(obj)
                elif isinstance(obj, dict):
                    for key, value in obj.items():
                        if key in ['name', 'description', 'reviewBody', 'text', 'content']:
                            extract_text_recursive(value)
                elif isinstance(obj, list):
                    for item in obj:
                        extract_text_recursive(item)
            
            extract_text_recursive(text)
            text = " ".join(text_parts)
            logger.debug(f"Converted dict to text, length: {len(text)}")
        elif isinstance(text, (list, tuple)):
            text_parts = []
            for item in text:
                if isinstance(item, str):
                    text_parts.append(item)
                elif isinstance(item, dict):
                    for value in item.values():
                        if isinstance(value, str):
                            text_parts.append(value)
            text = " ".join(text_parts)
        else:
            text = str(text) if text is not None else ""
    
    if not isinstance(url, str):
        url = str(url) if url is not None else ""
    
    # Ensure text is not empty after conversion
    if not text.strip():
        logger.warning("Text content is empty after type conversion")
        text = "No text content available"
    
    # Initialize extractors
    contact_extractor = ContactExtractor()
    business_extractor = BusinessInfoExtractor()
    scorer = LeadScorer()
    
    # Process sections if provided for AI lead extraction
    ai_lead_info = []
    if sections:
        ai_lead_info = _analyze_sections_for_client_info(sections, contact_extractor)
    
    # Extract from structured data first (traditional extraction)
    structured_contact_info = {}
    structured_business_info = {}
    
    if structured_data:
        structured_contact_info, structured_business_info = _extract_from_structured_data(structured_data)
    
    # Extract contact information from main text (existing logic)
    phones = contact_extractor.extract_phones(text, html)
    emails = contact_extractor.extract_emails(text, html)
    addresses = contact_extractor.extract_addresses(text)
    web_social = contact_extractor.extract_websites_social(text, html, url)
    
    # NEW: Merge structured data with extracted data
    phones.extend(structured_contact_info.get("phones", []))
    emails.extend(structured_contact_info.get("emails", []))

    contact_info = {
        "phones": phones,
        "emails": emails,
        "addresses": addresses,
        "websites": web_social["websites"],
        "social_media": web_social["social_media"]
    }
    print("="*100)
    print("Contact info: ", contact_info)
    print("="*100)
    # Extract business information
    company_details = business_extractor.extract_company_details(text, html, url)
    decision_makers = business_extractor.identify_decision_makers(text, html)
    
    # NEW: Merge business info
    business_info = {
        **company_details,
        **structured_business_info,  # Structured data takes precedence
        "decision_makers": decision_makers
    }
    
    # Add decision makers to contact info for scoring
    contact_info["decision_makers"] = decision_makers
    print("="*100)
    print("Business info: ", business_info)
    print("="*100)
    # Extract intent indicators (simple keyword matching for now)
    intent_categories = {
        "travel_planning": [
            "planning a trip", "arranging travel", "organizing tour",
            "planning to visit", "looking for transport", "itinerary",
            "bus booking", "charter service", "hire a bus", "reserve seats"
        ],
        "purchase_intent": ["buy", "purchase", "order", "get started", "subscribe"],
        "quote_request": ["request a quote", "get a quote", "pricing", "estimate"],
        "demo_interest": ["demo", "trial", "schedule a demo", "free trial"],
        "service_inquiry": ["services", "solutions", "offerings", "learn more"],
        "job_hiring": ["hiring", "apply now", "careers", "recruiting"],
        "support_request": ["help", "support", "customer service", "issue", "complaint"],
        "corporate_travel": [
            "corporate tour", "office trip", "team outing",
            "company travel", "business delegation", "staff transport"
        ],
        "general_inquiry": [
            "need a quote", "request for quotation", "price inquiry",
            "availability", "details please"
        ]
    }
    
    intent_weights = {
        "travel_planning": 0.9,
        "corporate_travel": 0.8,
        "group_travel": 0.85,
        "general_inquiry": 1.0,
        "past_travel": 0.4
    }
    text_lower = text.lower()
    intent_indicators: List[Dict[str, str]] = []

    for category, keywords in intent_categories.items():
        for keyword in keywords:
            if keyword in text_lower:
                intent_indicators.append({"category": category, "match": keyword})
    # Compute intent score (take strongest signal)
    intent_score = max(
        (intent_weights.get(ind["category"], 0) for ind in intent_indicators),
        default=0
    )
    
    # Calculate data confidence (average of extraction confidences)
    all_confidences = []
    for item_list in [phones, emails, addresses]:
        all_confidences.extend([item.get('confidence', 0) for item in item_list])
    
    data_confidence = sum(all_confidences) / len(all_confidences) if all_confidences else 0.5
    # print("\n================intent and dc ==========================\n")
    # print(intent_indicators)
    # print('\n')
    # print(data_confidence)
    # print("\n================End==========================\n")
    # Calculate lead score
    lead_score = scorer.calculate_lead_score(contact_info, business_info, intent_indicators, data_confidence)
    # print("\n================Start: Inside Lead extraction py==========================\n")
    # print(structured_data)
    # print("\n================End: Inside Lead extraction py==========================\n")

    structured_data_summary = _filter_json_ld_for_ai(structured_data) if structured_data else []
    # print("\n================Start: Inside Lead extraction py: after ai filter ==========================\n")
    # print(structured_data_summary)
    # print("\n================End: Inside Lead extraction py==========================\n")
    # print("\n=========== Still running: Passed suspicious place 4===========")
    return {
        "contact_information": contact_info,
        "business_information": business_info,
        "intent_indicators": intent_indicators,
        "intent_score": round(intent_score, 2),
        "lead_score": lead_score,
        "ai_lead_info": ai_lead_info,
        "structured_data_summary": structured_data_summary,  # NEW: Filtered for AI
        "extraction_metadata": {
            "url": url,
            "data_confidence": round(data_confidence, 3),
            "extraction_timestamp": None
        }
    }

def main():
    sections = [{'tag': 'p', 'text': 'TTC Portfolio of Brands', 'class': 'sisterbrands-collapsed__text text-label-legend', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': 'Get a Quote', 'class': 'topbar__link text-link-xs', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': 'Agents Login', 'class': 'topbar__link text-link-xs', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'My Trafalgar', 'class': 'topbar__dropdown-text text-label-s', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Destinations', 'class': 'nav-item__button-text text-label-s', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Ways To Go', 'class': 'nav-item__button-text text-label-s', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'About Us', 'class': 'nav-item__button-text text-label-s', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'River Cruises', 'class': 'nav-item__button-text text-label-s', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': '+91 22 26143300', 'class': 'navbar-contact__icon-link', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': '+91 22 26143300', 'class': 'navbar-contact__phone navbar-contact__phone--big-size text-label-l', 'id': '', 'parent_tag': 'a'}, {'tag': 'p', 'text': 'Customers', 'class': 'text-link-xs', 'id': '', 'parent_tag': 'button'}, {'tag': 'a', 'text': 'Find Out More', 'class': 'btn btn--L hero-content__button btn-pr btn--icon-Left', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Find Out More', 'class': 'text-button-l', 'id': '', 'parent_tag': 'a'}, {'tag': 'a', 'text': 'TRAFALGAR IS RATED4.6 / 5BASED ON 130,000+ VERIFIED REVIEWS\xa0 |', 'class': 'banner-with-mask', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'TRAFALGAR IS RATED4.6 / 5BASED ON 130,000+ VERIFIED REVIEWS\xa0 |', 'class': '', 'id': '', 'parent_tag': 'header'}, {'tag': 'h2', 'text': '2026 Price Drop Promise', 'class': 'image-tile-text-row__title no-subtitle rich-text text-h2-title-s', 'id': '', 'parent_tag': 'div'}, {'tag': 'h2', 'text': 'Off-season Bundles', 'class': 'image-tile-text-row__title no-subtitle rich-text text-h2-title-s', 'id': '', 'parent_tag': 'div'}, {'tag': 'h2', 'text': 'Last Minute Deals to Europe', 'class': 'image-tile-text-row__title no-subtitle rich-text text-h2-title-s', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Plus, other benefits for booking early**', 'class': '', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Land + air packages starting at $2,251* in partnership with United Airlines', 'class': '', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Act fast! Save on tours departing in the next 4 months', 'class': '', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': 'See Deals', 'class': 'btn btn--L image-tile-buttons-row__button btn-pr btn--icon-Left', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'See Deals', 'class': 'text-button-l', 'id': '', 'parent_tag': 'a'}, {'tag': 'a', 'text': 'See Bundles', 'class': 'btn btn--L image-tile-buttons-row__button btn-pr-inv btn--icon-Left', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'See Bundles', 'class': 'text-button-l', 'id': '', 'parent_tag': 'a'}, {'tag': 'a', 'text': 'See Deals', 'class': 'btn btn--L image-tile-buttons-row__button btn-pr btn--icon-Left', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'See Deals', 'class': 'text-button-l', 'id': '', 'parent_tag': 'a'}, {'tag': 'h2', 'text': 'Popular searches', 'class': 'title-section__title text-h2-title-s', 'id': '', 'parent_tag': 'header'}, {'tag': 'a', 'text': 'Domestic tripsSee America in a new light', 'class': 'tile__link-wrapper small-tile-row__content-tile', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Domestic trips', 'class': 'title-section__title text-h2-title-xxs', 'id': '', 'parent_tag': 'header'}, {'tag': 'p', 'text': 'See America in a new light', 'class': '', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': 'Vacations under 14 daysLimited availability. Selling fast.', 'class': 'tile__link-wrapper small-tile-row__content-tile', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Vacations under 14 days', 'class': 'title-section__title text-h2-title-xxs', 'id': '', 'parent_tag': 'header'}, {'tag': 'p', 'text': 'Limited availability. Selling fast.', 'class': '', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': "Today's best travel dealsSave now. Don't miss out.", 'class': 'tile__link-wrapper small-tile-row__content-tile', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': "Today's best travel deals", 'class': 'title-section__title text-h2-title-xxs', 'id': '', 'parent_tag': 'header'}, {'tag': 'p', 'text': "Save now. Don't miss out.", 'class': '', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': 'Offers for travel groups of 9+Save when you book 9+ guests', 'class': 'tile__link-wrapper small-tile-row__content-tile', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Offers for travel groups of 9+', 'class': 'title-section__title text-h2-title-xxs', 'id': '', 'parent_tag': 'header'}, {'tag': 'p', 'text': 'Save when you book 9+ guests', 'class': '', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': 'Past guest benefitsSavings with Global Tour Rewards', 'class': 'tile__link-wrapper small-tile-row__content-tile', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Past guest benefits', 'class': 'title-section__title text-h2-title-xxs', 'id': '', 'parent_tag': 'header'}, {'tag': 'p', 'text': 'Savings with Global Tour Rewards', 'class': '', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': 'Tours under $2000Browse our value vacations', 'class': 'tile__link-wrapper small-tile-row__content-tile', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Tours under $2000', 'class': 'title-section__title text-h2-title-xxs', 'id': '', 'parent_tag': 'header'}, {'tag': 'p', 'text': 'Browse our value vacations', 'class': '', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': 'All Last Minute Deals', 'class': 'btn btn--L trip-cards-component__button btn-pr btn--icon-Left', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'All Last Minute Deals', 'class': 'text-button-l', 'id': '', 'parent_tag': 'a'}, {'tag': 'h2', 'text': 'Looking for inspiration?', 'class': 'title-section__title text-h2-title-s', 'id': '', 'parent_tag': 'header'}, {'tag': 'a', 'text': 'Order or download your free brochure', 'class': 'remove-link-styles', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Order or download your free brochure', 'class': 'image-tile__title image-tile__title--clickable text-label-m', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': '14 reasons why you should do a River Cruise in Germany and France in 2026', 'class': 'remove-link-styles', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': '14 reasons why you should do a River Cruise in Germany and France in 2026', 'class': 'image-tile__title image-tile__title--clickable text-label-m', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': '17 Useful Travel Tips for First-Time Tourers – From Real Trafalgar Guests', 'class': 'remove-link-styles', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': '17 Useful Travel Tips for First-Time Tourers – From Real Trafalgar Guests', 'class': 'image-tile__title image-tile__title--clickable text-label-m', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': '25 best things to do in Spain in 2026', 'class': 'remove-link-styles', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': '25 best things to do in Spain in 2026', 'class': 'image-tile__title image-tile__title--clickable text-label-m', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': 'Got questions? Find your answer in our popular FAQs >', 'class': 'remove-link-styles', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Got questions? Find your answer in our popular FAQs >', 'class': '', 'id': '', 'parent_tag': 'header'}, {'tag': 'a', 'text': 'See All Destinations', 'class': 'btn btn--L btn-sec-inv btn--icon-Left', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'See All Destinations', 'class': 'text-button-l', 'id': '', 'parent_tag': 'a'}, {'tag': 'h2', 'text': 'Tour Differently.', 'class': 'title-section__title text-h2-title-m', 'id': '', 'parent_tag': 'header'}, {'tag': 'p', 'text': 'The ease. The experts. The icons. The locals. The hidden secrets. When it comes to your next vacation, nothing beats Trafalgar.', 'class': '', 'id': '', 'parent_tag': 'header'}, {'tag': 'h3', 'text': 'Must-sees to local secrets', 'class': 'title-section__title text-h2-title-xxs', 'id': '', 'parent_tag': 'header'}, {'tag': 'p', 'text': 'You’ll enjoy the icons and hidden gems with a Local Specialist by your side.', 'class': '', 'id': '', 'parent_tag': 'header'}, {'tag': 'a', 'text': 'Learn More', 'class': 'btn btn--L btn-sec btn--icon-Left', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Learn More', 'class': 'text-button-l', 'id': '', 'parent_tag': 'a'}, {'tag': 'h3', 'text': 'One-of-a-kind experiences', 'class': 'title-section__title text-h2-title-xxs', 'id': '', 'parent_tag': 'header'}, {'tag': 'p', 'text': 'Your exclusive Be My Guest and Stays with Stories experiences.', 'class': '', 'id': '', 'parent_tag': 'header'}, {'tag': 'a', 'text': 'Learn More', 'class': 'btn btn--L btn-sec btn--icon-Left', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Learn More', 'class': 'text-button-l', 'id': '', 'parent_tag': 'a'}, {'tag': 'h3', 'text': 'Everything taken care of', 'class': 'title-section__title text-h2-title-xxs', 'id': '', 'parent_tag': 'header'}, {'tag': 'p', 'text': 'Seamless travel from the moment you book your trip.', 'class': '', 'id': '', 'parent_tag': 'header'}, {'tag': 'a', 'text': 'Learn More', 'class': 'btn btn--L btn-sec btn--icon-Left', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Learn More', 'class': 'text-button-l', 'id': '', 'parent_tag': 'a'}, {'tag': 'h3', 'text': 'Responsible travel', 'class': 'title-section__title text-h2-title-xxs', 'id': '', 'parent_tag': 'header'}, {'tag': 'p', 'text': 'On every tour you’ll experience at least one MAKE TRAVEL MATTER® Experience.', 'class': '', 'id': '', 'parent_tag': 'header'}, {'tag': 'a', 'text': 'Learn More', 'class': 'btn btn--L btn-sec btn--icon-Left', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Learn More', 'class': 'text-button-l', 'id': '', 'parent_tag': 'a'}, {'tag': 'h2', 'text': 'Popular ways to go', 'class': 'title-section__title text-h2-title-s', 'id': '', 'parent_tag': 'header'}, {'tag': 'a', 'text': 'Family Tours', 'class': 'image_grid__card--clickable-link', 'id': '', 'parent_tag': 'div'}, {'tag': 'h2', 'text': 'Family Tours', 'class': 'image-grid__title text-h2-title-s', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': 'Couples Getaways', 'class': 'image_grid__card--clickable-link', 'id': '', 'parent_tag': 'div'}, {'tag': 'h2', 'text': 'Couples Getaways', 'class': 'image-grid__title text-h2-title-s', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': 'Last Minute Tours', 'class': 'image_grid__card--clickable-link', 'id': '', 'parent_tag': 'div'}, {'tag': 'h2', 'text': 'Last Minute Tours', 'class': 'image-grid__title text-h2-title-s', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': 'Single Parent Tours', 'class': 'image_grid__card--clickable-link', 'id': '', 'parent_tag': 'div'}, {'tag': 'h2', 'text': 'Single Parent Tours', 'class': 'image-grid__title text-h2-title-s', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': 'Food Travel', 'class': 'image_grid__card--clickable-link', 'id': '', 'parent_tag': 'div'}, {'tag': 'h2', 'text': 'Food Travel', 'class': 'image-grid__title text-h2-title-s', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': 'Safari Tours', 'class': 'image_grid__card--clickable-link', 'id': '', 'parent_tag': 'div'}, {'tag': 'h2', 'text': 'Safari Tours', 'class': 'image-grid__title text-h2-title-s', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': 'Sustainable Tours', 'class': 'image_grid__card--clickable-link', 'id': '', 'parent_tag': 'div'}, {'tag': 'h2', 'text': 'Sustainable Tours', 'class': 'image-grid__title text-h2-title-s', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': 'Cruise Tours', 'class': 'image_grid__card--clickable-link', 'id': '', 'parent_tag': 'div'}, {'tag': 'h2', 'text': 'Cruise Tours', 'class': 'image-grid__title text-h2-title-s', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': 'View All Ways to Go', 'class': 'btn btn--L image-grid__button btn-sec btn--icon-Left', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'View All Ways to Go', 'class': 'text-button-l', 'id': '', 'parent_tag': 'a'}, {'tag': 'h2', 'text': 'See happy guests traveling now #simplyTrafalgar', 'class': 'title-section__title text-h2-title-s', 'id': '', 'parent_tag': 'header'}, {'tag': 'p', 'text': 'Join us on social for your daily dose of travel inspiration, and see what travelers around the world are up to right now. \u200b', 'class': '', 'id': '', 'parent_tag': 'header'}, {'tag': 'h2', 'text': 'We are the world’s most loved tour company', 'class': 'title-section__title text-h2-title-m', 'id': '', 'parent_tag': 'header'}, {'tag': 'p', 'text': 'Ready to be inspired?', 'class': 'title-section__title text-h2-title-xxs', 'id': '', 'parent_tag': 'header'}, {'tag': 'p', 'text': 'Get your free brochure and plan your next escape.', 'class': '', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': 'Request a Brochure', 'class': 'btn btn--S tile__content-link tile__content-link-position--left btn-pr btn--icon-Left', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Request a Brochure', 'class': 'text-button-s', 'id': '', 'parent_tag': 'a'}, {'tag': 'p', 'text': '5 million happy guests...', 'class': 'title-section__title text-h2-title-xxs', 'id': '', 'parent_tag': 'header'}, {'tag': 'p', 'text': '...and counting. See what our past guests have to say.', 'class': '', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': 'Read Reviews', 'class': 'btn btn--S tile__content-link tile__content-link-position--left btn-pr btn--icon-Left', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Read Reviews', 'class': 'text-button-s', 'id': '', 'parent_tag': 'a'}, {'tag': 'td', 'text': 'Members-only pricing5%* discount on guided tours', 'class': '', 'id': '', 'parent_tag': 'tr'}, {'tag': 'p', 'text': 'Members-only pricing5%* discount on guided tours', 'class': '', 'id': '', 'parent_tag': 'td'}, {'tag': 'span', 'text': 'Members-only pricing', 'class': '', 'id': '', 'parent_tag': 'p'}, {'tag': 'span', 'text': 'Members-only pricing', 'class': '', 'id': '', 'parent_tag': 'strong'}, {'tag': 'span', 'text': '5%* discount on guided tours', 'class': '', 'id': '', 'parent_tag': 'p'}, {'tag': 'td', 'text': "Priority first lookYou're the first to find out about new trips & offers", 'class': '', 'id': '', 'parent_tag': 'tr'}, {'tag': 'p', 'text': "Priority first lookYou're the first to find out about new trips & offers", 'class': '', 'id': '', 'parent_tag': 'td'}, {'tag': 'span', 'text': 'Priority first look', 'class': '', 'id': '', 'parent_tag': 'p'}, {'tag': 'span', 'text': "You're the first to find out about new trips & offers", 'class': '', 'id': '', 'parent_tag': 'p'}, {'tag': 'td', 'text': 'Special recognitionFrom your Travel Director on your next tour', 'class': '', 'id': '', 'parent_tag': 'tr'}, {'tag': 'p', 'text': 'Special recognitionFrom your Travel Director on your next tour', 'class': '', 'id': '', 'parent_tag': 'td'}, {'tag': 'span', 'text': 'Special recognition', 'class': '', 'id': '', 'parent_tag': 'p'}, {'tag': 'span', 'text': 'Special recognition', 'class': '', 'id': '', 'parent_tag': 'strong'}, {'tag': 'span', 'text': 'From your Travel Director on your next tour', 'class': '', 'id': '', 'parent_tag': 'p'}, {'tag': 'td', 'text': 'Access to our portfolio of brandsEnjoy your rewards across the portfolio', 'class': '', 'id': '', 'parent_tag': 'tr'}, {'tag': 'p', 'text': 'Access to our portfolio of brandsEnjoy your rewards across the portfolio', 'class': '', 'id': '', 'parent_tag': 'td'}, {'tag': 'span', 'text': 'Access to our portfolio of brands', 'class': '', 'id': '', 'parent_tag': 'p'}, {'tag': 'span', 'text': 'Enjoy your rewards across the portfolio', 'class': '', 'id': '', 'parent_tag': 'p'}, {'tag': 'td', 'text': 'Referral programGive discounts and get travel credits', 'class': '', 'id': '', 'parent_tag': 'tr'}, {'tag': 'p', 'text': 'Referral programGive discounts and get travel credits', 'class': '', 'id': '', 'parent_tag': 'td'}, {'tag': 'span', 'text': 'Referral programGive discounts and get travel credits', 'class': '', 'id': '', 'parent_tag': 'p'}, {'tag': 'span', 'text': 'Give discounts and get travel credits', 'class': '', 'id': '', 'parent_tag': 'span'}, {'tag': 'a', 'text': 'Find Out More', 'class': 'btn btn--L btn-pr btn--icon-Left', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Find Out More', 'class': 'text-button-l', 'id': '', 'parent_tag': 'a'}, {'tag': 'h2', 'text': 'As seen on', 'class': 'title-section__title text-h2-title-s', 'id': '', 'parent_tag': 'header'}, {'tag': 'p', 'text': 'Help & Info', 'class': 'footer__title footer__title--empty-link text-label-l', 'id': '', 'parent_tag': 'div'}, {'tag': 'li', 'text': 'Who We Are', 'class': '', 'id': '', 'parent_tag': 'ul'}, {'tag': 'a', 'text': 'Who We Are', 'class': 'btn btn--S footer__link btn-ter btn--icon-Left', 'id': '', 'parent_tag': 'li'}, {'tag': 'p', 'text': 'Who We Are', 'class': 'text-paragraph-xs', 'id': '', 'parent_tag': 'a'}, {'tag': 'li', 'text': 'WE MAKE TRAVEL MATTER®', 'class': '', 'id': '', 'parent_tag': 'ul'}, {'tag': 'a', 'text': 'WE MAKE TRAVEL MATTER®', 'class': 'btn btn--S footer__link btn-ter btn--icon-Left', 'id': '', 'parent_tag': 'li'}, {'tag': 'p', 'text': 'WE MAKE TRAVEL MATTER®', 'class': 'text-paragraph-xs', 'id': '', 'parent_tag': 'a'}, {'tag': 'li', 'text': 'Unedited Reviews', 'class': '', 'id': '', 'parent_tag': 'ul'}, {'tag': 'a', 'text': 'Unedited Reviews', 'class': 'btn btn--S footer__link btn-ter btn--icon-Left', 'id': '', 'parent_tag': 'li'}, {'tag': 'p', 'text': 'Unedited Reviews', 'class': 'text-paragraph-xs', 'id': '', 'parent_tag': 'a'}, {'tag': 'li', 'text': 'Affiliates Hub', 'class': '', 'id': '', 'parent_tag': 'ul'}, {'tag': 'a', 'text': 'Affiliates Hub', 'class': 'btn btn--S footer__link btn-ter btn--icon-Left', 'id': '', 'parent_tag': 'li'}, {'tag': 'p', 'text': 'Affiliates Hub', 'class': 'text-paragraph-xs', 'id': '', 'parent_tag': 'a'}, {'tag': 'li', 'text': 'Our Destination Management Companies', 'class': '', 'id': '', 'parent_tag': 'ul'}, {'tag': 'a', 'text': 'Our Destination Management Companies', 'class': 'btn btn--S footer__link btn-ter btn--icon-Left', 'id': '', 'parent_tag': 'li'}, {'tag': 'p', 'text': 'Our Destination Management Companies', 'class': 'text-paragraph-xs', 'id': '', 'parent_tag': 'a'}, {'tag': 'li', 'text': 'Frequently Asked Questions', 'class': '', 'id': '', 'parent_tag': 'ul'}, {'tag': 'a', 'text': 'Frequently Asked Questions', 'class': 'btn btn--S footer__link btn-ter btn--icon-Left', 'id': '', 'parent_tag': 'li'}, {'tag': 'p', 'text': 'Frequently Asked Questions', 'class': 'text-paragraph-xs', 'id': '', 'parent_tag': 'a'}, {'tag': 'li', 'text': 'Travel Updates', 'class': '', 'id': '', 'parent_tag': 'ul'}, {'tag': 'a', 'text': 'Travel Updates', 'class': 'btn btn--S footer__link btn-ter btn--icon-Left', 'id': '', 'parent_tag': 'li'}, {'tag': 'p', 'text': 'Travel Updates', 'class': 'text-paragraph-xs', 'id': '', 'parent_tag': 'a'}, {'tag': 'li', 'text': 'Contact Us', 'class': '', 'id': '', 'parent_tag': 'ul'}, {'tag': 'a', 'text': 'Contact Us', 'class': 'btn btn--S footer__link btn-ter btn--icon-Left', 'id': '', 'parent_tag': 'li'}, {'tag': 'p', 'text': 'Contact Us', 'class': 'text-paragraph-xs', 'id': '', 'parent_tag': 'a'}, {'tag': 'p', 'text': 'Travel Planning', 'class': 'footer__title footer__title--empty-link text-label-l', 'id': '', 'parent_tag': 'div'}, {'tag': 'li', 'text': 'Get Your Free Brochure', 'class': '', 'id': '', 'parent_tag': 'ul'}, {'tag': 'a', 'text': 'Get Your Free Brochure', 'class': 'btn btn--S footer__link btn-ter btn--icon-Left', 'id': '', 'parent_tag': 'li'}, {'tag': 'p', 'text': 'Get Your Free Brochure', 'class': 'text-paragraph-xs', 'id': '', 'parent_tag': 'a'}, {'tag': 'li', 'text': 'Travel Insurance', 'class': '', 'id': '', 'parent_tag': 'ul'}, {'tag': 'a', 'text': 'Travel Insurance', 'class': 'btn btn--S footer__link btn-ter btn--icon-Left', 'id': '', 'parent_tag': 'li'}, {'tag': 'p', 'text': 'Travel Insurance', 'class': 'text-paragraph-xs', 'id': '', 'parent_tag': 'a'}, {'tag': 'li', 'text': 'Booking Conditions', 'class': '', 'id': '', 'parent_tag': 'ul'}, {'tag': 'a', 'text': 'Booking Conditions', 'class': 'btn btn--S footer__link btn-ter btn--icon-Left', 'id': '', 'parent_tag': 'li'}, {'tag': 'p', 'text': 'Booking Conditions', 'class': 'text-paragraph-xs', 'id': '', 'parent_tag': 'a'}, {'tag': 'li', 'text': 'Trip Deposit Level', 'class': '', 'id': '', 'parent_tag': 'ul'}, {'tag': 'a', 'text': 'Trip Deposit Level', 'class': 'btn btn--S footer__link btn-ter btn--icon-Left', 'id': '', 'parent_tag': 'li'}, {'tag': 'p', 'text': 'Trip Deposit Level', 'class': 'text-paragraph-xs', 'id': '', 'parent_tag': 'a'}, {'tag': 'li', 'text': 'Recommendations', 'class': '', 'id': '', 'parent_tag': 'ul'}, {'tag': 'a', 'text': 'Recommendations', 'class': 'btn btn--S footer__link btn-ter btn--icon-Left', 'id': '', 'parent_tag': 'li'}, {'tag': 'p', 'text': 'Recommendations', 'class': 'text-paragraph-xs', 'id': '', 'parent_tag': 'a'}, {'tag': 'p', 'text': 'Trafalgar Tours Limited is a proud member ofThe Travel Corporationportfolio of brands..', 'class': '', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': 'The Travel Corporation', 'class': '', 'id': '', 'parent_tag': 'p'}, {'tag': 'p', 'text': '#SimplyTrafalgar', 'class': '', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Travel House, Rue du Manoir St Peter Port, Guernsey, GY1 2JH', 'class': '', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Selected Region', 'class': 'regional-selector__row-region text-paragraph-xs', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'United States', 'class': 'text-label-m', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Selected Region', 'class': 'regional-selector-mobile__region-title text-paragraph-xs', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'United States', 'class': 'regional-selector-mobile__row__selected text-label-m', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': 'United Kingdom', 'class': '', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'United Kingdom', 'class': 'regional-selector-mobile__links__text text-default', 'id': '', 'parent_tag': 'a'}, {'tag': 'a', 'text': 'Australia', 'class': '', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Australia', 'class': 'regional-selector-mobile__links__text text-default', 'id': '', 'parent_tag': 'a'}, {'tag': 'a', 'text': 'New Zealand', 'class': '', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'New Zealand', 'class': 'regional-selector-mobile__links__text text-default', 'id': '', 'parent_tag': 'a'}, {'tag': 'a', 'text': 'South Africa', 'class': '', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'South Africa', 'class': 'regional-selector-mobile__links__text text-default', 'id': '', 'parent_tag': 'a'}, {'tag': 'p', 'text': 'Copyright 2025 Trafalgar. All rights reserved.MAKE TRAVEL MATTER® is a trademark of The TreadRight Foundation, registered in the U.S. and other countries and regions, and is being used under license.', 'class': '', 'id': '', 'parent_tag': 'div'}, {'tag': 'a', 'text': 'Terms and Conditions', 'class': 'btn btn--L sub-links__link btn-ter btn--icon-Left', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Terms and Conditions', 'class': 'text-link-xs', 'id': '', 'parent_tag': 'a'}, {'tag': 'a', 'text': 'Booking Conditions', 'class': 'btn btn--L sub-links__link btn-ter btn--icon-Left', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Booking Conditions', 'class': 'text-link-xs', 'id': '', 'parent_tag': 'a'}, {'tag': 'a', 'text': 'Privacy Policy', 'class': 'btn btn--L sub-links__link btn-ter btn--icon-Left', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Privacy Policy', 'class': 'text-link-xs', 'id': '', 'parent_tag': 'a'}, {'tag': 'a', 'text': 'Cookie Policy', 'class': 'btn btn--L sub-links__link btn-ter btn--icon-Left', 'id': '', 'parent_tag': 'div'}, {'tag': 'p', 'text': 'Cookie Policy', 'class': 'text-link-xs', 'id': '', 'parent_tag': 'a'}]

    contact_extractor = ContactExtractor()
    ai_lead_info = _analyze_sections_for_client_info(sections, contact_extractor)
    print(ai_lead_info)

if __name__ == "__main__":
    main()