"""
Enhanced Contact Extraction Strategies

This module implements advanced techniques to extract contact information
from business directories that typically hide or protect contact details.
"""

import re
import asyncio
from typing import Dict, List, Any, Optional
from loguru import logger


class ContactExtractionStrategy:
    """Advanced contact extraction techniques for business directories."""
    
    def __init__(self, page):
        self.page = page
        
    async def extract_all_contact_methods(self, company_data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply multiple contact extraction strategies."""
        
        # 1. HTML Source Analysis
        contact_info = await self._extract_from_html_source()
        
        # 2. Network Traffic Analysis
        network_contacts = await self._extract_from_network_requests()
        
        # 3. CSS Hidden Elements
        hidden_contacts = await self._extract_from_hidden_elements()
        
        # 4. Meta Tags and Structured Data
        meta_contacts = await self._extract_from_meta_tags()
        
        # 5. Image OCR (for contact images)
        image_contacts = await self._extract_from_contact_images()
        
        # Combine all sources
        combined_contacts = self._merge_contact_sources(
            contact_info, network_contacts, hidden_contacts, 
            meta_contacts, image_contacts
        )
        
        # Enhance original company data
        return self._enhance_company_data(company_data, combined_contacts)
    
    async def _extract_from_html_source(self) -> Dict[str, List[str]]:
        """Extract contacts from raw HTML source including comments and scripts."""
        try:
            html_content = await self.page.content()
            
            contacts = {"emails": [], "phones": [], "websites": []}
            
            # Advanced email patterns
            email_patterns = [
                r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
                r'mailto:([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})',
                r'email["\']?\s*:\s*["\']?([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})',
            ]
            
            # Advanced phone patterns
            phone_patterns = [
                r'\+?\d{1,3}[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,9}',
                r'tel:(\+?\d[\d\s\-\(\)]+)',
                r'phone["\']?\s*:\s*["\']?(\+?\d[\d\s\-\(\)]+)',
                r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',
            ]
            
            # Website patterns
            website_patterns = [
                r'https?://(?:[-\w.])+(?:[:\d]+)?(?:/(?:[\w/_.])*)?',
                r'www\.(?:[-\w.])+(?:[:\d]+)?(?:/(?:[\w/_.])*)?',
            ]
            
            # Extract from HTML source
            for pattern in email_patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE)
                contacts["emails"].extend(matches)
            
            for pattern in phone_patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE)
                contacts["phones"].extend(matches)
                
            for pattern in website_patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE)
                contacts["websites"].extend(matches)
            
            # Clean and deduplicate
            contacts["emails"] = list(set([email.strip() for email in contacts["emails"] if self._validate_email(email)]))
            contacts["phones"] = list(set([phone.strip() for phone in contacts["phones"] if self._validate_phone(phone)]))
            contacts["websites"] = list(set([website.strip() for website in contacts["websites"] if self._validate_website(website)]))
            
            return contacts
            
        except Exception as e:
            logger.warning(f"Error extracting from HTML source: {e}")
            return {"emails": [], "phones": [], "websites": []}
    
    async def _extract_from_network_requests(self) -> Dict[str, List[str]]:
        """Monitor network requests for contact information in API calls."""
        try:
            # This would require implementing network interception
            # For now, return empty but structure is ready for implementation
            return {"emails": [], "phones": [], "websites": []}
        except Exception as e:
            logger.warning(f"Error extracting from network requests: {e}")
            return {"emails": [], "phones": [], "websites": []}
    
    async def _extract_from_hidden_elements(self) -> Dict[str, List[str]]:
        """Extract contact info from CSS hidden or display:none elements."""
        try:
            contacts = {"emails": [], "phones": [], "websites": []}
            
            # Check for hidden contact elements
            hidden_selectors = [
                '[style*="display:none"]',
                '[style*="visibility:hidden"]',
                '.hidden',
                '.contact-hidden',
                '[data-contact]',
                '[data-email]',
                '[data-phone]'
            ]
            
            for selector in hidden_selectors:
                elements = await self.page.query_selector_all(selector)
                for element in elements:
                    text = await element.text_content()
                    if text:
                        # Extract emails
                        emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
                        contacts["emails"].extend(emails)
                        
                        # Extract phones
                        phones = re.findall(r'\+?\d{1,3}[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,9}', text)
                        contacts["phones"].extend(phones)
            
            return contacts
            
        except Exception as e:
            logger.warning(f"Error extracting from hidden elements: {e}")
            return {"emails": [], "phones": [], "websites": []}
    
    async def _extract_from_meta_tags(self) -> Dict[str, List[str]]:
        """Extract contact info from meta tags and structured data."""
        try:
            contacts = {"emails": [], "phones": [], "websites": []}
            
            # Check meta tags
            meta_tags = await self.page.query_selector_all('meta')
            for meta in meta_tags:
                content = await meta.get_attribute('content')
                if content:
                    emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', content)
                    contacts["emails"].extend(emails)
            
            return contacts
            
        except Exception as e:
            logger.warning(f"Error extracting from meta tags: {e}")
            return {"emails": [], "phones": [], "websites": []}
    
    async def _extract_from_contact_images(self) -> Dict[str, List[str]]:
        """Extract contact info from images (would require OCR)."""
        try:
            # This would require OCR implementation
            # Placeholder for future OCR-based contact extraction
            return {"emails": [], "phones": [], "websites": []}
        except Exception as e:
            logger.warning(f"Error extracting from images: {e}")
            return {"emails": [], "phones": [], "websites": []}
    
    def _merge_contact_sources(self, *sources) -> Dict[str, List[str]]:
        """Merge contact information from multiple sources."""
        merged = {"emails": [], "phones": [], "websites": []}
        
        for source in sources:
            if isinstance(source, dict):
                merged["emails"].extend(source.get("emails", []))
                merged["phones"].extend(source.get("phones", []))
                merged["websites"].extend(source.get("websites", []))
        
        # Deduplicate
        merged["emails"] = list(set(merged["emails"]))
        merged["phones"] = list(set(merged["phones"]))
        merged["websites"] = list(set(merged["websites"]))
        
        return merged
    
    def _enhance_company_data(self, company_data: Dict[str, Any], contacts: Dict[str, List[str]]) -> Dict[str, Any]:
        """Enhance company data with extracted contact information."""
        enhanced_data = company_data.copy()
        
        # Add emails
        if contacts["emails"]:
            if "email" not in enhanced_data:
                enhanced_data["email"] = []
            elif isinstance(enhanced_data["email"], str):
                enhanced_data["email"] = [enhanced_data["email"]]
            enhanced_data["email"].extend(contacts["emails"])
            enhanced_data["email"] = list(set(enhanced_data["email"]))
        
        # Add phones
        if contacts["phones"]:
            if "phone" not in enhanced_data:
                enhanced_data["phone"] = []
            elif isinstance(enhanced_data["phone"], str):
                enhanced_data["phone"] = [enhanced_data["phone"]]
            enhanced_data["phone"].extend(contacts["phones"])
            enhanced_data["phone"] = list(set(enhanced_data["phone"]))
        
        # Add websites
        if contacts["websites"]:
            if "websites" not in enhanced_data:
                enhanced_data["websites"] = []
            enhanced_data["websites"].extend(contacts["websites"])
            enhanced_data["websites"] = list(set(enhanced_data["websites"]))
        
        return enhanced_data
    
    def _validate_email(self, email: str) -> bool:
        """Validate email format and filter out false positives."""
        if not email or len(email) < 5 or len(email) > 100:
            return False

        # Basic email pattern
        pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$'
        if not re.match(pattern, email):
            return False

        # Filter out obvious false positives
        email_lower = email.lower()

        # Exclude file extensions (images, CSS, etc.)
        if any(ext in email_lower for ext in ['.png', '.jpg', '.jpeg', '.gif', '.css', '.js', '.ico', '.svg', '.webp']):
            return False

        # Exclude CSS-like patterns (numbers at start, @2x, etc.)
        if re.search(r'^\d|@2x|@3x|logo|icon|image|img|photo|pic|asset', email_lower):
            return False

        # Exclude emails with spaces or unusual characters
        if ' ' in email or any(char in email for char in ['<', '>', '{', '}', '[', ']', '|', '\\', '^']):
            return False

        # Exclude emails that look like timestamps or IDs
        local_part = email.split('@')[0]
        if re.match(r'^\d{10,}$', local_part):  # Long numeric local parts
            return False

        # Exclude emails with suspicious patterns
        if re.search(r'\d{8,}', email):  # 8+ consecutive digits
            return False

        # Exclude emails that are clearly not business emails
        suspicious_domains = ['example.com', 'test.com', 'sample.com', 'domain.com', 'email.com']
        domain = email.split('@')[1].lower()
        if domain in suspicious_domains:
            return False

        return True

    def _validate_phone(self, phone: str) -> bool:
        """Validate phone number and filter out false positives."""
        if not phone:
            return False

        # Remove common formatting but keep some structure
        cleaned = re.sub(r'[^\d+\-\(\)\.\s]', '', phone)

        # Filter out CSS measurements (contain dots and spaces)
        if '.' in cleaned and ' ' in cleaned:
            return False

        # Remove all non-digits for length check
        digits_only = re.sub(r'[^\d]', '', phone)

        # Should have 7-15 digits (reasonable phone number range)
        if not (7 <= len(digits_only) <= 15):
            return False

        # Filter out timestamps (13-digit numbers starting with 1, like Unix timestamps)
        if len(digits_only) == 13 and digits_only.startswith('1'):
            return False

        # Filter out obvious timestamps (10-digit numbers that look like Unix timestamps)
        if len(digits_only) == 10 and digits_only.startswith(('1', '2')):
            # Additional check: if it looks like a timestamp (reasonable date range)
            try:
                timestamp = int(digits_only)
                # Unix timestamps from 2000-2050 are roughly 946684800 to 2524608000
                if 946684800 <= timestamp <= 2524608000:
                    return False
            except:
                pass

        # Should not be all the same digit (like CSS measurements)
        if len(set(digits_only)) == 1 and len(digits_only) > 3:
            return False

        # Should not be simple sequential patterns
        if len(digits_only) >= 6:
            # Check if it's a simple pattern like 123456, 987654, etc.
            for i in range(len(digits_only) - 2):
                if (int(digits_only[i+1]) == int(digits_only[i]) + 1 and
                    int(digits_only[i+2]) == int(digits_only[i+1]) + 1):
                    # Allow some sequential but not long sequences
                    if len(digits_only) >= 8:
                        return False

        # Should have some formatting (not just digits)
        if len(phone) == len(digits_only) and len(digits_only) > 10:
            # Long numbers without formatting are suspicious
            return False

        # Filter out numbers that start with 0 (unless international)
        if not phone.startswith('+') and digits_only.startswith('0') and len(digits_only) > 7:
            return False

        return True
    
    def _validate_website(self, website: str) -> bool:
        """Validate website URL."""
        if not website:
            return False
        return any(website.startswith(prefix) for prefix in ['http://', 'https://', 'www.'])