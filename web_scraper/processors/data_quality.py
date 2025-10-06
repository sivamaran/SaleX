from __future__ import annotations

import re
import json
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urlparse
from datetime import datetime, timezone
import math
from loguru import logger

try:
    from fuzzywuzzy import fuzz  # type: ignore
    _HAS_FUZZYWUZZY = True
except ImportError:
    _HAS_FUZZYWUZZY = False


class LeadDeduplicator:
    """Multi-level deduplication for lead data as specified in Phase 6.1."""
    
    def __init__(self, similarity_threshold: float = 0.85):
        self.similarity_threshold = similarity_threshold
        
    def deduplicate_leads(self, leads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Perform multi-level deduplication on lead data."""
        logger.info(f"Starting deduplication of {len(leads)} leads")
        
        if not leads:
            return []
        
        # Step 1: Exact match deduplication
        unique_leads = self._exact_match_deduplication(leads)
        logger.info(f"After exact match: {len(unique_leads)} leads")
        
        # Step 2: Fuzzy match deduplication
        unique_leads = self._fuzzy_match_deduplication(unique_leads)
        logger.info(f"After fuzzy match: {len(unique_leads)} leads")
        
        # Step 3: Cross-reference deduplication
        unique_leads = self._cross_reference_deduplication(unique_leads)
        logger.info(f"After cross-reference: {len(unique_leads)} leads")
        
        return unique_leads
    
    def _exact_match_deduplication(self, leads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove exact duplicates based on email, phone, website URL."""
        seen_keys = set()
        unique_leads = []
        
        for lead in leads:
            # Create composite key from primary identifiers
            key_parts = []
            
            # Email key
            emails = lead.get('contact_information', {}).get('emails', [])
            if emails:
                primary_email = emails[0].get('value', '').lower().strip()
                if primary_email:
                    key_parts.append(f"email:{primary_email}")
            
            # Phone key
            phones = lead.get('contact_information', {}).get('phones', [])
            if phones:
                primary_phone = phones[0].get('clean_value', '').strip()
                if primary_phone:
                    key_parts.append(f"phone:{primary_phone}")
            
            # Website key
            websites = lead.get('contact_information', {}).get('websites', [])
            if websites:
                primary_website = websites[0].get('domain', '').lower().strip()
                if primary_website:
                    key_parts.append(f"website:{primary_website}")
            
            # Create composite key
            if key_parts:
                composite_key = "|".join(sorted(key_parts))
                if composite_key not in seen_keys:
                    seen_keys.add(composite_key)
                    unique_leads.append(lead)
                else:
                    # Merge with existing lead using confidence-based selection
                    existing_idx = self._find_lead_index_by_key(unique_leads, composite_key)
                    if existing_idx is not None:
                        merged_lead = self._merge_leads(unique_leads[existing_idx], lead)
                        unique_leads[existing_idx] = merged_lead
            else:
                # No primary identifiers, keep as unique
                unique_leads.append(lead)
        
        return unique_leads
    
    def _fuzzy_match_deduplication(self, leads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove fuzzy duplicates based on company names (85% similarity)."""
        if not _HAS_FUZZYWUZZY:
            logger.warning("fuzzywuzzy not available, skipping fuzzy deduplication")
            return leads
        
        unique_leads = []
        processed_indices = set()
        
        for i, lead in enumerate(leads):
            if i in processed_indices:
                continue
            
            company_name = lead.get('business_information', {}).get('company_name', '').strip()
            if not company_name or len(company_name) < 3:
                unique_leads.append(lead)
                continue
            
            # Find similar company names
            similar_leads = [lead]
            similar_indices = {i}
            
            for j, other_lead in enumerate(leads[i+1:], i+1):
                if j in processed_indices:
                    continue
                
                other_company = other_lead.get('business_information', {}).get('company_name', '').strip()
                if not other_company:
                    continue
                
                # Calculate similarity
                similarity = fuzz.ratio(company_name.lower(), other_company.lower()) / 100.0
                
                if similarity >= self.similarity_threshold:
                    similar_leads.append(other_lead)
                    similar_indices.add(j)
            
            # Merge similar leads
            if len(similar_leads) > 1:
                merged_lead = self._merge_multiple_leads(similar_leads)
                unique_leads.append(merged_lead)
            else:
                unique_leads.append(lead)
            
            processed_indices.update(similar_indices)
        
        return unique_leads
    
    def _cross_reference_deduplication(self, leads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicates based on address normalization and matching."""
        unique_leads = []
        processed_indices = set()
        
        for i, lead in enumerate(leads):
            if i in processed_indices:
                continue
            
            addresses = lead.get('contact_information', {}).get('addresses', [])
            if not addresses:
                unique_leads.append(lead)
                continue
            
            normalized_address = self._normalize_address(addresses[0].get('value', ''))
            if not normalized_address:
                unique_leads.append(lead)
                continue
            
            # Find leads with similar addresses
            similar_leads = [lead]
            similar_indices = {i}
            
            for j, other_lead in enumerate(leads[i+1:], i+1):
                if j in processed_indices:
                    continue
                
                other_addresses = other_lead.get('contact_information', {}).get('addresses', [])
                if not other_addresses:
                    continue
                
                other_normalized = self._normalize_address(other_addresses[0].get('value', ''))
                if not other_normalized:
                    continue
                
                # Check address similarity
                if self._addresses_similar(normalized_address, other_normalized):
                    similar_leads.append(other_lead)
                    similar_indices.add(j)
            
            # Merge similar leads
            if len(similar_leads) > 1:
                merged_lead = self._merge_multiple_leads(similar_leads)
                unique_leads.append(merged_lead)
            else:
                unique_leads.append(lead)
            
            processed_indices.update(similar_indices)
        
        return unique_leads
    
    def _normalize_address(self, address: str) -> str:
        """Normalize address for comparison."""
        if not address:
            return ""
        
        # Convert to lowercase and remove extra whitespace
        normalized = re.sub(r'\s+', ' ', address.lower().strip())
        
        # Standardize common abbreviations
        abbreviations = {
            r'\bstreet\b': 'st', r'\bavenue\b': 'ave', r'\broad\b': 'rd',
            r'\bboulevard\b': 'blvd', r'\blane\b': 'ln', r'\bdrive\b': 'dr',
            r'\bcourt\b': 'ct', r'\bplace\b': 'pl', r'\bapartment\b': 'apt'
        }
        
        for full_form, abbrev in abbreviations.items():
            normalized = re.sub(full_form, abbrev, normalized)
        
        # Remove punctuation except hyphens in ZIP codes
        normalized = re.sub(r'[^\w\s\-]', '', normalized)
        
        return normalized
    
    def _addresses_similar(self, addr1: str, addr2: str) -> bool:
        """Check if two normalized addresses are similar."""
        if not addr1 or not addr2:
            return False
        
        # Extract street numbers
        num1 = re.search(r'^\d+', addr1)
        num2 = re.search(r'^\d+', addr2)
        
        # If different street numbers, not similar
        if num1 and num2 and num1.group() != num2.group():
            return False
        
        # Check overall similarity
        if _HAS_FUZZYWUZZY:
            similarity = fuzz.ratio(addr1, addr2) / 100.0
            return similarity >= 0.8
        else:
            return addr1 == addr2
    
    def _merge_leads(self, lead1: Dict[str, Any], lead2: Dict[str, Any]) -> Dict[str, Any]:
        """Merge two leads using confidence-based field selection."""
        merged = {}
        
        # Merge contact information
        merged['contact_information'] = self._merge_contact_info(
            lead1.get('contact_information', {}),
            lead2.get('contact_information', {})
        )
        
        # Merge business information - data completeness prioritization
        merged['business_information'] = self._merge_business_info(
            lead1.get('business_information', {}),
            lead2.get('business_information', {})
        )
        
        # Merge intent indicators
        intent1 = set(lead1.get('intent_indicators', []))
        intent2 = set(lead2.get('intent_indicators', []))
        merged['intent_indicators'] = list(intent1.union(intent2))
        
        # Use higher lead score - source credibility weighting
        score1 = lead1.get('lead_score', {}).get('total_score', 0)
        score2 = lead2.get('lead_score', {}).get('total_score', 0)
        merged['lead_score'] = lead1.get('lead_score', {}) if score1 >= score2 else lead2.get('lead_score', {})
        
        # Merge extraction metadata
        merged['extraction_metadata'] = self._merge_metadata(
            lead1.get('extraction_metadata', {}),
            lead2.get('extraction_metadata', {})
        )
        
        return merged
    
    def _merge_multiple_leads(self, leads: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Merge multiple leads."""
        if not leads:
            return {}
        
        result = leads[0]
        for lead in leads[1:]:
            result = self._merge_leads(result, lead)
        
        return result
    
    def _merge_contact_info(self, contact1: Dict[str, Any], contact2: Dict[str, Any]) -> Dict[str, Any]:
        """Merge contact information with deduplication."""
        merged = {}
        
        # Merge emails
        emails1 = contact1.get('emails', [])
        emails2 = contact2.get('emails', [])
        merged['emails'] = self._merge_contact_list(emails1, emails2, 'value')
        
        # Merge phones
        phones1 = contact1.get('phones', [])
        phones2 = contact2.get('phones', [])
        merged['phones'] = self._merge_contact_list(phones1, phones2, 'clean_value')
        
        # Merge addresses
        addresses1 = contact1.get('addresses', [])
        addresses2 = contact2.get('addresses', [])
        merged['addresses'] = self._merge_contact_list(addresses1, addresses2, 'value')
        
        # Merge websites
        websites1 = contact1.get('websites', [])
        websites2 = contact2.get('websites', [])
        merged['websites'] = self._merge_contact_list(websites1, websites2, 'domain')
        
        # Merge social media
        social1 = contact1.get('social_media', [])
        social2 = contact2.get('social_media', [])
        merged['social_media'] = self._merge_contact_list(social1, social2, 'url')
        
        return merged
    
    def _merge_contact_list(self, list1: List[Dict[str, Any]], list2: List[Dict[str, Any]], key: str) -> List[Dict[str, Any]]:
        """Merge two contact lists, removing duplicates."""
        seen_values = set()
        merged = []
        
        # Add items from both lists, preferring higher confidence
        all_items = sorted(list1 + list2, key=lambda x: x.get('confidence', 0), reverse=True)
        
        for item in all_items:
            value = item.get(key, '').lower().strip()

            # Normalize based on field
            if key == 'value' and '@' in value:  # email
                value = value.lower()
            elif key == 'clean_value':  # phone
                digits = re.sub(r'\D', '', value)
                if not (7 <= len(digits) <= 15 and len(set(digits)) > 1):
                    continue  # skip invalid phones
                value = digits
            elif key == 'domain':  # website
                value = value.lower()
            elif key == 'value':
                value = self._normalize_address(item.get(key, ''))

            if value and value not in seen_values:
                seen_values.add(value)
                merged.append(item)
        
        return merged
    
    def _merge_business_info(self, business1: Dict[str, Any], business2: Dict[str, Any]) -> Dict[str, Any]:
        """Merge business information with data completeness prioritization."""
        merged = {}
        
        # Use non-empty values, preferring first lead
        fields = ['company_name', 'industry', 'size_estimate', 'travel_relevance']
        for field in fields:
            value1 = business1.get(field)
            value2 = business2.get(field)
            merged[field] = value1 if value1 else value2
        
        # Merge services
        services1 = set(business1.get('services', []))
        services2 = set(business2.get('services', []))
        merged['services'] = list(services1.union(services2))
        
        # Merge decision makers
        dm1 = business1.get('decision_makers', [])
        dm2 = business2.get('decision_makers', [])
        merged['decision_makers'] = self._merge_decision_makers(dm1, dm2)
        
        return merged
    
    def _merge_decision_makers(self, dm1: List[Dict[str, Any]], dm2: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Merge decision makers, removing duplicates by name."""
        seen_names = set()
        merged = []
        
        all_dms = sorted(dm1 + dm2, key=lambda x: x.get('authority_score', 0), reverse=True)
        
        for dm in all_dms:
            name = dm.get('name', '').lower().strip()
            if name and name not in seen_names:
                seen_names.add(name)
                merged.append(dm)
        
        return merged
    
    def _merge_metadata(self, meta1: Dict[str, Any], meta2: Dict[str, Any]) -> Dict[str, Any]:
        """Merge extraction metadata."""
        merged = {}
        
        # Use higher data confidence
        conf1 = meta1.get('data_confidence', 0)
        conf2 = meta2.get('data_confidence', 0)
        merged['data_confidence'] = max(conf1, conf2)
        
        # Use more recent timestamp
        ts1 = meta1.get('extraction_timestamp', '')
        ts2 = meta2.get('extraction_timestamp', '')
        merged['extraction_timestamp'] = max(ts1, ts2) if ts1 and ts2 else (ts1 or ts2)
        
        # Combine URLs
        urls = []
        if meta1.get('url'):
            urls.append(meta1['url'])
        if meta2.get('url') and meta2['url'] not in urls:
            urls.append(meta2['url'])
        merged['url'] = urls[0] if len(urls) == 1 else urls
        
        return merged
    
    def _find_lead_index_by_key(self, leads: List[Dict[str, Any]], key: str) -> Optional[int]:
        """Find lead index that matches the given composite key."""
        for i, lead in enumerate(leads):
            if self._leads_match_key(lead, key):
                return i
        return None
    
    def _leads_match_key(self, lead: Dict[str, Any], key: str) -> bool:
        """Check if lead matches the composite key."""
        # Reconstruct key from lead
        key_parts = []
        
        emails = lead.get('contact_information', {}).get('emails', [])
        if emails:
            primary_email = emails[0].get('value', '').lower().strip()
            if primary_email:
                key_parts.append(f"email:{primary_email}")
        
        phones = lead.get('contact_information', {}).get('phones', [])
        if phones:
            primary_phone = phones[0].get('clean_value', '').strip()
            if primary_phone:
                key_parts.append(f"phone:{primary_phone}")
        
        websites = lead.get('contact_information', {}).get('websites', [])
        if websites:
            primary_website = websites[0].get('domain', '').lower().strip()
            if primary_website:
                key_parts.append(f"website:{primary_website}")
        
        lead_key = "|".join(sorted(key_parts))
        return lead_key == key


def process_leads_with_quality_engine(leads: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Process leads through the complete data quality engine."""
    logger.info("Starting data quality processing")
    
    # Initialize components
    deduplicator = LeadDeduplicator()
    validator = DataValidator()
    scorer = QualityScorer()
    
    # Step 1: Deduplication
    unique_leads = deduplicator.deduplicate_leads(leads)
    
    # Step 2: Validation and Quality Scoring
    processed_leads = []
    for lead in unique_leads:
        validation_results = validator.validate_lead(lead)
        quality_score = scorer.calculate_quality_score(lead, validation_results)
        
        # Add quality metadata to lead
        lead['data_quality'] = {
            'validation_results': validation_results,
            'quality_score': quality_score
        }
        
        processed_leads.append(lead)
    
    # Sort by quality score
    processed_leads.sort(key=lambda x: x['data_quality']['quality_score']['total_score'], reverse=True)
    
    return {
        'processed_leads': processed_leads,
        'summary': {
            'original_count': len(leads),
            'deduplicated_count': len(unique_leads),
            'final_count': len(processed_leads),
            'duplicates_removed': len(leads) - len(unique_leads),
            'average_quality_score': sum(lead['data_quality']['quality_score']['total_score'] 
                                       for lead in processed_leads) / len(processed_leads) if processed_leads else 0
        }
    }


class DataValidator:
    """Data validation pipeline as specified in Phase 6.2."""
    
    def validate_lead(self, lead: Dict[str, Any]) -> Dict[str, Any]:
        """Validate a single lead and return validation results."""
        validation_results = {
            "business_validation": {},
            "contact_validation": {},
            "overall_valid": True,
            "validation_errors": [],
            "validation_warnings": [],
        }

        # --- Business info check ---
        business_info = lead.get("business_information", {})
        biz_result = self._validate_business_info(business_info)
        validation_results["business_validation"] = biz_result

        # If there are errors from business validation, keep them as warnings (soft)
        if biz_result.get("errors"):
            validation_results["validation_warnings"].extend(biz_result["errors"])
        if biz_result.get("warnings"):
            validation_results["validation_warnings"].extend(biz_result["warnings"])

        # --- Contact info check ---
        contact_info = lead.get("contact_information", {})
        contact_result = self._validate_contact_info(contact_info)
        validation_results["contact_validation"] = contact_result

        if contact_result.get("errors"):
            validation_results["validation_errors"].extend(contact_result["errors"])
            validation_results["overall_valid"] = False
        if contact_result.get("warnings"):
            validation_results["validation_warnings"].extend(contact_result["warnings"])

        # --- Require at least one usable contact channel ---
        has_phone = bool(contact_info.get("phones"))
        has_email = bool(contact_info.get("emails"))
        has_website = bool(contact_info.get("websites"))

        if not (has_phone or has_email or has_website):
            validation_results["overall_valid"] = False
            validation_results["validation_errors"].append("No contact method available")

        # --- Decide final validity ---
        # If only business name missing, treat as still valid but penalized elsewhere
        if (
            not has_phone
            and not has_email
            and not has_website
        ):
            validation_results["overall_valid"] = False

        return validation_results
    
    def _validate_contact_info(self, contact_info: Dict[str, Any]) -> Dict[str, Any]:
        """Validate contact information."""
        results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        # Email syntax and domain validation
        emails = contact_info.get('emails', [])
        for email in emails:
            if not self._validate_email(email.get('value', '')):
                results['errors'].append(f"Invalid email: {email.get('value', '')}")
        
        # Phone number format validation
        phones = contact_info.get('phones', [])
        valid_phones = []
        for phone in phones:
            if self._validate_phone(phone.get('value', '')):
                valid_phones.append(phone)
            else:
                results['warnings'].append(f"Invalid phone removed: {phone.get('value', '')}")
        contact_info['phones'] = valid_phones  # drop invalid phones
        
        # Set overall validity
        results['valid'] = len(results['errors']) == 0
        
        return results
    
    def _validate_email(self, email: str) -> bool:
        """Email syntax and domain validation."""
        if not email:
            return False
        
        # Basic syntax validation
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(email_pattern, email))
    
    def _validate_phone(self, phone: str) -> bool:
        """Phone number format validation."""
        if not phone:
            return False
        
        # Extract digits
        digits = re.sub(r'\D', '', phone)
        
        # Check length and patterns
        return 7 <= len(digits) <= 15 and len(set(digits)) > 1
    
    def _validate_business_info(self, business_info: Dict[str, Any]) -> Dict[str, Any]:
        """Business validation."""
        results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        # Company name validation
        company_name = business_info.get('company_name', '').strip()
        if not company_name or company_name.lower() in ['unknown', 'n/a', 'none']:
            results['warnings'].append('Missing or invalid company name')
        
        # Industry validation
        industry = business_info.get('industry', '').strip()
        if not industry or industry == 'general':
            results['warnings'].append('Industry not classified')
        
        # Set overall validity
        results['valid'] = True
        
        return results


class QualityScorer:
    """Quality scoring system as specified in Phase 6.3."""
    
    def calculate_quality_score(self, lead: Dict[str, Any], validation_results: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate comprehensive quality score."""
        
        # Component scores (0-1 scale)
        completeness_score = self._calculate_completeness_score(lead)
        accuracy_score = self._calculate_accuracy_score(validation_results)
        freshness_score = self._calculate_freshness_score(lead)
        reliability_score = self._calculate_reliability_score(lead)
        
        # Weighted total (0-100 scale)
        weights = {
            'completeness': 0.40,
            'accuracy': 0.30,
            'freshness': 0.15,
            'reliability': 0.15
        }
        
        total_score = (
            completeness_score * weights['completeness'] +
            accuracy_score * weights['accuracy'] +
            freshness_score * weights['freshness'] +
            reliability_score * weights['reliability']
        ) * 100
        
        return {
            'total_score': round(total_score, 1),
            'component_scores': {
                'completeness': round(completeness_score * 100, 1),
                'accuracy': round(accuracy_score * 100, 1),
                'freshness': round(freshness_score * 100, 1),
                'reliability': round(reliability_score * 100, 1)
            },
            'weights': weights,
            'quality_grade': self._get_quality_grade(total_score)
        }
    
    def _calculate_completeness_score(self, lead: Dict[str, Any]) -> float:
        """Completeness score - only counts valid, useful fields."""
        validator = DataValidator()
        validation = validator.validate_lead(lead)

        score = 0
        checks = 0

        # Phone (must be valid)
        checks += 1
        if validation['contact_validation']['valid'] and lead.get('contact_information', {}).get('phones'):
            score += 1

        # Industry (not empty or 'general')
        checks += 1
        industry = lead.get('business_information', {}).get('industry', '').strip().lower()
        if industry and industry not in ['general', 'n/a', 'none', 'unknown']:
            score += 1

        # Lead score (non-zero)
        checks += 1
        if lead.get('lead_score', {}).get('total_score', 0) > 0:
            score += 1

        # Optional: company name or email add bonus completeness
        if lead.get('business_information', {}).get('company_name', '').strip():
            score += 0.5
            checks += 0.5
        if lead.get('contact_information', {}).get('emails'):
            score += 0.5
            checks += 0.5

        raw_score = score / checks if checks else 0.0
        return min(raw_score, 0.95)  # cap to avoid inflated 100%
    
    def _calculate_accuracy_score(self, validation_results: Dict[str, Any]) -> float:
        """Accuracy score - validation results."""   
        base_score = 1.0
        
        # Heavy penalty for validation errors (contact issues)
        errors = len(validation_results.get('validation_errors', []))
        base_score -= errors * 0.3
        
        # Light penalty for warnings (business info issues)
        warnings = len(validation_results.get('validation_warnings', []))
        base_score -= warnings * 0.1
        
        # Additional penalty if overall validation failed
        if not validation_results.get('overall_valid', True):
            base_score -= 0.2
        
        return max(0.0, min(1.0, base_score))
    

    def _calculate_freshness_score(self, lead: Dict[str, Any]) -> float:
        """Freshness score - data extraction timestamp."""
        timestamp_str = lead.get('extraction_metadata', {}).get('extraction_timestamp', '')
    
        if not timestamp_str:
            return 0.5
        
        try:
            # Handle different timestamp formats more robustly
            if timestamp_str.endswith('Z'):
                timestamp_str = timestamp_str.replace('Z', '+00:00')
            elif not timestamp_str.endswith(('+00:00', '-00:00')) and 'T' in timestamp_str:
                # If no timezone info, assume UTC
                timestamp_str += '+00:00'
            
            extraction_time = datetime.fromisoformat(timestamp_str)
            
            # Always use UTC for consistent comparison
            now = datetime.now(timezone.utc)
            
            # Ensure extraction_time is timezone-aware (convert to UTC if needed)
            if extraction_time.tzinfo is None:
                extraction_time = extraction_time.replace(tzinfo=timezone.utc)
            else:
                extraction_time = extraction_time.astimezone(timezone.utc)
            
            age_days = (now - extraction_time).days
            score = math.exp(-age_days / 365.0)
            return min(1.0, max(0.0, score))
            
        except Exception:
            return 0.5
    
    def _calculate_reliability_score(self, lead: Dict[str, Any]) -> float:
        """Reliability score - source credibility."""
        data_confidence = lead.get('extraction_metadata', {}).get('data_confidence', 0.0)
        
        contact_confidences = []
        for field in ['emails', 'phones', 'websites']:
            for item in lead.get('contact_information', {}).get(field, []):
                if 'confidence' in item:
                    contact_confidences.append(item['confidence'])
        
        avg_contact_conf = sum(contact_confidences) / len(contact_confidences) if contact_confidences else 0.0
        
        # Weighted blend: data_confidence dominates but contacts also matter
        reliability = 0.7 * data_confidence + 0.3 * avg_contact_conf
        return max(0.0, min(reliability, 1.0))
    
    def _get_quality_grade(self, score: float) -> str:
        """Convert quality score to letter grade."""
        if score >= 90:
            return 'A'
        elif score >= 80:
            return 'B'
        elif score >= 70:
            return 'C'
        elif score >= 60:
            return 'D'
        else:
            return 'F'
    
    def _has_field_value(self, lead: Dict[str, Any], field_path: Tuple[str, ...]) -> bool:
        """Check if a nested field has a non-empty value."""
        current = lead
        for key in field_path:
            if not isinstance(current, dict) or key not in current:
                return False
            current = current[key]
        
        if isinstance(current, list):
            return len(current) > 0
        elif isinstance(current, str):
            return bool(current.strip())
        elif isinstance(current, (int, float)):
            return current != 0
        else:
            return current is not None
