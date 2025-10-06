from __future__ import annotations

import json
import csv
import gzip
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from enum import Enum
import uuid

from pydantic import BaseModel, Field, field_validator
from loguru import logger


class LeadStatus(str, Enum):
    """Lead processing status enumeration"""
    NEW = "new"
    CONTACTED = "contacted"
    QUALIFIED = "qualified"
    CONVERTED = "converted"
    REJECTED = "rejected"


class SocialMediaProfile(BaseModel):
    """Social media profile information"""
    platform: str
    url: str
    followers: Optional[int] = None
    verified: Optional[bool] = None


class ContactInfo(BaseModel):
    """Contact information with confidence scores"""
    email: Optional[str] = None
    email_confidence: Optional[float] = None
    phone: Optional[str] = None
    phone_confidence: Optional[float] = None
    address: Optional[str] = None
    address_confidence: Optional[float] = None
    website: Optional[str] = None
    website_confidence: Optional[float] = None


class BusinessInfo(BaseModel):
    """Business information details"""
    company_name: Optional[str] = None
    industry: Optional[str] = None
    services: List[str] = Field(default_factory=list)
    company_size: Optional[str] = None
    description: Optional[str] = None


class LeadModel(BaseModel):
    """Complete lead data model as specified in Phase 7.1"""
    
    # Core identification
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_url: str
    extraction_timestamp: datetime = Field(default_factory=datetime.now)
    
    # Business information
    business_name: Optional[str] = None
    contact_person: Optional[Union[str, List[str]]] = None
    
    # Contact details
    email: Optional[Union[str, List[str]]] = None
    phone: Optional[Union[str, List[str]]] = None
    address: Optional[Union[str, List[str]]] = None
    website: Optional[Union[str, List[str]]] = None
    social_media: Dict[str, SocialMediaProfile] = Field(default_factory=dict)
    
    # Business classification
    industry: Optional[str] = None
    services: List[str] = Field(default_factory=list)
    
    # Scoring and analysis
    lead_score: Optional[float] = None
    lead_classification: Optional[str] = None
    factor_scores: List[Dict[str, Any]] = Field(default_factory=list)
    confidence_scores: Dict[str, float] = Field(default_factory=dict)
    
    # Data quality
    data_sources: List[str] = Field(default_factory=list)
    quality_score: Optional[float] = None
    quality_grade: Optional[str] = None
    
    # AI insights
    notes: Optional[str] = None
    ai_leads: List[Dict[str, Any]] = Field(default_factory=list)  # AI extracted leads
    
    # Processing status
    status: LeadStatus = LeadStatus.NEW
    
    @field_validator('extraction_timestamp', mode='before')
    @classmethod
    def parse_timestamp(cls, v):
        """Parse timestamp from various formats"""
        if isinstance(v, str):
            # Handle ISO format with Z suffix
            if v.endswith('Z'):
                v = v.replace('Z', '+00:00')
            try:
                return datetime.fromisoformat(v)
            except ValueError:
                # Try parsing without timezone info
                try:
                    return datetime.fromisoformat(v.split('+')[0].split('Z')[0])
                except ValueError:
                    # Fallback to current time if parsing fails
                    logger.warning(f"Could not parse timestamp: {v}, using current time")
                    return datetime.now()
        elif isinstance(v, datetime):
            return v
        else:
            return datetime.now()
    
    def to_flat_dict(self) -> Dict[str, Any]:
        """Convert to flattened dictionary for CSV export"""
        # Helper function to safely convert lists to strings
        def safe_list_to_string(value):
            if isinstance(value, list):
                return '; '.join(str(item) for item in value)
            elif value is None:
                return ''
            else:
                return str(value)
        
        # Helper function to safely convert factor_scores
        def format_factor_scores(factor_scores):
            if not factor_scores:
                return ''
            try:
                return '; '.join(f"{fs.get('factor', 'unknown')}:{fs.get('score', 0)}" 
                              for fs in factor_scores if isinstance(fs, dict))
            except Exception:
                return str(factor_scores) if factor_scores else ''
        
        flat = {
            'id': self.id,
            'source_url': self.source_url,
            'extraction_timestamp': self.extraction_timestamp.isoformat(),
            'business_name': self.business_name or '',
            'contact_person': safe_list_to_string(self.contact_person),
            'email': safe_list_to_string(self.email),
            'phone': safe_list_to_string(self.phone),
            'address': safe_list_to_string(self.address),
            'website': safe_list_to_string(self.website),
            'industry': self.industry or '',
            'services': safe_list_to_string(self.services),
            'lead_score': self.lead_score,
            'lead_classification': self.lead_classification or '',
            'factor_scores': format_factor_scores(self.factor_scores),
            'data_sources': safe_list_to_string(self.data_sources),
            'quality_score': self.quality_score,
            'quality_grade': self.quality_grade or '',
            'notes': self.notes or '',
            'ai_leads_count': len(self.ai_leads),
            'ai_leads': json.dumps(self.ai_leads) if self.ai_leads else '',
            'status': self.status.value
        }
        
        # Add confidence scores as separate columns
        for field, score in self.confidence_scores.items():
            flat[f'confidence_{field}'] = score
            
        # Add social media profiles
        for platform, profile in self.social_media.items():
            flat[f'social_{platform}_url'] = profile.url
            if profile.followers is not None:
                flat[f'social_{platform}_followers'] = profile.followers
            if profile.verified is not None:
                flat[f'social_{platform}_verified'] = profile.verified
                
        return flat
    
    @staticmethod
    def calculate_composite_confidence(items: List[Dict[str, Any]]) -> float:
        """Calculate composite confidence score from multiple items"""
        if not items:
            return 0.0
        
        # Ensure items is a list and handle various formats
        if not isinstance(items, list):
            return 0.0
            
        # Base confidence from best match
        confidences = []
        for item in items:
            if isinstance(item, dict):
                confidence = item.get('confidence', 0.0)
                if isinstance(confidence, (int, float)):
                    confidences.append(float(confidence))
        
        if not confidences:
            return 0.0
            
        max_confidence = max(confidences)
        
        # Bonus for having multiple high-confidence matches
        high_confidence_count = sum(1 for conf in confidences if conf > 0.8)
        quantity_bonus = min(0.1 * (high_confidence_count - 1), 0.2)  # Max 0.2 bonus
        
        return min(max_confidence + quantity_bonus, 1.0)
        
    @classmethod
    def from_extraction_data(cls, extraction_data: Dict[str, Any], source_url: str) -> 'LeadModel':
        """Create LeadModel from lead extraction pipeline output"""
        
        # Safely extract nested data with default fallbacks
        contact_info = extraction_data.get('contact_information', {})
        emails = contact_info.get('emails', []) if isinstance(contact_info.get('emails'), list) else []
        phones = contact_info.get('phones', []) if isinstance(contact_info.get('phones'), list) else []
        addresses = contact_info.get('addresses', []) if isinstance(contact_info.get('addresses'), list) else []
        websites = contact_info.get('websites', []) if isinstance(contact_info.get('websites'), list) else []
        social_media = contact_info.get('social_media', {}) if isinstance(contact_info.get('social_media'), dict) else {}
        
        # Extract business information
        business_info = extraction_data.get('business_information', {})
        
        # Extract scoring information
        lead_score_data = extraction_data.get('lead_score', {})
        
        # Extract metadata
        metadata = extraction_data.get('extraction_metadata', {})
        
        # Extract AI leads
        ai_leads = extraction_data.get('ai_leads', [])
        
        # Build confidence scores
        confidence_scores = {}
        if emails:
            confidence_scores['email'] = cls.calculate_composite_confidence(emails)
        if phones:
            confidence_scores['phone'] = cls.calculate_composite_confidence(phones)
        if addresses:
            confidence_scores['address'] = cls.calculate_composite_confidence(addresses)
        if websites:
            confidence_scores['website'] = cls.calculate_composite_confidence(websites)
            
        # Build social media profiles
        social_profiles = {}
        for platform, data in social_media.items():
            if isinstance(data, dict) and 'url' in data:
                try:
                    social_profiles[platform] = SocialMediaProfile(
                        platform=platform,
                        url=data['url'],
                        followers=data.get('followers'),
                        verified=data.get('verified')
                    )
                except Exception as e:
                    logger.warning(f"Failed to create social media profile for {platform}: {e}")
        
        # Extract contact person safely
        contact_person = None
        decision_makers = business_info.get("decision_makers", [])
        if isinstance(decision_makers, list) and decision_makers:
            contact_person = [p.get("name") for p in decision_makers if isinstance(p, dict) and p.get("name")]
            if not contact_person:
                contact_person = None
        
        # Extract values from lists safely
        def extract_values(items):
            if not isinstance(items, list) or not items:
                return None
            values = [item.get("value") for item in items if isinstance(item, dict) and item.get("value")]
            return values if values else None
        
        # Get timestamp
        timestamp_str = metadata.get('extraction_timestamp')
        if timestamp_str:
            try:
                extraction_timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                extraction_timestamp = datetime.now()
        else:
            extraction_timestamp = datetime.now()
        
        return cls(
            source_url=source_url,
            extraction_timestamp=extraction_timestamp,
            business_name=business_info.get('company_name'),
            contact_person=contact_person,
            email=extract_values(emails),
            phone=extract_values(phones),
            address=extract_values(addresses),
            website=extract_values(websites),
            social_media=social_profiles,
            industry=business_info.get('industry'),
            services=business_info.get('services', []) if isinstance(business_info.get('services'), list) else [],
            lead_score=lead_score_data.get('total_score'),
            lead_classification=lead_score_data.get('classification'),
            factor_scores=lead_score_data.get('factor_scores', []) if isinstance(lead_score_data.get('factor_scores'), list) else [],
            confidence_scores=confidence_scores,
            data_sources=[source_url],
            ai_leads=ai_leads,
        )


class LeadStorage:
    """Lead storage and retrieval system"""
    
    def __init__(self, storage_path: str = "leads_data"):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(exist_ok=True)
        
    def save_lead(self, lead: LeadModel) -> str:
        """Save a single lead to storage"""
        lead_file = self.storage_path / f"lead_{lead.id}.json"
        
        try:
            # Convert lead to dict and handle datetime serialization
            lead_dict = lead.model_dump()
            
            with open(lead_file, 'w', encoding='utf-8') as f:
                json.dump(lead_dict, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"Saved lead {lead.id} to {lead_file}")
            return str(lead_file)
        except Exception as e:
            logger.error(f"Failed to save lead {lead.id}: {e}")
            raise
    
    def save_leads_batch(self, leads: List[LeadModel]) -> List[str]:
        """Save multiple leads to storage"""
        saved_files = []
        for lead in leads:
            try:
                saved_files.append(self.save_lead(lead))
            except Exception as e:
                logger.error(f"Failed to save lead {lead.id}: {e}")
                continue
        return saved_files
    
    def load_lead(self, lead_id: str) -> Optional[LeadModel]:
        """Load a single lead by ID"""
        lead_file = self.storage_path / f"lead_{lead_id}.json"
        
        if not lead_file.exists():
            logger.warning(f"Lead file not found: {lead_file}")
            return None
            
        try:
            with open(lead_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return LeadModel(**data)
        except Exception as e:
            logger.error(f"Failed to load lead {lead_id} from {lead_file}: {e}")
            return None
    
    def load_all_leads(self) -> List[LeadModel]:
        """Load all leads from storage"""
        leads = []
        lead_files = list(self.storage_path.glob("lead_*.json"))
        
        if not lead_files:
            logger.info("No lead files found in storage")
            return leads
            
        for lead_file in lead_files:
            try:
                with open(lead_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    lead = LeadModel(**data)
                    leads.append(lead)
            except Exception as e:
                logger.warning(f"Failed to load lead from {lead_file}: {e}")
                continue
                
        logger.info(f"Loaded {len(leads)} leads from storage")
        return leads
    
    def filter_leads(self, 
                    min_score: Optional[float] = None,
                    max_score: Optional[float] = None,
                    status: Optional[LeadStatus] = None,
                    industry: Optional[str] = None,
                    start_date: Optional[datetime] = None,
                    end_date: Optional[datetime] = None) -> List[LeadModel]:
        """Filter leads based on criteria"""
        
        leads = self.load_all_leads()
        filtered = []
        
        for lead in leads:
            # Score filtering
            if min_score is not None and (lead.lead_score is None or lead.lead_score < min_score):
                continue
            if max_score is not None and (lead.lead_score is None or lead.lead_score > max_score):
                continue
                
            # Status filtering
            if status is not None and lead.status != status:
                continue
                
            # Industry filtering
            if industry is not None and lead.industry != industry:
                continue
                
            # Date filtering
            if start_date is not None and lead.extraction_timestamp < start_date:
                continue
            if end_date is not None and lead.extraction_timestamp > end_date:
                continue
                
            filtered.append(lead)
            
        return filtered
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics"""
        leads = self.load_all_leads()
        
        if not leads:
            return {"total_leads": 0}
            
        statuses = {}
        industries = {}
        scores = [lead.lead_score for lead in leads if lead.lead_score is not None]
        
        for lead in leads:
            statuses[lead.status.value] = statuses.get(lead.status.value, 0) + 1
            if lead.industry:
                industries[lead.industry] = industries.get(lead.industry, 0) + 1
        
        return {
            "total_leads": len(leads),
            "status_distribution": statuses,
            "industry_distribution": industries,
            "average_score": sum(scores) / len(scores) if scores else 0,
            "score_range": {"min": min(scores), "max": max(scores)} if scores else None,
            "oldest_lead": min(lead.extraction_timestamp for lead in leads).isoformat(),
            "newest_lead": max(lead.extraction_timestamp for lead in leads).isoformat(),
        }

    def delete_lead(self, lead_id: str) -> bool:
        """Delete a lead from storage"""
        lead_file = self.storage_path / f"lead_{lead_id}.json"
        
        if not lead_file.exists():
            logger.warning(f"Lead file not found: {lead_file}")
            return False
            
        try:
            lead_file.unlink()
            logger.info(f"Deleted lead {lead_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete lead {lead_id}: {e}")
            return False

    def export_to_csv(self, filename: str, leads: Optional[List[LeadModel]] = None) -> str:
        """Export leads to CSV file"""
        if leads is None:
            leads = self.load_all_leads()
            
        if not leads:
            logger.warning("No leads to export")
            return ""
            
        csv_file = self.storage_path / filename
        
        try:
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                # Get all possible fields from all leads to ensure complete headers
                all_fieldnames = set()
                lead_data = []
                
                for lead in leads:
                    flat_data = lead.to_flat_dict()
                    lead_data.append(flat_data)
                    all_fieldnames.update(flat_data.keys())
                
                fieldnames = sorted(list(all_fieldnames))
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                writer.writeheader()
                for data in lead_data:
                    # Fill missing fields with empty string
                    row_data = {field: data.get(field, '') for field in fieldnames}
                    writer.writerow(row_data)
            
            logger.info(f"Exported {len(leads)} leads to {csv_file}")
            return str(csv_file)
        except Exception as e:
            logger.error(f"Failed to export leads to CSV: {e}")
            raise