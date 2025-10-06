from __future__ import annotations

from typing import Dict, List, Optional, Any
from pydantic import BaseModel, HttpUrl, Field


class ClassificationResult(BaseModel):
	url: HttpUrl
	classification: str = Field(description="'static' or 'dynamic'")
	confidence: float = Field(ge=0.0, le=1.0)
	indicators: Dict[str, float] = Field(default_factory=dict)
	reasons: List[str] = Field(default_factory=list)
	status_code: Optional[int] = None


class PageContent(BaseModel):
	url: HttpUrl
	status_code: int
	elapsed_seconds: float
	encoding: Optional[str]
	content_type: Optional[str]
	html: str
	text: str
	metadata: Dict[str, str] = Field(default_factory=dict)
	processed: Optional[Dict[str, Any]] = None


class Lead(BaseModel):
	# Comprehensive lead model with all required fields
	id: Optional[str] = None
	name: Optional[str] = None
	company_name: Optional[str] = None
	source_url: Optional[HttpUrl] = None
	source_platform: Optional[str] = None
	extraction_timestamp: Optional[str] = None

	# Contact information
	contact_info: Dict[str, Any] = Field(default_factory=dict)

	# Business details
	location: Optional[str] = None
	industry: Optional[str] = None
	company_type: Optional[str] = None
	bio: Optional[str] = None
	address: Optional[str] = None

	# Lead classification
	type: Optional[str] = None  # "lead" or "competitor"
	link_details: Optional[str] = None
	what_we_can_offer: Optional[str] = None

	# Legacy fields for backward compatibility
	business_name: Optional[str] = None
	contact_person: Optional[str] = None
	email: Optional[str] = None
	phone: Optional[str] = None
	website: Optional[HttpUrl] = None

	social_media: Dict[str, str] = Field(default_factory=dict)
	services: List[str] = Field(default_factory=list)

	lead_score: Optional[float] = None
	intent_indicators: List[str] = Field(default_factory=list)
	confidence_scores: Dict[str, float] = Field(default_factory=dict)
	data_sources: List[str] = Field(default_factory=list)

	notes: Optional[str] = None
	status: Optional[str] = None
