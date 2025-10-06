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
	# Minimal lead model aligned with plan Phase 7.1 (will expand later phases)
	id: Optional[str] = None
	source_url: Optional[HttpUrl] = None
	extraction_timestamp: Optional[str] = None

	business_name: Optional[str] = None
	contact_person: Optional[str] = None
	email: Optional[str] = None
	phone: Optional[str] = None
	address: Optional[str] = None
	website: Optional[HttpUrl] = None

	social_media: Dict[str, str] = Field(default_factory=dict)
	industry: Optional[str] = None
	services: List[str] = Field(default_factory=list)

	lead_score: Optional[float] = None
	intent_indicators: List[str] = Field(default_factory=list)
	confidence_scores: Dict[str, float] = Field(default_factory=dict)
	data_sources: List[str] = Field(default_factory=list)

	notes: Optional[str] = None
	status: Optional[str] = None


class FacebookScrapingConfig(BaseModel):
	"""Configuration for Facebook scraping operations"""
	max_workers: int = Field(default=3, description="Maximum concurrent workers for Facebook scraping")
	batch_size: int = Field(default=3, description="Number of URLs to process in each batch")
	context_pool_size: int = Field(default=3, description="Number of browser contexts to maintain")
	rate_limit_delay: float = Field(default=3.0, description="Delay between requests in seconds")
	context_reuse_limit: int = Field(default=10, description="Maximum number of scrapes per browser context before renewal")
	max_retries: int = Field(default=3, description="Maximum retry attempts for failed scrapes")
	retry_delay: float = Field(default=2.0, description="Delay between retry attempts in seconds")
	timeout_seconds: int = Field(default=30, description="Timeout for individual scrape operations")
