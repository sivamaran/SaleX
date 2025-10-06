# AI-Powered Web Scraper for Intelligent Lead Generation

A sophisticated web scraping system that combines traditional pattern-based extraction with AI-powered analysis to generate high-quality business leads from websites. Originally designed for bus travel agencies but adaptable to any industry.

## 🚀 Features

### Core Capabilities
- **Hybrid Extraction Pipeline**: Combines traditional regex patterns (70%) with AI enhancement (30%) for optimal accuracy and cost-effectiveness
- **Smart URL Classification**: Automatically detects static vs dynamic websites for optimal scraping strategy
- **Multi-Format Input Support**: Process URLs from JSON, CSV, or TXT files
- **Intelligent Contact Extraction**: Extract emails, phone numbers, addresses, and social media profiles with confidence scoring
- **Business Information Analysis**: Identify company names, industries, services, and decision makers
- **AI-Powered Enhancement**: Uses Google Gemini LLM for complex content analysis and lead validation
- **Advanced Deduplication**: Multi-level duplicate detection with intelligent merging
- **Lead Scoring & Classification**: Comprehensive scoring system with quality assessment
- **Anti-Detection Mechanisms**: Stealth browsing capabilities to avoid bot detection
- **Structured Data Processing**: Extract and analyze JSON-LD and microdata
- **Export Flexibility**: Output to JSON, CSV formats with customizable fields

### Quality & Performance
- **Data Quality Engine**: Validates and scores lead information for reliability
- **Confidence Scoring**: Per-field confidence ratings for extracted data
- **Error Handling**: Robust error recovery and fallback mechanisms
- **Concurrent Processing**: Multi-threaded URL processing for improved performance
- **Rate Limiting**: Respectful scraping with configurable delays
- **Caching System**: Classification and content caching to reduce redundant requests

## 📋 Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Usage Guide](#usage-guide)
- [Architecture Overview](#architecture-overview)
- [Pipeline Description](#pipeline-description)
- [Output Structure](#output-structure)
- [Module Documentation](#module-documentation)
- [API Reference](#api-reference)
- [Examples](#examples)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## 🛠 Installation

### Prerequisites
- Python 3.10 or higher
- Google Gemini API key (for AI features)
- Playwright browsers (for dynamic content)

### Setup Instructions

1. **Clone the repository**
```bash
git clone <repository-url>
cd web_scraper
```

2. **Create virtual environment**
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Install Playwright browsers**
```bash
playwright install chromium
```

5. **Configure environment variables**
```bash
cp .env.example .env
# Edit .env file with your API keys
```

### Environment Variables

Create a `.env` file in the project root:

```env
# Google Gemini AI Configuration
GEMINI_API_KEY=your_gemini_api_key_here
GEMINI_MAX_CALLS_PER_RUN=5
GEMINI_MIN_INTERVAL_SECONDS=2.0

# Scraping Configuration
MAX_WORKERS=5
DELAY_BETWEEN_REQUESTS=1.0
REQUEST_TIMEOUT=30

# Storage Configuration
STORAGE_PATH=leads_data
ENABLE_DEBUG_RESULTS=true
```

## 🚀 Quick Start

### Basic Usage

```python
from web_scraper.main_app import WebScraperOrchestrator

# Initialize the orchestrator
scraper = WebScraperOrchestrator(
    storage_path="my_leads",
    enable_ai=False,
    enable_quality_engine=False,
    max_workers=3
)

# Process URLs from a file
results = scraper.run_complete_pipeline(
    url_file="urls.txt",
    batch_size=10,
    export_format="json",
    export_path="leads_output.json"
)

print(f"Processed {results['total_processed']} URLs")
print(f"Generated {results['successful_leads']} leads")
```

### Command Line Usage

```bash
# Process URLs from a text file
python main_app.py --url-file urls.txt --output leads.json

# Process specific URLs
python main_app.py --urls "https://example1.com,https://example2.com" --output leads.csv

# Enable debug mode
python main_app.py --url-file urls.txt --debug --output leads.json
```

## ⚙️ Configuration

### WebScraperOrchestrator Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `storage_path` | str | "leads_data" | Directory to store lead data |
| `enable_ai` | bool | True | Enable AI enhancement features |
| `enable_quality_engine` | bool | True | Enable data quality validation |
| `max_workers` | int | 5 | Maximum concurrent workers |
| `delay_between_requests` | float | 1.0 | Delay between requests (seconds) |

### AI Configuration

The system uses Google Gemini for AI-powered analysis:

- **Model**: gemini-2.0-flash
- **Temperature**: 0.1 (for consistent results)
- **Max Tokens**: 2048
- **Rate Limiting**: Configurable calls per run and minimum intervals

## 📖 Usage Guide

### Input Formats

#### 1. Text File (urls.txt)
```
https://example1.com
https://example2.com
https://example3.com
```

#### 2. JSON File (urls.json)
```json
{
  "urls": [
    "https://example1.com",
    "https://example2.com"
  ]
}
```

#### 3. CSV File (urls.csv)
```csv
url
https://example1.com
https://example2.com
```

### Processing Pipeline

```python
# Step-by-step processing
scraper = WebScraperOrchestrator()

# 1. Load URLs
urls = scraper.load_urls_from_file("input_urls.txt")

# 2. Process in batches
successful_leads, failed_urls = scraper.process_urls_batch(urls[:10])

# 3. Save leads
for lead in successful_leads:
    scraper.storage.save_lead(lead)

# 4. Export results
export_path = scraper.export_manager.export_to_json(
    leads=successful_leads,
    filename="exported_leads.json"
)
```

### Advanced Configuration

```python
# Custom anti-detection settings
from web_scraper.utils.anti_detection import AntiDetectionManager

anti_detection = AntiDetectionManager(
    enable_fingerprint_evasion=True,
    enable_behavioral_mimicking=True,
    enable_network_obfuscation=True
)

# Custom scraper with anti-detection
from web_scraper.scrapers.scraper_static import StaticScraper

static_scraper = StaticScraper(
    timeout=45,
    anti_detection=anti_detection
)
```

## 🏗 Architecture Overview

### System Components

```
┌─────────────────────────────────────────────────────────────┐
│                    WebScraperOrchestrator                   │
│                     (Main Controller)                      │
└─────────────────────┬───────────────────────────────────────┘
                      │
      ┌───────────────┼───────────────┐
      │               │               │
┌─────▼─────┐  ┌─────▼─────┐  ┌─────▼─────┐
│    URL    │  │  Content  │  │    AI     │
│Classifier │  │Extractors │  │Integration│
└───────────┘  └───────────┘  └───────────┘
      │               │               │
      │        ┌─────▼─────┐         │
      │        │Processing │         │
      │        │ Pipeline  │         │
      │        └─────┬─────┘         │
      │              │               │
      └──────────────┼───────────────┘
                     │
            ┌────────▼────────┐
            │  Data Quality   │
            │    Engine       │
            └────────┬────────┘
                     │
            ┌────────▼────────┐
            │   Storage &     │
            │    Export       │
            └─────────────────┘
```

### Module Structure

```
web_scraper/
├── main_app.py                 # Main orchestrator
├── ai_integration/
│   └── ai.py                   # Gemini AI integration
├── scrapers/
│   ├── scraper_static.py       # Static content scraper
│   └── scraper_dynamic.py      # Dynamic content scraper
├── utils/
│   ├── classification.py       # URL classification
│   └── anti_detection.py       # Anti-bot detection
├── extractors/
│   ├── lead_extraction.py      # Lead information extraction
│   └── json_ld_filter.py       # Structured data filtering
├── processors/
│   ├── processing.py           # Content processing
│   └── data_quality.py         # Quality validation
├── storage/
│   ├── storage.py              # Data storage and models
│   └── export.py               # Export functionality
└── data_models/
    └── models.py               # Data models and schemas
```

## 🔄 Pipeline Description

### Phase 1: URL Classification & Content Extraction

1. **URL Classification**
   - Analyze HTML structure and content
   - Detect SPA frameworks (React, Vue, Angular)
   - Calculate text-to-HTML ratio
   - Classify as static or dynamic

2. **Content Extraction**
   - **Static**: HTTP requests with anti-detection headers
   - **Dynamic**: Playwright browser automation with stealth mode
   - HTML cleaning and normalization
   - Structured data (JSON-LD) extraction

### Phase 2: Traditional Pattern-Based Extraction

3. **Contact Information Extraction**
   - Email addresses with role classification
   - Phone numbers with format validation
   - Physical addresses with confidence scoring
   - Website URLs and social media profiles

4. **Business Information Extraction**
   - Company name from title tags and content
   - Industry classification using keyword matching
   - Service identification and categorization
   - Decision maker identification with authority scoring

### Phase 3: AI Enhancement Layer

5. **Smart Content Filtering**
   - Filter high-value content sections
   - Prioritize contact and business information areas
   - Remove noise and irrelevant content

6. **AI-Powered Analysis**
   - Business entity disambiguation
   - Contact information validation and enhancement
   - Missing information completion
   - Confidence score calibration

### Phase 4: Data Quality & Output

7. **Quality Assessment**
   - Data validation and normalization
   - Duplicate detection and merging
   - Lead scoring and classification
   - Confidence score aggregation

8. **Export & Storage**
   - JSON format with full metadata
   - CSV format with flattened structure
   - Lead storage with indexing
   - Final leads generation with AI integration

## 📊 Output Structure

### Lead Data Model

```json
{
  "id": "uuid-string",
  "source_url": "https://example.com",
  "extraction_timestamp": "2024-01-01T12:00:00Z",
  "business_name": "Company Name",
  "contact_person": ["John Smith", "Jane Doe"],
  "email": ["contact@example.com", "info@example.com"],
  "phone": ["+1-555-0123", "+1-555-0124"],
  "address": ["123 Main St, City, State 12345"],
  "website": ["https://company.com"],
  "social_media": {
    "linkedin": {
      "platform": "LinkedIn",
      "url": "https://linkedin.com/company/example",
      "followers": 1000,
      "verified": true
    }
  },
  "industry": "education_services",
  "services": ["career_guidance", "educational_counseling"],
  "lead_score": 85.5,
  "lead_classification": "hot_lead",
  "confidence_scores": {
    "email": 0.95,
    "phone": 0.87,
    "address": 0.72,
    "website": 0.91
  },
  "ai_leads": [
    {
      "ai_contacts": [
        {
          "name": "John Smith",
          "email": "j.smith@example.com",
          "phone": "+1-555-0123",
          "organization": "Example Corp",
          "role": "CEO",
          "confidence": 0.9,
          "source": "about_section"
        }
      ],
      "organization_info": {
        "primary_name": "Example Corporation",
        "industry": "Technology Services",
        "services": ["consulting", "development"],
        "location": "San Francisco, CA"
      },
      "addresses": [
        {
          "address": "123 Tech Street, San Francisco, CA 94105",
          "type": "street",
          "confidence": 0.85,
          "source": "contact_section"
        }
      ]
    }
  ],
  "quality_score": 0.89,
  "quality_grade": "A",
  "status": "new"
}
```

### Export Formats

#### JSON Export
- Complete nested data structure
- All metadata and confidence scores
- AI analysis results
- Structured for programmatic access

#### CSV Export
- Flattened data structure
- Separate columns for confidence scores
- Social media profiles as individual columns
- Excel-compatible formatting

#### Final Leads JSON
- Consolidated traditional + AI results
- Separate entries for different contacts
- Optimized for sales team usage
- Includes lead scoring and prioritization

## 📚 Module Documentation

### Core Modules

#### WebScraperOrchestrator (`main_app.py`)
**Main controller class that orchestrates the entire pipeline**

Key Methods:
- `run_complete_pipeline()`: Execute full processing pipeline
- `process_urls_batch()`: Process URLs in concurrent batches
- `classify_and_fetch_content()`: URL classification and content extraction
- `process_and_extract_leads()`: Lead extraction and AI enhancement
- `generate_final_leads()`: Create consolidated output

#### AI Integration (`ai_integration/ai.py`)
**Google Gemini LLM integration for intelligent content analysis**

Key Functions:
- `extract_client_info_from_sections()`: AI-powered lead extraction
- `disambiguate_business_entities()`: Business entity identification
- `validate_and_enhance()`: Contact information validation
- `generate_extraction_strategy()`: Dynamic extraction strategy

#### Static Scraper (`scrapers/scraper_static.py`)
**HTTP-based scraping for static websites**

Features:
- Anti-detection headers and user agents
- Connection pooling and session reuse
- Retry mechanisms with exponential backoff
- Encoding detection and normalization

#### Dynamic Scraper (`scrapers/scraper_dynamic.py`)
**Playwright-based scraping for dynamic websites**

Features:
- Stealth browser automation
- Popup dismissal and interaction simulation
- Network idle detection
- Human-like behavior simulation

#### Lead Extraction (`extractors/lead_extraction.py`)
**Traditional pattern-based information extraction**

Classes:
- `ContactExtractor`: Email, phone, address extraction
- `BusinessInfoExtractor`: Company details and decision makers

#### Data Quality Engine (`processors/data_quality.py`)
**Lead validation and quality assessment**

Features:
- Multi-level validation rules
- Confidence score calibration
- Lead scoring algorithms
- Quality grade assignment

#### Storage System (`storage/storage.py`)
**Data persistence and export functionality**

Classes:
- `LeadModel`: Pydantic data model with validation
- `LeadStorage`: CRUD operations and filtering
- Export to JSON/CSV formats

### Utility Modules

#### URL Classification (`utils/classification.py`)
- Static vs dynamic website detection
- HTML structure analysis
- SPA framework identification
- Classification caching

#### Anti-Detection (`utils/anti_detection.py`)
- Browser fingerprint evasion
- Human behavior simulation
- Network request obfuscation
- Rate limiting and delays

#### Content Processing (`processors/processing.py`)
- HTML cleaning and normalization
- Structured data extraction
- Content sectioning
- Basic contact pattern matching

## 🔧 API Reference

### WebScraperOrchestrator

```python
class WebScraperOrchestrator:
    def __init__(
        self,
        storage_path: str = "leads_data",
        enable_ai: bool = True,
        enable_quality_engine: bool = True,
        max_workers: int = 5,
        delay_between_requests: float = 1.0
    )
    
    def run_complete_pipeline(
        self,
        urls: List[str] = None,
        url_file: str = None,
        batch_size: int = 50,
        export_format: str = "json",
        export_path: str = None,
        generate_final_leads: bool = True
    ) -> Dict[str, Any]
    
    def load_urls_from_file(self, file_path: str) -> List[str]
    
    def process_urls_batch(
        self, 
        urls: List[str]
    ) -> Tuple[List[LeadModel], List[Dict[str, Any]]]
```

### LeadStorage

```python
class LeadStorage:
    def save_lead(self, lead: LeadModel) -> str
    def load_lead(self, lead_id: str) -> Optional[LeadModel]
    def load_all_leads(self) -> List[LeadModel]
    def filter_leads(
        self,
        min_score: Optional[float] = None,
        status: Optional[LeadStatus] = None,
        industry: Optional[str] = None
    ) -> List[LeadModel]
    def export_to_csv(self, filename: str) -> str
```

## 💡 Examples

### Example 1: Basic Lead Generation

```python
from web_scraper.main_app import WebScraperOrchestrator

# Initialize scraper
scraper = WebScraperOrchestrator(
    storage_path="travel_leads",
    enable_ai=True
)

# Process travel agency websites
urls = [
    "https://example-travel.com",
    "https://bus-tours.com",
    "https://charter-services.com"
]

results = scraper.run_complete_pipeline(
    urls=urls,
    export_format="json",
    export_path="travel_leads.json"
)

print(f"Generated {len(results['successful_leads'])} leads")
```

### Example 2: Batch Processing with Custom Settings

```python
# Custom configuration for large-scale processing
scraper = WebScraperOrchestrator(
    storage_path="bulk_leads",
    max_workers=10,
    delay_between_requests=0.5
)

# Process URLs from CSV file
results = scraper.run_complete_pipeline(
    url_file="company_urls.csv",
    batch_size=25,
    export_format="csv",
    export_path="extracted_leads.csv"
)

# Filter high-quality leads
high_quality_leads = scraper.storage.filter_leads(
    min_score=80.0,
    status=LeadStatus.NEW
)

print(f"Found {len(high_quality_leads)} high-quality leads")
```

### Example 3: AI-Only Processing

```python
# Focus on AI enhancement for complex websites
scraper = WebScraperOrchestrator(
    enable_ai=True,
    enable_quality_engine=True
)

# Process complex dynamic websites
complex_urls = [
    "https://spa-website.com",
    "https://react-app.com"
]

results = scraper.run_complete_pipeline(
    urls=complex_urls,
    generate_final_leads=True
)

# Access AI-extracted contacts
for lead in results['successful_leads']:
    if lead.ai_leads:
        print(f"AI found {len(lead.ai_leads)} additional contacts")
```

## 🐛 Troubleshooting

### Common Issues

#### 1. Playwright Installation Issues
```bash
# Reinstall Playwright browsers
playwright install --force chromium

# Check Playwright installation
playwright --version
```

#### 2. Gemini API Errors
```python
# Check API key configuration
import os
print(f"API Key configured: {bool(os.getenv('GEMINI_API_KEY'))}")

# Verify API key format
api_key = os.getenv('GEMINI_API_KEY', '')
print(f"Key starts with AIza: {api_key.startswith('AIza')}")
```

#### 3. Memory Issues with Large Batches
```python
# Reduce batch size and workers
scraper = WebScraperOrchestrator(
    max_workers=2,
    delay_between_requests=2.0
)

# Process in smaller batches
results = scraper.run_complete_pipeline(
    url_file="urls.txt",
    batch_size=10  # Reduced from default 50
)
```

#### 4. Rate Limiting Issues
```python
# Increase delays between requests
scraper = WebScraperOrchestrator(
    delay_between_requests=3.0,  # Increased delay
    max_workers=1  # Sequential processing
)
```

### Debug Mode

Enable debug mode to save intermediate results:

```python
# Debug results are saved to debug_results/ directory
scraper = WebScraperOrchestrator()

# HTML and sections are automatically saved when processing
# Check debug_results/ folder for intermediate files
```

### Logging Configuration

```python
from loguru import logger

# Configure detailed logging
logger.add(
    "scraper.log",
    level="DEBUG",
    format="{time} | {level} | {message}",
    rotation="10 MB"
)
```

## 🤝 Contributing

### Development Setup

1. Fork the repository
2. Create a feature branch
3. Install development dependencies:
```bash
pip install -r requirements-dev.txt
```

4. Run tests:
```bash
pytest tests/
```

5. Submit a pull request

### Code Style

- Follow PEP 8 guidelines
- Use type hints for all functions
- Add docstrings for public methods
- Maintain test coverage above 80%

### Testing

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_main_app.py

# Run with coverage
pytest --cov=web_scraper tests/
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Google Gemini AI for intelligent content analysis
- Playwright team for browser automation
- BeautifulSoup for HTML parsing
- Pydantic for data validation
- The open-source community for various dependencies

## 📞 Support

For support and questions:
- Create an issue on GitHub
- Check the troubleshooting section
- Review the examples and documentation

---

**Built with ❤️ for intelligent lead generation**