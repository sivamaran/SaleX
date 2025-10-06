# AI Lead Generation Application

A comprehensive lead generation system that integrates multiple scrapers to collect, process, and enhance leads from various sources with unified storage and processing. Scraping (Instagram, LinkedIn, YouTube, Facebook, and general websites) to identify potential customers based on Ideal Customer Profiles (ICP). The system features a unified data model for consistent lead storage and processing across all platforms.

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [System Flow](#system-flow)
- [Scrapers](#scrapers)
- [Database](#database)
- [Unified Data Model](#unified-data-model)
- [API Documentation](#api-documentation)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Contributing](#contributing)
- [License](#license)

## Overview

This application orchestrates multiple specialized scrapers to collect potential lead data from various platforms based on a predefined Ideal Customer Profile. It uses Google's Gemini AI to generate targeted search queries and coordinates data collection across web scraping, Instagram, LinkedIn, YouTube, and Facebook platforms.

**Target Use Case:** Premium bus travel and group tour services seeking corporate clients, wedding planners, educational institutions, and family group organizers.

## Key Features

- **Multi-platform Scraping**: Unified interface for Instagram, LinkedIn, YouTube, Facebook, and general web scraping
- **AI-Powered Query Generation**: Uses Google's Gemini AI to generate targeted search queries
- **Unified Data Model**: Standardized data storage across all platforms in the `unified_leads` collection
- **Dual Storage**: Maintains both platform-specific and unified data collections
- **Data Quality Scoring**: Automated quality assessment for each lead
- **Batch Processing**: Efficient handling of large datasets
- **Duplicate Detection**: Smart deduplication based on URL and content
- **Modular Architecture**: Easy to extend with new scrapers and data sources
- **Comprehensive Logging**: Detailed logging for debugging and monitoring

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Flask API Layer                          │
│                     (app.py)                                │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│           Lead Generation Orchestrator                      │
│                   (main.py)                                 │
├─────────────────────┼───────────────────────────────────────┤
│  ICP Management     │  Query Generation    │   URL Collection │
│  Scraper Selection  │  (Gemini AI)        │  Classification   │
└─────────────────────┼───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                 Scraper Layer                               │
├─────────────────┬─────────────┬─────────────┬───────────────┤
│  Web Scraper    │ Instagram   │  LinkedIn   │   YouTube     │
│   (General)     │   Scraper   │   Scraper   │   Scraper     │
└─────────────────┴─────────────┴─────────────┴───────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                 Data Storage Layer                          │
├─────────────────┬───────────────────────────────────────────┤
│   MongoDB       │            JSON Reports                   │
│  (Primary)      │           (Backup/Export)                 │
└─────────────────┴───────────────────────────────────────────┘
```

### Core Components

1. **Flask API Layer** (`app.py`) - RESTful API interface
2. **Orchestrator** (`main.py`) - Coordinates the entire pipeline
3. **Scraper Modules** - Platform-specific data collection
4. **Database Layer** - MongoDB for data persistence
5. **AI Integration** - Gemini AI for query generation

## System Flow

### Complete Pipeline Flow

```
1. ICP Definition
   ├─ Load ICP data
   └─ Define target customer characteristics

2. Scraper Selection
   ├─ User selects desired scrapers
   └─ Available: web, instagram, linkedin, youtube

3. Query Generation (AI-Powered)
   ├─ Gemini AI analyzes ICP data
   ├─ Generates 15 base search queries
   └─ Adds platform-specific variations

4. URL Collection
   ├─ Execute queries via web_url_scraper
   ├─ Collect search result URLs
   └─ Classify URLs by platform type

5. Data Scraping
   ├─ Route URLs to appropriate scrapers
   ├─ Extract lead information
   └─ Store results in MongoDB

6. Report Generation
   ├─ Compile results from all scrapers
   ├─ Generate summary statistics
   └─ Export JSON report
```

### Data Classification Flow

```
URLs from Search Results
          │
          ▼
    URL Classifier
          │
    ┌─────┼─────┬─────┬─────┐
    ▼     ▼     ▼     ▼     ▼
 General │  Instagram │ LinkedIn │ YouTube
         │           │          │
         ▼           ▼          ▼          ▼
   Web Scraper | IG Scraper | LI Scraper | YT Scraper | FB Scraper
```

## Scrapers

### 1. Web Scraper (General)
- **Purpose**: Scrapes general websites for contact information
- **Target**: Corporate websites, business directories, service pages
- **Data Extracted**: Company info, contact details, business descriptions
- **Technology**: Selenium-based with anti-detection measures

### 2. Instagram Scraper
- **Purpose**: Extracts profile information from Instagram accounts
- **Target**: Travel influencers, wedding planners, event organizers
- **Data Extracted**: Profile bio, follower count, contact info, recent posts
- **Technology**: Browser automation with headless mode

### 3. LinkedIn Scraper
- **Purpose**: Collects professional profiles and company information
- **Target**: HR managers, corporate executives, event coordinators
- **Data Extracted**: Professional background, company details, contact information
- **Technology**: Selenium with anti-detection capabilities

### 4. YouTube Scraper
- **Purpose**: Analyzes YouTube channels for travel content creators
- **Target**: Travel vloggers, tour companies, educational channels
- **Data Extracted**: Channel info, subscriber count, video content analysis
- **Technology**: Browser automation with content analysis

### 5. Facebook Scraper
- **Purpose**: Extracts profile and page information from Facebook
- **Target**: Community groups, event organizers, local businesses
- **Data Extracted**: Profile info, page details, group memberships, contact information
- **Technology**: Browser automation with anti-detection measures

## Database

The application uses MongoDB for data storage with the following collections:

### Core Collections

#### URLs Collection
```javascript
{
  "_id": ObjectId,
  "url": "https://example.com",
  "url_type": "general|instagram|linkedin|youtube|facebook",
  "query": "search query used",
  "domain": "example.com",
  "created_at": ISODate,
  "scraped": boolean,
  "last_scraped_at": ISODate,
  "scrape_attempts": Number,
  "error": String
}
```

#### Platform-Specific Collections
- `web_leads` - General website data
- `instagram_leads` - Instagram profile data  
- `linkedin_leads` - LinkedIn profile/company data
- `youtube_leads` - YouTube channel data
- `facebook_leads` - Facebook profile and page data
- `leadgen_leads` - Processed and filtered leads from all sources

## Unified Data Model

All scraped leads are normalized and stored in the `unified_leads` collection with a consistent schema:

```javascript
{
  "_id": ObjectId,
  "url": "source_url",
  "platform": "instagram|linkedin|youtube|web",
  "content_type": "profile|post|video|article",
  "source": "scraper_source",
  "profile": {
    "username": "username",
    "full_name": "Full Name",
    "bio": "Bio text",
    "job_title": "Job Title",
    "location": "City, Country",
    "employee_count": "100-500"
  },
  "contact": {
    "emails": ["email@example.com"],
    "phone_numbers": ["+1234567890"],
    "address": "123 Street, City, Country",
    "websites": ["https://example.com"],
    "social_media_handles": {
      "twitter": "@handle",
      "facebook": "username",
      "linkedin": "username"
    },
    "bio_links": ["https://link1.com", "https://link2.com"]
  },
  "content": {
    "caption": "Post/video caption",
    "upload_date": "2024-01-01T12:00:00Z",
    "channel_name": "Channel Name",
    "author_name": "Author Name"
  },
  "metadata": {
    "scraped_at": "2024-01-01T12:00:00Z",
    "data_quality_score": 0.95,
    "source_url": "original_source_url",
    "platform_specific_data": {}
  },
  "is_processed": Boolean,
  "processing_errors": [String],
  "created_at": ISODate,
  "updated_at": ISODate
}
```

### Data Quality Scoring

Each lead is assigned a data quality score (0-1) based on:
- Presence of contact information (email, phone)
- Completeness of profile data
- Social media presence
- Data freshness

#### Unified Leads Collection
All scraped leads are normalized and stored in the `unified_leads` collection with a consistent schema:

```javascript
{
  "_id": ObjectId,
  "url": "source_url",
  "platform": "instagram|linkedin|youtube|web",
  "content_type": "profile|post|video|article",
  "source": "scraper_source",
  "profile": {
    "username": "username",
    "full_name": "Full Name",
    "bio": "Bio text",
    "job_title": "Job Title",
    "location": "City, Country",
    "employee_count": "100-500"
  },
  "contact": {
    "emails": ["email@example.com"],
    "phone_numbers": ["+1234567890"],
    "address": "123 Street, City, Country",
    "websites": ["https://example.com"],
    "social_media_handles": {
      "twitter": "@handle",
      "facebook": "username",
      "linkedin": "username"
    },
    "bio_links": ["https://link1.com", "https://link2.com"]
  },
  "content": {
    "caption": "Post/video caption",
    "upload_date": "2024-01-01T12:00:00Z",
    "channel_name": "Channel Name",
    "author_name": "Author Name"
  },
  "metadata": {
    "scraped_at": "2024-01-01T12:00:00Z",
    "data_quality_score": 0.95,
    "original_source": "original_platform_collection"
  }
}
```

### Data Transformation Pipeline

1. **Platform-Specific Collection**: Raw data is first stored in platform-specific collections
2. **Normalization**: Data is transformed into the unified schema
3. **Deduplication**: Duplicate leads are identified and merged
4. **Quality Scoring**: Each lead receives a data quality score
5. **Unified Storage**: Processed leads are stored in the `unified_leads` collection

### Database Services

The application uses a centralized MongoDB manager that provides:
- Connection pooling
- Data validation and transformation
- Error handling and retries
- Consistent document structure across platforms
- URL deduplication
- Batch processing for improved performance
- Data quality scoring
- Schema validation and migration support

## API Documentation

### Base URL
```
http://localhost:5000
```

### Authentication
Currently, the API does not require authentication for development. In production, consider adding API key authentication.

### Endpoints

#### 1. Health Check
- **GET** `/health`
  - Check if the API is running
  - Response:
    ```json
    {
      "status": "healthy",
      "timestamp": "2023-01-01T12:00:00.000000",
      "service": "Lead Generation Backend"
    }
    ```

#### 2. Get Available Scrapers
- **GET** `/api/scrapers`
  - List all available scrapers and their status
  - Response:
    ```json
    {
      "success": true,
      "data": {
        "available_scrapers": ["web_scraper", "instagram", "linkedin", "youtube", "facebook"],
        "scrapers_info": {
          "web_scraper": {"description": "General web page scraper", "requires_urls": true},
          "instagram": {"description": "Instagram profile scraper", "requires_urls": false},
          "linkedin": {"description": "LinkedIn profile scraper", "requires_urls": false},
          "youtube": {"description": "YouTube channel scraper", "requires_urls": false},
          "facebook": {"description": "Facebook profile and page scraper", "requires_urls": false}
        }
      }
    }
    ```

#### 3. Lead Filtering
- **POST** `/api/lead-filtering/run`
  - Run lead filtering process on collected data
  - Request Body (optional):
    ```json
    {
      "query_filter": {"platform": "web"},
      "batch_size": 50
    }
    ```
  - Response:
    ```json
    {
      "success": true,
      "message": "Lead filtering completed successfully",
      "stats": {
        "total_processed": 150,
        "leads_added": 42,
        "leads_updated": 15,
        "duplicates_skipped": 93,
        "execution_time": "2.34s"
      }
    }
    ```

#### 4. Contact Enhancement
- **POST** `/api/contact-enhancement/run`
  - Enhance contact information for existing leads
  - Request Body (optional):
    ```json
    {
      "limit": 100,
      "batch_size": 20
    }
    ```
  - Response:
    ```json
    {
      "success": true,
      "message": "Contact enhancement completed",
      "stats": {
        "leads_enhanced": 78,
        "emails_added": 45,
        "phones_added": 33,
        "execution_time": "1.25m"
      }
    }
    ```

#### 5. Get Leads by ICP
- **GET** `/api/leads/icp/<icp_identifier>`
  - Retrieve leads filtered by ICP identifier
  - Query Parameters:
    - `limit`: Number of results to return (default: 50)
    - `skip`: Number of results to skip (for pagination)
    - `sort_field`: Field to sort by (default: "_id")
    - `sort_order`: Sort order (1 for ascending, -1 for descending, default: -1)
  - Response:
    ```json
    {
      "success": true,
      "data": [
        {
          "_id": "5f8d...",
          "url": "https://example.com/contact",
          "platform": "web",
          "profile": {
            "full_name": "John Doe",
            "job_title": "HR Manager"
          },
          "contact": {
            "emails": ["john@example.com"],
            "phones": ["+1234567890"]
          },
          "data_quality_score": 0.85
        }
      ],
      "total_count": 123,
      "page": 1,
      "page_size": 50
    }
    ```

#### 6. Get ICP Statistics
- **GET** `/api/icp/stats/<icp_identifier>`
  - Get statistics for a specific ICP
  - Response:
    ```json
    {
      "success": true,
      "data": {
        "icp_identifier": "premium-bus-travel_20241201",
        "total_leads": 150,
        "leads_by_platform": {
          "web": 75,
          "instagram": 35,
          "linkedin": 25,
          "youtube": 15
        },
        "data_quality": {
          "average_score": 0.82,
          "high_quality": 120,
          "medium_quality": 25,
          "low_quality": 5
        },
        "last_updated": "2023-01-01T12:00:00.000Z"
      }
    }
    ```

#### 7. Get ICP Template
- **GET** `/api/icp/template`
  - Get ICP template with all required fields
  - Response:
    ```json
    {
      "success": true,
      "data": {
        "icp_template": {
          "product_details": {
            "product_name": "Premium Bus Travel & Group Tour Services",
            "product_category": "Travel & Tourism/Transportation Services",
            "usps": ["Luxury bus fleet with premium amenities", "..."],
            "pain_points_solved": ["Complicated group travel logistics", "..."]
          },
          "icp_information": {
            "target_industry": ["Corporate Companies", "Educational Institutions", "..."],
            "company_size": "10-1000+ employees/members",
            "decision_maker_persona": ["HR Manager", "Event Coordinator", "..."],
            "region": ["India", "Major Cities", "Tourist Destinations"],
            "budget_range": "$5,000-$50,000 annually"
          }
        }
      }
    }
    ```

#### 4. Lead Generation Pipeline
- **POST** `/api/lead-generation/run`
  - Run complete lead generation pipeline
  - Request Body:
    ```json
    {
      "icp_data": {
        "product_details": {
          "product_name": "Premium Bus Travel Services",
          "product_category": "Travel & Tourism",
          "usps": ["Luxury fleet", "Custom packages"],
          "pain_points_solved": ["Complex logistics", "High costs"]
        },
        "icp_information": {
          "target_industry": ["Corporate Companies", "Wedding Planners"],
          "company_size": "10-500 employees",
          "decision_maker_persona": ["HR Manager", "Event Coordinator"],
          "region": ["India", "Major Cities"],
          "budget_range": "$5,000-$25,000 annually"
        }
      },
      "selected_scrapers": ["web_scraper", "instagram", "linkedin"]
    }
    ```
  - Response:
    ```json
    {
      "success": true,
      "data": {
        "pipeline_id": "gen_abc123xyz",
        "status": "started",
        "message": "Lead generation pipeline initiated",
        "icp_identifier": "premium-bus-travel_20240918_1234_a1b2c3d4"
      }
    }
    ```

#### 5. Direct Lead Generation
- **POST** `/api/lead-generation/direct`
  - Run lead generation using existing URLs
  - Request Body:
    ```json
    {
      "scraper_selections": {
        "web_scraper": 10,
        "instagram": 5,
        "linkedin": 5
      },
      "icp_identifier": "premium-bus-travel_20240918_1234_a1b2c3d4"
    }
    ```

#### 6. Lead Processing
- **POST** `/api/leads/filter`
  - Filter and process raw leads
  - Request Body (optional):
    ```json
    {
      "query_filter": {},
      "batch_size": 50
    }
    ```

- **POST** `/api/leads/enhance`
  - Enhance leads with additional contact information
  - Request Body (optional):
    ```json
    {
      "limit": 0,
      "batch_size": 20
    }
    ```

#### 7. Data Retrieval
- **GET** `/api/leads/available-urls`
  - Get count of available unprocessed URLs by type
  - Response:
    ```json
    {
      "success": true,
      "data": {
        "web_scraper": 42,
        "instagram": 15,
        "linkedin": 28,
        "youtube": 10
      }
    }
    ```

- **GET** `/api/leads/icp/<icp_identifier>`
  - Get leads filtered by ICP identifier
  - Response includes paginated list of leads

- **GET** `/api/leads/icp/<icp_identifier>/stats`
  - Get statistics for a specific ICP
  - Response includes lead counts, source distribution, and data quality metrics
```

**Response:**
```json
{
  "success": true,
  "data": {
    "pipeline_metadata": {
      "execution_time_seconds": 245.67,
      "start_time": "2025-01-15T10:30:00",
      "end_time": "2025-01-15T10:34:05",
      "selected_scrapers": ["web_scraper", "instagram", "linkedin"],
      "total_queries_generated": 9,
      "total_urls_collected": 156,
      "successful_scrapers": 3,
      "total_scrapers": 3
    },
    "url_collection": {
      "classified_urls_count": {
        "general": 89,
        "instagram": 23,
        "linkedin": 31,
        "youtube": 13
      },
      "total_urls": 156
    },
    "scraper_results_summary": {
      "web_scraper": {
        "status": "success",
        "leads_found": 45,
        "urls_processed": 10
      },
      "instagram": {
        "status": "success", 
        "profiles_found": 12,
        "success_rate": 0.85
      },
      "linkedin": {
        "status": "success",
        "profiles_found": 18,
        "failed_scrapes": 2
      }
    },
    "report_file": "orchestration_report_20250115_103405.json",
    "queries_used": [
      "corporate team building outings bus travel",
      "wedding destination travel packages India",
      "..."
    ]
  }
}
```

#### Generate Search Queries Only
```http
POST /api/queries/generate
```
**Request Body:**
```json
{
  "icp_data": { /* same as above */ },
  "selected_scrapers": ["web_scraper", "instagram"]
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "queries": [
      "corporate team building outings bus travel",
      "wedding destination travel packages India",
      "family reunion vacation planning services",
      "..."
    ],
    "total_queries": 9,
    "selected_scrapers": ["web_scraper", "instagram"]
  }
}
```

#### Get System Status
```http
GET /api/status
```
**Response:**
```json
{
  "success": true,
  "data": {
    "system_status": "operational",
    "components": {
      "gemini_ai": {
        "available": true,
        "status": "connected"
      },
      "mongodb": {
        "available": true,
        "status": "connected"
      },
      "scrapers": {
        "web_scraper": true,
        "instagram": true,
        "linkedin": true,
        "youtube": true
      }
    },
    "timestamp": "2025-01-15T10:30:00"
  }
}
```
# API Reference

### Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/api/scrapers` | List available scrapers |
| GET | `/api/icp/template` | Get ICP data template |
| POST | `/api/lead-generation/run` | Run complete pipeline |
| POST | `/api/queries/generate` | Generate queries only |
| GET | `/api/status` | System status |

## Installation

### Prerequisites
- Python 3.8+
- MongoDB instance
- Google Gemini AI API key
- Chrome/Chromium browser (for scrapers)

### Setup Steps

1. **Clone the repository**
```bash
git clone <repository-url>
cd lead-generation-app
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Install additional packages**
```bash
pip install google-generativeai python-dotenv flask flask-cors
```

4. **Set up environment variables**
Create a `.env` file:
```env
GEMINI_API_KEY=your_gemini_api_key_here

GOOGLE_API_KEY=you_google_client_key
GOOGLE_SEARCH_ENGINE_ID=your_google_search_engine_id

MONGODB_URI=mongodb://localhost:27017/
MONGODB_DATABASE_NAME=lead_generation_db

FLASK_ENV=development
PORT=5000
```

5. **Install Chrome WebDriver**
```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install -y google-chrome-stable

# Or download ChromeDriver manually
# Place in PATH or specify path in scraper configs
```

### MongoDB Setup
1. Install MongoDB locally or use MongoDB Atlas
2. Create a database named `lead_generation`
3. The application will automatically create necessary collections

### Gemini AI Setup
1. Visit [Google AI Studio](https://makersuite.google.com/app/apikey)
2. Generate an API key
3. Add the key to your `.env` file

## Configuration

### MongoDB Configuration
The application expects MongoDB to be running on the default port (27017). Update the connection string in your environment variables if using a different setup.

### Scraper Configuration
Each scraper module has its own configuration options:
- **Headless mode**: Enabled by default for production
- **Anti-detection**: Enabled to bypass bot detection
- **Rate limiting**: Built-in delays between requests
- **Error handling**: Automatic retries with exponential backoff

### AI Configuration
Configure the Gemini AI model in the orchestrator:
- Model: `gemini-2.0-flash`
- Temperature: Default (configurable)
- Max tokens: Default (configurable)

## Usage

### Running the Flask API

1. **Start the Flask server**
```bash
python app.py
```

2. **Access the API**
The server will start on `http://localhost:5000`

### Using the CLI Interface

1. **Run the orchestrator directly**
```bash
python main.py
```

2. **Follow the interactive prompts**
- Select scrapers to use
- Monitor progress in real-time
- Review results in generated reports

### API Usage Examples

#### Complete Pipeline via API
```python
import requests

# Define your ICP data
icp_data = {
    "product_details": {
        "product_name": "Premium Bus Travel Services",
        # ... other product details
    },
    "icp_information": {
        "target_industry": ["Corporate Companies", "Wedding Planners"],
        # ... other ICP details
    }
}

# Run the complete pipeline
response = requests.post(
    "http://localhost:5000/api/lead-generation/run",
    json={
        "icp_data": icp_data,
        "selected_scrapers": ["web_scraper", "instagram", "linkedin"]
    }
)

results = response.json()
print(f"Pipeline completed in {results['data']['pipeline_metadata']['execution_time_seconds']} seconds")
```

#### Generate Queries Only
```python
response = requests.post(
    "http://localhost:5000/api/queries/generate",
    json={
        "icp_data": icp_data,
        "selected_scrapers": ["web_scraper", "instagram"]
    }
)

queries = response.json()['data']['queries']
print(f"Generated {len(queries)} search queries")
```


## Project Structure

```
lead-generation-app/
├── main.py                    # Main orchestrator
├── app.py                     # Flask API server
│   ├── main.py
│   └── README.md
├── web_scraper/           # General web scraping module
│   ├── ai_integration/    # AI components
│   ├── data_models/       # Data models
│   ├── extractors/        # Data extraction logic
│   ├── processors/        # Data processing pipelines
│   ├── main.py
│   └── README.md
├── web_url_scraper/       # URL collection module
│   ├── database_service.py
│   ├── main.py
│   └── README.md
├── yt_scraper/            # YouTube scraping module
│   ├── src/
│   ├── main.py
│   └── README.md
├── app.py                 # Flask API entry point
├── contact_scraper.py     # Contact information extraction
├── filter_web_lead.py     # Lead filtering and processing
├── main.py                # Main orchestration script
├── requirements.txt        # Python dependencies
└── README.md              # This file
```

## Key Features

### AI-Powered Query Generation
- Uses Google Gemini AI to analyze ICP data
- Generates contextually relevant search queries
- Adapts queries based on target industries and personas
- Falls back to predefined queries if AI is unavailable

### Multi-Platform Data Collection
- **Web Scraper**: General websites and business directories
- **Instagram**: Social media profiles and engagement data
- **LinkedIn**: Professional networks and company information
- **YouTube**: Content creators and channel analytics

### Intelligent URL Classification
- Automatically routes URLs to appropriate scrapers
- Supports multiple domain patterns per platform
- Handles edge cases and unknown domains gracefully

### Robust Error Handling
- Individual scraper failures don't stop the pipeline
- Automatic retries with exponential backoff
- Comprehensive logging at all levels
- Graceful degradation when services are unavailable

### Data Persistence
- MongoDB for scalable data storage
- JSON exports for backup and analysis
- Automatic deduplication of URLs and leads
- Historical data tracking

## Error Handling

### Common Error Scenarios

1. **Gemini AI Unavailable**
   - Falls back to predefined queries
   - Logs warning but continues execution

2. **MongoDB Connection Failed**
   - Continues with file-based storage
   - Warns user about data persistence limitations

3. **Individual Scraper Failures**
   - Logs error details
   - Continues with other scrapers
   - Reports failure in final summary

4. **Network Issues**
   - Implements retry logic with exponential backoff
   - Times out gracefully after maximum attempts
   - Preserves partial results

### API Error Responses

```json
{
  "success": false,
  "error": "Detailed error message",
  "error_type": "validation|system|network",
  "timestamp": "2025-01-15T10:30:00"
}
```

## Performance Considerations

### Rate Limiting
- 2-second delays between search queries
- Platform-specific rate limiting for scrapers
- Configurable batch sizes for URL processing

### Resource Management
- Headless browser mode for reduced memory usage
- Automatic cleanup of browser instances
- Limited concurrent scraper instances

### Scalability
- Asynchronous operation support
- Modular scraper architecture
- Database connection pooling
- Horizontal scaling potential

## Security Features

### Anti-Detection Measures
- Randomized user agents
- Proxy support (configurable)
- Human-like interaction patterns
- Session management

### Data Protection
- No storage of sensitive authentication data
- Configurable data retention policies
- Secure API endpoints
- Input validation and sanitization

## Monitoring and Logging

### Log Levels
- **INFO**: Normal operation status
- **WARNING**: Non-critical issues (fallbacks, retries)
- **ERROR**: Failed operations with error details

### Metrics Tracked
- Query generation success rate
- URL collection statistics
- Scraper success rates
- Pipeline execution time
- Data quality metrics

## Future Enhancements

### Planned Features
1. **Dynamic ICP Management**
   - Web form for ICP configuration
   - Multiple ICP profiles support
   - ICP validation and optimization

2. **Advanced AI Integration**
   - Lead qualification scoring
   - Sentiment analysis of scraped content
   - Automated follow-up suggestions

3. **Enhanced Data Processing**
   - Real-time duplicate detection
   - Lead enrichment from multiple sources
   - Export to CRM systems

4. **Monitoring Dashboard**
   - Real-time pipeline status
   - Performance analytics
   - Data quality metrics

### Extensibility
- Plugin architecture for new scrapers
- Configurable data processing pipelines
- Custom export formats
- Third-party integrations

## Troubleshooting

### Common Issues

1. **Scrapers failing to start**
   - Check Chrome/ChromeDriver installation
   - Verify display configuration (for headless mode)
   - Check system resources and memory

2. **MongoDB connection errors**
   - Verify MongoDB service is running
   - Check connection string format
   - Ensure database permissions

3. **Gemini AI errors**
   - Verify API key is correctly set
   - Check network connectivity
   - Monitor API quota usage

4. **Low URL collection**
   - Adjust search queries for broader results
   - Check web_url_scraper configuration
   - Verify search engine accessibility

### Debug Mode
Enable detailed logging:
```python
logging.basicConfig(level=logging.DEBUG)
```

### Performance Monitoring
Track execution with timing logs and monitor system resources during scraping operations.

## Contributing

When adding new scrapers or modifying the pipeline:

1. Follow the existing scraper interface patterns
2. Implement proper error handling and logging
3. Add MongoDB integration
4. Update the orchestrator routing logic
5. Document new configuration options
6. Add appropriate tests

## License

This project is licensed under the MIT License - see the LICENSE file for details.

---

**Note**: This application is designed for legitimate business lead generation purposes. Please ensure compliance with all applicable laws and platform terms of service when using this software.