# MongoDB Database Integration for Lead Generation

This module provides MongoDB database storage for all scrapers in the Lead Generation Application. Instead of saving leads to JSON files, the scrapers now save data directly to MongoDB collections for better data management, querying, and scalability.

## Features

- **Centralized Database**: All scrapers save to the same MongoDB database
- **Separate Collections**: Each scraper has its own collection for organized data storage
- **Automatic Indexing**: Performance-optimized indexes for common queries
- **Duplicate Prevention**: Unique indexes prevent duplicate entries based on URLs
- **Metadata Tracking**: Automatic timestamp and source tracking for all leads
- **Batch Operations**: Efficient batch insertion for multiple leads
- **Search & Export**: Comprehensive search and export utilities

## Database Structure

### Database Name
- `lead_generation_db`

### Collections
1. **instagram_leads** - Instagram profile and post data
2. **linkedin_leads** - LinkedIn profile and company data  
3. **web_leads** - General web scraping lead data
4. **youtube_leads** - YouTube channel and video data

### Common Fields
All collections include these standard fields:
- `_id`: MongoDB ObjectId
- `url`: Source URL (unique index)
- `scraped_at`: Timestamp when data was scraped
- `source`: Scraper source identifier

## Installation

### Prerequisites
1. **MongoDB Server**: Install and run MongoDB locally or use MongoDB Atlas
2. **Python Dependencies**: Install required packages

### Setup

1. **Install MongoDB Dependencies**:
```bash
pip install pymongo>=4.5.0 dnspython>=2.4.0
```

2. **Configure MongoDB Connection**:
   - **Local MongoDB**: Default connection string is `mongodb://localhost:27017/`
   - **MongoDB Atlas**: Set environment variable `MONGODB_URI` with your connection string
   - **Custom Configuration**: Pass connection string directly to MongoDBManager

3. **Environment Variables** (optional):
```bash
export MONGODB_URI="mongodb://username:password@host:port/database"
```

## Usage

### Basic Usage

All scrapers now automatically save to MongoDB by default. The existing JSON file saving is maintained as a backup.

```python
# Instagram Scraper
from instagram_scraper.main import InstagramScraper

scraper = InstagramScraper(use_mongodb=True)  # Default: True
result = await scraper.scrape(urls)

# LinkedIn Scraper  
from linkedin_scraper.main import LinkedInScraperMain

scraper = LinkedInScraperMain(use_mongodb=True)  # Default: True
result = await scraper.scrape_async(urls)

# YouTube Scraper
from yt_scraper.main import YouTubeScraperInterface

scraper = YouTubeScraperInterface(use_mongodb=True)  # Default: True
await scraper.scrape_multiple_urls(urls)

# Web Scraper
from web_scraper.main_app import WebScraperOrchestrator

orchestrator = WebScraperOrchestrator(use_mongodb=True)  # Default: True
results = orchestrator.run_complete_pipeline(urls=urls)
```

### Database Management

Use the database utilities for advanced operations:

```python
from database.db_utils import DatabaseUtils

db_utils = DatabaseUtils()

# Get database statistics
stats = db_utils.get_database_stats()
print(f"Total leads: {stats['total_leads']}")

# Search leads
results = db_utils.search_leads(
    query="tech company",
    source="linkedin",
    limit=50
)

# Export leads
output_file = db_utils.export_leads(
    source="instagram",
    format="csv",
    date_from="2024-01-01"
)

# Get recent leads
recent = db_utils.get_recent_leads(hours=24, source="web")

# Find duplicates
duplicates = db_utils.get_duplicate_leads(source="linkedin")

# Cleanup old data
deleted = db_utils.cleanup_old_leads(days=30)
```

### Command Line Interface

```bash
# Get database statistics
python database/db_utils.py --action stats

# Search leads
python database/db_utils.py --action search --query "tech" --source linkedin

# Export leads
python database/db_utils.py --action export --source instagram --format csv

# Get recent leads
python database/db_utils.py --action recent --hours 48 --source web

# Find duplicates
python database/db_utils.py --action duplicates --source linkedin

# Cleanup old data
python database/db_utils.py --action cleanup --days 60
```

## Data Schema

### Instagram Leads
```json
{
  "_id": "ObjectId",
  "url": "https://www.instagram.com/username/",
  "content_type": "profile|article|video",
  "username": "username",
  "full_name": "Full Name",
  "followers_count": "100K",
  "following_count": "500",
  "biography": "Bio text",
  "bio_links": [...],
  "is_private": false,
  "is_verified": false,
  "is_business_account": true,
  "business_email": "email@domain.com",
  "business_phone_number": "+1234567890",
  "business_category_name": "Category",
  "scraped_at": "2024-01-01T12:00:00Z",
  "source": "instagram_scraper"
}
```

### LinkedIn Leads
```json
{
  "_id": "ObjectId", 
  "url": "https://www.linkedin.com/in/username/",
  "url_type": "profile|company|post",
  "username": "username",
  "full_name": "Full Name",
  "job_title": ["Title1", "Title2"],
  "title": "Current Title",
  "followers": 1000,
  "connections": 500,
  "about": "About text",
  "location": "City, Country",
  "website": "https://website.com",
  "contact_info": {...},
  "scraping_timestamp": 1704067200,
  "scraped_at": "2024-01-01T12:00:00Z",
  "source": "linkedin_scraper"
}
```

### Web Leads
```json
{
  "_id": "ObjectId",
  "url": "https://website.com",
  "domain": "website.com",
  "business_name": "Company Name",
  "contact_person": "Contact Name",
  "email": "email@domain.com",
  "phone": "+1234567890",
  "address": "Full Address",
  "website": "https://website.com",
  "social_media": {...},
  "services": ["Service1", "Service2"],
  "industry": "Industry",
  "scraped_at": "2024-01-01T12:00:00Z",
  "source": "web_scraper"
}
```

### YouTube Leads
```json
{
  "_id": "ObjectId",
  "url": "https://www.youtube.com/watch?v=VIDEO_ID",
  "content_type": "video|channel",
  "title": "Video Title",
  "channel_name": "Channel Name",
  "upload_date": "Oct 25, 2009",
  "views": "100K",
  "subscribers": "1M",
  "social_media_handles": {...},
  "email": ["email1@domain.com"],
  "description": "Video description",
  "scraped_at": "2024-01-01T12:00:00Z",
  "source": "youtube_scraper"
}
```

## Performance Optimization

### Indexes
The following indexes are automatically created for optimal performance:

- **URL Index**: Unique index on `url` field for duplicate prevention
- **Username Index**: Index on `username` field for user-based queries
- **Timestamp Index**: Index on `scraped_at` field for time-based queries
- **Content Type Index**: Index on `content_type` field for filtering
- **Domain Index**: Index on `domain` field for web leads

### Connection Pooling
- Default pool size: 100 connections
- Configurable via `max_pool_size` parameter
- Automatic connection management

## Error Handling

The MongoDB integration includes comprehensive error handling:

- **Connection Failures**: Graceful fallback to file-based storage
- **Duplicate Key Errors**: Automatic handling of duplicate URLs
- **Insertion Failures**: Detailed logging of failed insertions
- **Batch Operations**: Partial success reporting for batch operations

## Migration from JSON Files

If you have existing JSON files, you can import them to MongoDB:

```python
import json
from database.mongodb_manager import get_mongodb_manager

mongodb_manager = get_mongodb_manager()

# Load JSON file
with open('instagram_leads.json', 'r') as f:
    leads = json.load(f)

# Import to MongoDB
stats = mongodb_manager.insert_batch_leads(leads, 'instagram')
print(f"Imported {stats['success_count']} leads")
```

## Monitoring and Maintenance

### Database Statistics
```python
from database.db_utils import DatabaseUtils

db_utils = DatabaseUtils()
stats = db_utils.get_database_stats()

print(f"Total leads: {stats['total_leads']}")
print(f"Instagram leads: {stats['instagram']}")
print(f"LinkedIn leads: {stats['linkedin']}")
print(f"Web leads: {stats['web']}")
print(f"YouTube leads: {stats['youtube']}")
```

### Regular Maintenance
```bash
# Cleanup old data (older than 30 days)
python database/db_utils.py --action cleanup --days 30

# Find and review duplicates
python database/db_utils.py --action duplicates

# Export data for backup
python database/db_utils.py --action export --format json
```

## Troubleshooting

### Common Issues

1. **Connection Failed**:
   - Check if MongoDB server is running
   - Verify connection string
   - Check network connectivity

2. **Permission Denied**:
   - Ensure MongoDB user has write permissions
   - Check database access rights

3. **Duplicate Key Errors**:
   - Normal behavior for duplicate URLs
   - Check logs for duplicate counts

4. **Performance Issues**:
   - Monitor index usage
   - Consider increasing connection pool size
   - Review query patterns

### Logging
Enable detailed logging by setting the log level:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Security Considerations

1. **Connection Security**: Use SSL/TLS for production MongoDB connections
2. **Authentication**: Implement proper MongoDB authentication
3. **Network Security**: Restrict MongoDB access to trusted networks
4. **Data Privacy**: Ensure compliance with data protection regulations

## Support

For issues or questions:
1. Check the troubleshooting section
2. Review MongoDB logs
3. Verify scraper configurations
4. Test with minimal data sets
