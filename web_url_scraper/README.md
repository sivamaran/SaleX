# Google URL Scraper MVP

A Python application that automatically scrapes URLs from Google search results using the Google Custom Search API and stores them in MongoDB.

## Features

- 🔍 **Google Search Integration**: Uses Google Custom Search API for reliable search results
- 📄 **Multi-page Scraping**: Supports pagination to get more results (up to 3 pages in MVP)
- 🗄️ **MongoDB Storage**: Stores URLs with metadata in MongoDB database
- 🔄 **Duplicate Prevention**: Automatically prevents duplicate URL storage
- ✅ **URL Validation**: Filters out invalid URLs before storage
- 🏷️ **URL Type Detection**: Automatically categorizes URLs by type (social media, general)
- 📊 **URL Type Analytics**: Provides statistics and filtering by URL types
- 🖥️ **Command Line Interface**: Easy-to-use CLI for running searches

## URL Types

The application automatically detects and categorizes URLs into the following types:

- **Instagram**: URLs containing `instagram.com`
- **Facebook**: URLs containing `facebook.com`
- **Reddit**: URLs containing `reddit.com`
- **Quora**: URLs containing `quora.com`
- **Twitter**: URLs containing `twitter.com` or `x.com`
- **LinkedIn**: URLs containing `linkedin.com`
- **General**: All other URLs (websites, blogs, etc.)

URL type detection happens automatically during the scraping process and is stored in the database for easy filtering and analysis.

## Prerequisites

- Python 3.8 or higher
- MongoDB database (local or cloud)
- Google Custom Search API key
- Google Custom Search Engine ID

## Installation

1. **Clone or download the project files**

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables:**
   Create a `.env` file in the project root with the following variables:
   ```
   GOOGLE_API_KEY=your_google_api_key_here
   GOOGLE_SEARCH_ENGINE_ID=your_search_engine_id_here
   MONGODB_URI=mongodb://localhost:27017/
   MONGODB_DATABASE_NAME=url_scraper
   MONGODB_COLLECTION_NAME=scraped_urls
   ```

## Setup Instructions

### 1. Google Custom Search API Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing one
3. Enable the "Custom Search API"
4. Create credentials (API Key)
5. Copy the API key to your `.env` file

### 2. Google Custom Search Engine Setup

1. Go to [Google Programmable Search Engine](https://programmablesearchengine.google.com/)
2. Create a new search engine
3. Configure it to search the entire web
4. Copy the Search Engine ID to your `.env` file

### 3. MongoDB Setup

1. Install MongoDB locally or use MongoDB Atlas (cloud)
2. Update the `MONGODB_URI` in your `.env` file
3. The application will automatically create the database and collection

## Testing

Before using the application, you should test that everything is working correctly.

### Quick Test (Recommended)

Run a quick test to verify basic functionality:

```bash
python quick_test.py
```

This will test:
- Configuration validation
- Database connection
- Google API connection
- URL validation

### Comprehensive Test

Run the full test suite to verify all components:

```bash
python test_app.py
```

This will test:
- Configuration validation
- Database connection and operations
- Google API connection and search
- URL validation and filtering
- Database operations (save, retrieve, duplicate prevention)
- Full workflow (search → filter → save)

### Manual Testing

You can also test individual components:

```python
# Test configuration
python -c "import config; print(config.validate_config())"

# Test database connection
python -c "import database_service; print(database_service.test_database_connection())"

# Test Google API
python -c "import google_service; results = google_service.search_google('python'); print(f'Found {len(results)} results')"
```

## Usage

### Basic Usage

Run the application and enter a search query when prompted:

```bash
python main.py
```

### Command Line Usage

Provide the search query as a command line argument:

```bash
python main.py "python programming tutorials"
```

### Using as a Module in Other Files

You can import and use the scraper functionality in your own Python files with just one line of code:

```python
from web_url_scraper.main import main

# Single line to run entire flow
success = main("your search query here")

# Or check the result
if main("python programming tutorials"):
    print("Search completed successfully!")
else:
    print("Search failed!")
```

The `main()` function handles the complete workflow:
- Input validation
- Google search execution
- URL filtering and validation
- Database storage
- Results summary and statistics

**Return Value:**
- Returns `True` if the entire process completes successfully
- Returns `False` if there's an error or no results found

**Prerequisites for Module Usage:**
- Ensure the application is properly initialized (database connection, config validation)
- All dependencies must be available
- The `.env` file must be configured correctly

### Example Output

```
Initializing Google URL Scraper...
----------------------------------------
Configuration validation passed!

Testing database connection...
MongoDB connection successful!
Database connection test successful!

Setting up database indexes...
Creating unique index on URL field...
Creating index on search_query field...
Creating index on created_at field...
Database indexes created successfully!

Configuration Summary:
  mongodb_uri: mongodb://localhost:27017/
  mongodb_database: url_scraper
  mongodb_collection: scraped_urls
  max_pages: 3
  results_per_page: 10

Application initialized successfully!
Using search query from command line: python programming tutorials
Starting search for: python programming tutorials
Executing Google search...
Starting multi-page search for: python programming tutorials (max pages: 3)
Processing page 1...
Searching Google for: python programming tutorials (start: 1)
Found 10 results
Processing page 2...
Searching Google for: python programming tutorials (start: 11)
Found 10 results
Processing page 3...
Searching Google for: python programming tutorials (start: 21)
Found 10 results
Total results found: 30
Found 30 total URLs
Filtering valid URLs...
Valid URLs remaining: 30
Valid URLs to process: 30
Saving URLs to database...
Processing 30 URLs for storage...
Storage complete: 30 new URLs, 0 duplicates skipped

==================================================
SEARCH COMPLETED SUCCESSFULLY
==================================================
Search Query: python programming tutorials
Total URLs Found: 30
Valid URLs: 30
New URLs Added: 30
Duplicates Skipped: 0
==================================================

Operation completed successfully!
```

## Project Structure

```
web_url_scraper/
├── .env                          # Environment variables
├── requirements.txt              # Python dependencies
├── config.py                     # Configuration management
├── google_service.py             # Google search functionality
├── database_service.py           # MongoDB operations
├── main.py                       # Main application entry point
├── test_url_types.py             # URL type detection tests
├── example_usage.py              # URL type functionality examples
└── README.md                     # This file
```

## Configuration

The application uses the following configuration variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `GOOGLE_API_KEY` | Your Google Custom Search API key | Required |
| `GOOGLE_SEARCH_ENGINE_ID` | Your Google Custom Search Engine ID | Required |
| `MONGODB_URI` | MongoDB connection string | `mongodb://localhost:27017/` |
| `MONGODB_DATABASE_NAME` | Database name | `url_scraper` |
| `MONGODB_COLLECTION_NAME` | Collection name | `scraped_urls` |

## Data Storage

Each URL is stored in MongoDB with the following structure:

```json
{
  "url": "https://example.com",
  "title": "Example Page Title",
  "snippet": "Page description snippet...",
  "url_type": "general",
  "search_query": "original search query",
  "created_at": "2024-01-01T12:00:00.000Z",
  "scraped_at": "2024-01-01T12:00:00.000Z"
}
```

### URL Type Field

The `url_type` field is automatically populated based on the URL domain:

- `"instagram"` - URLs from Instagram
- `"facebook"` - URLs from Facebook  
- `"reddit"` - URLs from Reddit
- `"quora"` - URLs from Quora
- `"twitter"` - URLs from Twitter/X
- `"linkedin"` - URLs from LinkedIn
- `"general"` - All other URLs

## URL Type Functions

The application provides several functions for working with URL types:

### Database Functions

```python
# Get all URLs of a specific type
instagram_urls = database_service.get_urls_by_type('instagram')

# Count URLs by type
facebook_count = database_service.count_urls_by_type('facebook')

# Get URL type statistics
stats = database_service.get_url_type_statistics()

# Get URLs by query and type
python_twitter = database_service.get_urls_by_query_and_type('python', 'twitter')

# Delete URLs by type
deleted_count = database_service.delete_urls_by_type('reddit')
```

### URL Type Detection

```python
# Detect URL type manually
url_type = google_service.detect_url_type('https://www.instagram.com/user123')
# Returns: 'instagram'
```

### Example Usage

```python
# Get statistics for all URL types
stats = database_service.get_url_type_statistics()
print(f"Total URLs: {stats['total_urls']}")
print("URL Type Breakdown:")
for url_type, count in stats['url_types'].items():
    print(f"  {url_type}: {count}")

# Filter URLs by type for analysis
social_media_urls = []
for url_type in ['instagram', 'facebook', 'twitter', 'reddit', 'quora', 'linkedin']:
    urls = database_service.get_urls_by_type(url_type)
    social_media_urls.extend(urls)
```

## Error Handling

The application includes comprehensive error handling for:

- Invalid API keys or search engine IDs
- Network connectivity issues
- Database connection problems
- Invalid search queries
- Duplicate URL handling

## Limitations (MVP)

- Limited to 3 pages of search results
- Basic URL validation
- No advanced duplicate detection
- No web interface
- No scheduled execution

## Troubleshooting

### Common Issues

1. **"Configuration validation failed"**
   - Check your `.env` file exists and has all required variables
   - Verify API key and search engine ID are correct

2. **"MongoDB connection failed"**
   - Ensure MongoDB is running
   - Check your MongoDB URI is correct
   - Verify network connectivity

3. **"Google API error"**
   - Check your API key is valid
   - Verify the Custom Search API is enabled
   - Check your API quota hasn't been exceeded

### Getting Help

If you encounter issues:

1. Check the error messages for specific details
2. Verify all prerequisites are met
3. Test your API key and search engine ID separately
4. Ensure MongoDB is accessible

## License

This project is for educational and development purposes.