# LinkedIn Data Scraper

A comprehensive LinkedIn data extraction tool that uses browser automation with JSON-LD extraction as the primary data source. This scraper can extract data from LinkedIn profiles, companies, posts, and newsletters with high accuracy and anti-detection capabilities.

## Features

- **Multi-type URL Support**: Profiles, companies, posts, and newsletters
- **JSON-LD Primary Extraction**: Uses structured data for maximum accuracy
- **Anti-Detection**: Advanced browser fingerprint masking and human-like behavior
- **Fallback Data Sources**: Meta tags and OpenGraph data when JSON-LD unavailable
- **Network Monitoring**: Captures and analyzes LinkedIn API requests
- **Popup Handling**: Automatically closes LinkedIn login/signup popups
- **Structured Output**: Clean, organized JSON output with comprehensive metadata

## Supported LinkedIn URL Types

| URL Type | Example | Extracted Data |
|----------|---------|----------------|
| **Profile** | `linkedin.com/in/username` | Name, job title, followers, location, about |
| **Company** | `linkedin.com/company/name` | Name, description, employee count, address |
| **Post** | `linkedin.com/posts/...` | Headline, author, comments, likes, date |
| **Newsletter** | `linkedin.com/newsletters/...` | Name, description, author, publication date |

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd linkedin-scraper

# Install required dependencies
pip install asyncio beautifulsoup4 playwright

# Install Playwright browsers
playwright install chromium
```

## Quick Start

### Simple Function Usage (Recommended)

```python
from main import linkedin_scraper

# Define URLs to scrape
urls = [
    "https://www.linkedin.com/in/williamhgates/",
    "https://www.linkedin.com/company/microsoft/",
    "https://www.linkedin.com/posts/username_post-activity-id",
    "https://www.linkedin.com/newsletters/newsletter-name-id"
]

# Scrape data (one line!)
results = linkedin_scraper(urls)

# Results are automatically saved to "linkedin_scraped_data.json"
print(f"Scraped {len(results['scraped_data'])} profiles successfully!")
```

### Class-Based Usage

```python
from main import LinkedInScraper

scraper = LinkedInScraper(headless=True)
results = scraper.scrape(urls, output_filename="custom_output.json")
```

### Advanced Usage with Custom Settings

```python
from linkedin_data_extractor_v1 import LinkedInDataExtractor

async def custom_scraping():
    extractor = LinkedInDataExtractor(
        headless=False,  # Show browser window
        enable_anti_detection=True,
        is_mobile=False
    )
    
    await extractor.start()
    
    # Extract data from specific URL
    data = await extractor.extract_linkedin_data("https://www.linkedin.com/in/username")
    
    # Save structured data
    await extractor.save_linkedin_data_to_json(data, "profile_data.json")
    
    await extractor.stop()

# Run async function
import asyncio
asyncio.run(custom_scraping())
```

## Output Structure

The scraper generates structured JSON output with the following format:

```json
{
  "scraping_metadata": {
    "timestamp": 1699123456.789,
    "date": "2024-01-15 14:30:45",
    "total_urls": 4,
    "successful_scrapes": 3,
    "failed_scrapes": 1,
    "scraper_version": "linkedin_scraper_main_v1.0"
  },
  "scraped_data": [
    {
      "url": "https://www.linkedin.com/in/username",
      "url_type": "profile",
      "username": "username",
      "full_name": "John Doe",
      "job_title": "Software Engineer",
      "followers": 1500,
      "connections": 500,
      "about": "Experienced software engineer...",
      "location": "San Francisco, CA",
      "website": "https://johndoe.com",
      "scraping_timestamp": 1699123456.789,
      "scraping_date": "2024-01-15 14:30:45"
    }
  ],
  "failed_urls": [
    {
      "url": "https://www.linkedin.com/in/private-profile",
      "error": "Profile not accessible"
    }
  ]
}
```

### Profile Data Fields

```json
{
  "username": "linkedin-username",
  "full_name": "Full Name",
  "job_title": "Current Job Title",
  "title": "Profile Title",
  "followers": 1500,
  "connections": 500,
  "about": "Profile description/summary",
  "location": "City, Country",
  "website": "https://website.com",
  "contact_info": {}
}
```

### Company Data Fields

```json
{
  "username": "company-slug",
  "full_name": "Company Name",
  "address": "Street, City, State, ZIP, Country",
  "website": "https://company.com",
  "about_us": "Company description",
  "employee_count": 50000
}
```

### Post Data Fields

```json
{
  "url": "https://linkedin.com/posts/...",
  "headline": "Post title/headline",
  "author_url": "https://linkedin.com/in/author",
  "author_name": "Author Name",
  "full_name": "Author Name",
  "comment_count": 25,
  "likes_count": 150,
  "followers": 1000,
  "date_published": "2024-01-15"
}
```

### Newsletter Data Fields

```json
{
  "username": "newsletter-id",
  "full_name": "Newsletter Name",
  "description": "Newsletter description",
  "author_name": "Author Name",
  "date_published": "2024-01-15"
}
```

## Configuration Options

### LinkedInDataExtractor Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `headless` | bool | True | Run browser in headless mode |
| `enable_anti_detection` | bool | True | Enable anti-detection features |
| `is_mobile` | bool | False | Use mobile user agent |

### linkedin_scraper Function Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `urls` | List[str] | Required | List of LinkedIn URLs to scrape |
| `output_filename` | str | "linkedin_scraped_data.json" | Output JSON filename |
| `headless` | bool | True | Run browser in headless mode |

## Data Extraction Methods

### 1. JSON-LD Extraction (Primary)

The scraper primarily extracts data from JSON-LD structured data embedded in LinkedIn pages:

```html
<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@type": "Person",
  "name": "John Doe",
  "jobTitle": "Software Engineer",
  ...
}
</script>
```

### 2. Meta Tags (Secondary)

Falls back to OpenGraph and Twitter meta tags:

```html
<meta property="og:title" content="John Doe - Software Engineer">
<meta property="og:description" content="Profile description">
```

### 3. Network Request Monitoring

Monitors LinkedIn API requests for additional data:

- `/voyager/api/` endpoints
- Profile and company API responses
- Real-time interaction data

## Anti-Detection Features

- **Browser Fingerprint Masking**: Randomized user agents, screen resolutions
- **Human-like Behavior**: Random delays, mouse movements, scrolling
- **Popup Handling**: Automatic dismissal of LinkedIn login prompts
- **Request Throttling**: Configurable delays between requests
- **Stealth Mode**: Advanced evasion techniques
- **Retry mechanism**: It reduce sign-up page encounters by making requests appear as legitimate referrals from Google search results rather than direct bot access, especially for "profile" type.

## Error Handling

The scraper includes comprehensive error handling:

```python
# Automatic retry on failures
# Graceful handling of private/inaccessible profiles
# Detailed error reporting in output JSON
# Cleanup of browser resources
```

## Testing

Run the included test suite:

```bash
python linkedin_data_extractor_v1.py
```

This will test all supported URL types:
- Profile: Bill Gates
- Company: Microsoft
- Post: Sample LinkedIn post
- Newsletter: Sample newsletter

## File Structure

```
linkedin-scraper/
├── main.py                      # Main scraper interface
├── linkedin_data_extractor_v1.py # Core extraction engine
├── browser_manager.py           # Browser automation (not shown)
├── README.md                    # This file
├── linkedin_scraped_data.json   # Output file (generated)
└── requirements.txt             # Dependencies
```

## API Reference

### linkedin_scraper(urls, output_filename="linkedin_scraped_data.json", headless=True)

Main function to scrape LinkedIn URLs.

**Parameters:**
- `urls` (List[str]): LinkedIn URLs to scrape
- `output_filename` (str): Output JSON filename
- `headless` (bool): Run browser in headless mode

**Returns:**
- `Dict[str, Any]`: Scraping results with metadata

### LinkedInDataExtractor Class

Core extraction engine with advanced features.

**Methods:**
- `start()`: Initialize browser and network monitoring
- `extract_linkedin_data(url)`: Extract data from specific URL
- `save_linkedin_data_to_json(data, filename)`: Save data to JSON
- `stop()`: Clean up browser resources

## Limitations

- **Rate Limiting**: LinkedIn may rate limit requests
- **Private Profiles**: Cannot access private/restricted profiles
- **Login Required**: Some data requires LinkedIn login
- **Dynamic Content**: Some content loads via JavaScript
- **Terms of Service**: Use responsibly and respect LinkedIn's ToS

## Best Practices

1. **Respect Rate Limits**: Add delays between requests
2. **Handle Errors**: Always check for failed scrapes
3. **Data Validation**: Verify extracted data accuracy
4. **Privacy**: Respect user privacy and data protection laws
5. **Monitoring**: Monitor for anti-bot measures

## Troubleshooting

### Common Issues

**Browser not starting:**
```bash
playwright install chromium
```

**Empty data extraction:**
- Check if profile is public
- Verify URL format
- Enable `headless=False` to debug

**Rate limiting:**
- Increase delays between requests
- Use different IP addresses
- Respect LinkedIn's usage limits

### Debug Mode

Enable debug mode to see browser window:

```python
results = linkedin_scraper(urls, headless=False)
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new features
4. Submit a pull request

## License

This project is for educational purposes only. Users are responsible for complying with LinkedIn's Terms of Service and applicable laws.

## Changelog

### v1.0
- Initial release with JSON-LD extraction
- Support for profiles, companies, posts, newsletters
- Anti-detection features
- Comprehensive error handling

---

**⚠️ Disclaimer**: This tool is for educational and research purposes only. Users must comply with LinkedIn's Terms of Service and all applicable laws. The authors are not responsible for any misuse of this software.