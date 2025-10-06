# Instagram Scraper

A powerful, Instagram data extraction tool with comprehensive anti-detection measures, advanced network monitoring, and stealth browser automation. This system uses cutting-edge techniques to extract GraphQL data from Instagram profiles, posts, and reels while evading detection mechanisms.

## 🚀 Features

### Core Functionality
- **Multi-Content Support**: Extract data from profiles, posts, and reels
- **Anti-Detection System**: Advanced fingerprint evasion and behavioral mimicking
- **Network Monitoring**: Captures GraphQL and API requests for comprehensive data extraction
- **Clean Data Output**: Structured JSON output with formatted counts and business information
- **Batch Processing**: Process multiple URLs efficiently with error handling
- **Mobile Support**: Optional mobile user agent and viewport simulation

### Anti-Detection Features
- **Fingerprint Evasion**: Canvas, WebGL, audio, timezone, locale, screen, plugins, fonts, and hardware fingerprint randomization
- **Behavioral Mimicking**: Human-like scrolling, mouse movements, and click patterns
- **Network Obfuscation**: Request spacing, jitter, and connection pooling
- **Stealth Headers**: Realistic browser headers and user agents
- **Popup Handling**: Automatic Instagram popup detection and closure
- **Hardware Correlation**: Realistic hardware profiles with geographic logic
- **Mobile & Desktop Support**: Optimized for both mobile and desktop environments

### Data Extraction Capabilities
- **Profile Data**: Username, full name, followers, following, biography, verification status, business information
- **Post Data**: Caption, likes, comments, author, post date, media URLs
- **Reel Data**: Video information, views, duration, thumbnail URLs
- **Business Information**: Email, phone, category, professional account status
- **Network Analysis**: Request/response monitoring and GraphQL data capture

## 📋 Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage Examples](#usage-examples)
- [API Reference](#api-reference)
- [Configuration](#configuration)
- [Anti-Detection System](#anti-detection-system)
- [Data Output Format](#data-output-format)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## 🛠️ Installation

### Prerequisites
- Python 3.8+
- pip
- Git

### Setup
```bash
# Clone the repository
git clone <repository-url>
cd insta-scraper

# Create virtual environment
python -m venv insta-venv
source insta-venv/bin/activate  # On Windows: insta-venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium
```

### Dependencies
The scraper uses the following key dependencies:
- `playwright`: Browser automation
- `beautifulsoup4`: HTML parsing
- `fake-useragent`: User agent generation
- `asyncio`: Asynchronous programming
- `json`: Data serialization

## 🚀 Quick Start

### Basic Usage
```python
import asyncio
from main import scrape_instagram_urls

async def main():
    urls = [
        "https://www.instagram.com/username/",
        "https://www.instagram.com/p/post_id/",
        "https://www.instagram.com/reel/reel_id/"
    ]
    
    result = await scrape_instagram_urls(urls)
    print(f"Success: {result['success']}")
    print(f"Data entries: {len(result['data'])}")

asyncio.run(main())
```

### Command Line Usage
```bash
python main.py
```

## 📖 Usage Examples

### Example 1: Simple One-Line Usage
```python
from main import scrape_instagram_urls

urls = [
    "https://www.instagram.com/90svogue.__",
    "https://www.instagram.com/p/DMQMR4IzyJb/",
    "https://www.instagram.com/reel/CSb6-Rap2Ip/"
]

result = await scrape_instagram_urls(urls)
```

### Example 2: Custom Options
```python
result = await scrape_instagram_urls(
    urls=urls,
    headless=False,  # Show browser window
    enable_anti_detection=True,
    is_mobile=True,  # Use mobile mode
    output_file="custom_output.json"
)
```

### Example 3: Class-Based Usage
```python
from main import InstagramScraper

scraper = InstagramScraper(
    headless=True,
    enable_anti_detection=True,
    is_mobile=False,
    output_file="class_output.json"
)

result = await scraper.scrape(urls)
```

### Example 4: Batch Processing
```python
# Large batch with error handling
urls = [
    "https://www.instagram.com/90svogue.__",
    "https://www.instagram.com/p/DMQMR4IzyJb/",
    "https://www.instagram.com/reel/CSb6-Rap2Ip/",
    # ... more URLs
]

result = await scrape_instagram_urls(urls)

# Check results
if result['success']:
    print(f"✅ All URLs processed successfully")
    for entry in result['data']:
        print(f"  - {entry['content_type']}: {entry.get('username', 'N/A')}")
else:
    print(f"❌ Some errors occurred: {len(result['errors'])}")
```

## 🔧 API Reference

### Main Functions

#### `scrape_instagram_urls(urls, headless=True, enable_anti_detection=True, is_mobile=False, output_file=None)`
Convenience function for scraping Instagram URLs.

**Parameters:**
- `urls` (List[str]): List of Instagram URLs to scrape
- `headless` (bool): Run browser in headless mode (default: True)
- `enable_anti_detection` (bool): Enable anti-detection features (default: True)
- `is_mobile` (bool): Use mobile user agent and viewport (default: False)
- `output_file` (str, optional): File path to save results

**Returns:**
```python
{
    'success': bool,
    'data': List[Dict],
    'summary': Dict,
    'errors': List[Dict],
    'output_file': str,
    'stealth_report': Dict
}
```

#### `InstagramScraper` Class
Main scraper class with comprehensive functionality.

**Constructor:**
```python
InstagramScraper(
    headless: bool = True,
    enable_anti_detection: bool = True,
    is_mobile: bool = False,
    output_file: Optional[str] = None
)
```

**Methods:**
- `scrape(urls: List[str]) -> Dict[str, Any]`: Main scraping method with automatic profile discovery when processing article or video URLs, automatically extracts usernames and scrapes their profile data
- `_determine_content_type_from_url(url: str, data: Dict[str, Any]) -> str`: Determine content type
- `_format_count(count) -> str`: Format numbers to readable format

### Advanced Components

#### `AdvancedGraphQLExtractor` Class
Handles advanced data extraction with network monitoring.

**Key Methods:**
- `extract_graphql_data(url: str) -> Dict[str, Any]`: Extract data from URL
- `extract_user_profile_data(username: str) -> Dict[str, Any]`: Extract profile data
- `extract_post_data(post_id: str) -> Dict[str, Any]`: Extract post data
- `extract_reel_data(reel_id: str) -> Dict[str, Any]`: Extract reel data
- `get_stealth_report() -> Dict[str, Any]`: Get stealth status report

#### `BrowserManager` Class
Manages browser automation with anti-detection features.

**Key Methods:**
- `navigate_to_with_popup_close(url: str) -> bool`: Navigate and handle popups
- `get_page_content() -> str`: Get HTML content
- `get_rendered_text() -> str`: Get rendered text
- `execute_human_scroll(target_position: int) -> None`: Human-like scrolling
- `execute_human_mouse_move(x: int, y: int) -> None`: Human-like mouse movement
- `execute_human_click(x: int, y: int) -> None`: Human-like clicking

#### `AntiDetectionManager` Class
Comprehensive anti-detection system.

**Key Methods:**
- `generate_stealth_context_options(is_mobile: bool) -> Dict[str, Any]`: Generate stealth options
- `generate_stealth_scripts() -> List[str]`: Generate stealth scripts
- `generate_human_scroll_pattern(target_position: int) -> List[Dict[str, Any]]`: Human scroll patterns
- `get_stealth_report() -> Dict[str, Any]`: Get comprehensive stealth report

## ⚙️ Configuration

### Anti-Detection Configuration
```python
# Enable/disable specific anti-detection features
anti_detection = AntiDetectionManager(
    enable_fingerprint_evasion=True,    # Browser fingerprint randomization
    enable_behavioral_mimicking=True,   # Human-like behavior
    enable_network_obfuscation=True     # Network request obfuscation
)
```

### Browser Configuration
```python
# Browser manager options
browser_manager = BrowserManager(
    headless=True,                    # Run in background
    enable_anti_detection=True,       # Enable stealth features
    is_mobile=False                   # Desktop vs mobile mode
)
```

### Human Behavior Profiles
```python
# Customize human behavior patterns
human_profile = HumanBehaviorProfile(
    scroll_speed_range=(0.5, 2.0),      # Scroll speed variation
    mouse_speed_range=(100, 300),       # Mouse movement speed
    click_delay_range=(0.1, 0.5),       # Click delay variation
    pause_probability=0.15,             # Probability of pauses
    hesitation_probability=0.25,        # Probability of hesitations
    exploration_probability=0.1         # Probability of exploration
)
```

## 🛡️ Anti-Detection System

### Fingerprint Evasion
The scraper implements comprehensive fingerprint evasion:

- **Canvas Fingerprinting**: Randomizes canvas rendering
- **WebGL Fingerprinting**: Modifies WebGL parameters
- **Audio Fingerprinting**: Randomizes audio context
- **Timezone Fingerprinting**: Uses realistic timezone data
- **Locale Fingerprinting**: Simulates different locales
- **Screen Fingerprinting**: Varies screen resolution and color depth
- **Plugin Fingerprinting**: Randomizes plugin information
- **Font Fingerprinting**: Varies available fonts
- **Hardware Fingerprinting**: Modifies hardware concurrency

### Behavioral Mimicking
Human-like behavior simulation:

- **Scrolling Patterns**: Variable speed with pauses and hesitations
- **Mouse Movements**: Natural mouse movement curves
- **Click Patterns**: Realistic click timing and positioning
- **Page Interaction**: Random exploration and pauses
- **Request Timing**: Human-like request spacing

### Network Obfuscation
Network request obfuscation:

- **Request Spacing**: Variable delays between requests
- **Jitter Factor**: Adds randomness to timing
- **Connection Pooling**: Manages connection reuse
- **Header Randomization**: Varies request headers
- **User Agent Rotation**: Rotates user agents

## 📊 Data Output Format

### Profile Data Structure
```json
{
  "url": "https://www.instagram.com/username/",
  "content_type": "profile",
  "full_name": "User Full Name",
  "username": "username",
  "followers_count": "1.2K",
  "following_count": "500",
  "biography": "User biography text...",
  "bio_links": ["https://example.com"],
  "is_private": false,
  "is_verified": true,
  "is_business_account": false,
  "is_professional_account": true,
  "business_email": "user@example.com",
  "business_phone_number": "+1234567890",
  "business_category_name": "Business Category"
}
```

### Post/Reel Data Structure
```json
{
  "url": "https://www.instagram.com/p/post_id/",
  "content_type": "article",
  "likes_count": "1.5K",
  "comments_count": "234",
  "username": "username",
  "post_date": "July 18, 2025",
  "caption": "Post caption text..."
}
```

### Response Structure
```json
{
  "success": true,
  "data": [...],  // Array of extracted data
  "summary": {
    "total_original_urls": 3,
    "additional_profiles_extracted": 2,
    "total_extractions": 5,
    "successful_extractions": 5,
    "failed_extractions": 0,
    "success_rate": 100.0,
    "total_time_seconds": 45.2,
    "average_time_per_url": 15.1,
    "content_type_breakdown": {
      "profile": 3,
      "article": 1,
      "video": 1
    }
  },
  "errors": [],
  "output_file": "output.json",
  "stealth_report": {...}
}
```

## 🔍 Supported URL Types

### Profile URLs
- `https://www.instagram.com/username/`
- `https://www.instagram.com/username`

### Post URLs
- `https://www.instagram.com/p/post_id/`
- `https://www.instagram.com/p/post_id`

### Reel URLs
- `https://www.instagram.com/reel/reel_id/`
- `https://www.instagram.com/reel/reel_id`

## 🚨 Troubleshooting

### Common Issues

#### Browser Launch Failures
```bash
# Install Playwright browsers
playwright install chromium

# Check system dependencies
playwright install-deps
```

#### Anti-Detection Issues
```python
# Disable anti-detection for debugging
result = await scrape_instagram_urls(
    urls=urls,
    enable_anti_detection=False
)
```

#### Network Timeouts
```python
# Increase timeout in browser manager
browser_manager = BrowserManager(
    headless=True,
    enable_anti_detection=True
)
# Modify timeout settings in anti_detection.py
```

#### Memory Issues
```python
# Process URLs in smaller batches
batch_size = 5
for i in range(0, len(urls), batch_size):
    batch = urls[i:i+batch_size]
    result = await scrape_instagram_urls(batch)
```

### Debug Mode
```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Use non-headless mode for visual debugging
result = await scrape_instagram_urls(
    urls=urls,
    headless=False
)
```

### Error Handling
```python
try:
    result = await scrape_instagram_urls(urls)
    if result['success']:
        print("✅ Scraping successful")
    else:
        print(f"❌ Errors: {len(result['errors'])}")
        for error in result['errors']:
            print(f"  - {error['url']}: {error['error']}")
except Exception as e:
    print(f"❌ Critical error: {e}")
```

## 📈 Performance Optimization

### Batch Processing
```python
# Process large datasets efficiently
async def process_large_dataset(urls, batch_size=10):
    results = []
    for i in range(0, len(urls), batch_size):
        batch = urls[i:i+batch_size]
        result = await scrape_instagram_urls(batch)
        results.extend(result['data'])
    return results
```

### Memory Management
```python
# Clean up resources after processing
scraper = InstagramScraper()
try:
    result = await scraper.scrape(urls)
finally:
    await scraper.extractor.stop()
```

### Caching
```python
# Implement caching for repeated requests
import json
import os

def load_cached_data(username):
    cache_file = f"cache/{username}.json"
    if os.path.exists(cache_file):
        with open(cache_file, 'r') as f:
            return json.load(f)
    return None

def save_cached_data(username, data):
    os.makedirs("cache", exist_ok=True)
    cache_file = f"cache/{username}.json"
    with open(cache_file, 'w') as f:
        json.dump(data, f)
```

## 🤝 Contributing

### Development Setup
```bash
# Clone repository
git clone <repository-url>
cd insta-scraper

# Create development environment
python -m venv dev-env
source dev-env/bin/activate

# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
python -m pytest tests/
```

### Code Style
- Follow PEP 8 guidelines
- Use type hints
- Add docstrings for all functions
- Write unit tests for new features

### Pull Request Process
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Update documentation
6. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## ⚠️ Disclaimer

This tool is for educational and research purposes only. Please respect Instagram's Terms of Service and rate limits. The authors are not responsible for any misuse of this tool.

## 🆘 Support

### Issues
- Check the [troubleshooting section](#troubleshooting)
- Search existing [issues](../../issues)
- Create a new issue with detailed information

### Documentation
- Review the [API reference](#api-reference)
- Check [example usage](#usage-examples)
- Read the [configuration guide](#configuration)

### Community
- Join discussions in [GitHub Discussions](../../discussions)
- Share your use cases and improvements
- Report bugs and request features

---

**Note**: This scraper is designed to be respectful of Instagram's infrastructure. Please use responsibly and consider implementing appropriate delays between requests to avoid overwhelming their servers. 