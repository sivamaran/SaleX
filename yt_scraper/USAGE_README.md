# YouTube Data Scraper - Main Interface

A powerful YouTube scraper with **1-2 line usage** that can extract data from videos, shorts, and channels with advanced anti-detection features.

## 🚀 Quick Start - Command Line Usage

### Single URL (1 line)
```bash
python main.py --url "https://www.youtube.com/watch?v=VIDEO_ID"
```

### Multiple URLs (1 line)
```bash
python main.py --urls "url1,url2,url3" --output my_data.json
```

### From File (1 line)
```bash
python main.py --file urls.txt
```

### Interactive Mode
```bash
python main.py --interactive
```

## 🐍 Programmatic Usage (1-2 lines)

### Single URL
```python
import asyncio
from main import quick_scrape

# 1 line to scrape any YouTube URL
asyncio.run(quick_scrape("https://www.youtube.com/watch?v=VIDEO_ID"))
```

### Multiple URLs
```python
import asyncio
from main import quick_batch_scrape

urls = ["url1", "url2", "url3"]
# 1 line to scrape multiple URLs
asyncio.run(quick_batch_scrape(urls, "results.json"))
```

### From File
```python
import asyncio
from main import quick_file_scrape

# 1 line to scrape from file
asyncio.run(quick_file_scrape("urls.txt", "results.json"))
```

## 📋 Command Line Options

| Option | Description | Example |
|--------|-------------|---------|
| `--url` | Single YouTube URL | `--url "https://youtube.com/watch?v=..."` |
| `--urls` | Multiple URLs (comma-separated) | `--urls "url1,url2,url3"` |
| `--file` | File with URLs (one per line) | `--file urls.txt` |
| `--output, -o` | Output file name | `--output my_data.json` |
| `--headless` | Run without browser window (default) | `--headless` |
| `--show-browser` | Show browser window | `--show-browser` |
| `--interactive` | Start interactive mode | `--interactive` |
| `--no-anti-detection` | Disable anti-detection features | `--no-anti-detection` |

## 📁 File Formats

### Input File (urls.txt)
```
https://www.youtube.com/watch?v=VIDEO_ID1
https://www.youtube.com/shorts/SHORT_ID
https://www.youtube.com/@CHANNEL_NAME
```

### Output JSON Structure
```json
[
  {
    "url": "https://www.youtube.com/watch?v=VIDEO_ID",
    "content_type": "video",
    "title": "Video Title",
    "channel_name": "Channel Name",
    "views": "1.2M",
    "upload_date": "2023-01-01",
    "subscribers": "500K",
    "social_media_handles": {
      "instagram": [{"username": "handle", "url": "https://instagram.com/handle"}],
      "twitter": [{"username": "handle", "url": "https://twitter.com/handle"}]
    },
    "email": ["contact@example.com"]
  }
]
```

## 🎯 Supported Content Types

- **Videos**: Regular YouTube videos (`/watch?v=`)
- **Shorts**: YouTube Shorts (`/shorts/`)
- **Channels**: Channel pages (`/@channel` or `/channel/`)

## 🛡️ Anti-Detection Features

- Browser fingerprint evasion
- Human-like behavioral mimicking
- Network request obfuscation
- Automatic popup handling
- Stealth mode operation

## 📊 What Gets Extracted

### For Videos:
- Title, Channel Name, Upload Date
- View Count, Subscriber Count
- Description (with social media handles extracted)
- Email addresses from description
- Channel URL

### For Shorts:
- Title, Channel Name, Upload Date
- View Count, Channel URL

### For Channels:
- Channel Name, Subscriber Count
- Channel Description
- Social media handles from description
- Email addresses from description

## 🔧 Advanced Usage

### Custom Settings
```python
from main import YouTubeScraperInterface

scraper = YouTubeScraperInterface(headless=False, enable_anti_detection=True)
await scraper.scrape_single_url("url", "output.json")
```

### Batch Processing
```python
urls = ["url1", "url2", "url3"]
await scraper.scrape_multiple_urls(urls, "batch_output.json")
```

## 📝 Examples

Check `usage_examples.py` for complete working examples:

```bash
python usage_examples.py
```

## 🚨 Usage Notes

1. **Respect YouTube's Terms**: Use responsibly and respect rate limits
2. **Network Speed**: Scraping speed depends on your internet connection
3. **Anti-Bot Measures**: The scraper includes anti-detection, but YouTube may still block excessive requests
4. **Browser Dependencies**: Requires Playwright browser installation

## 🔧 Installation Requirements

Make sure you have the required dependencies:
- `playwright`
- `beautifulsoup4`
- `zstandard`
- And the browser_manager.py module

## 🎉 Quick Test

Test the scraper with a simple command:

```bash
python main.py --url "https://www.youtube.com/watch?v=dQw4w9WgXcQ" --show-browser
```

This will scrape the famous Rick Roll video and show you the browser in action!