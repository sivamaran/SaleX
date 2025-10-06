# YouTube Data Scraper

A powerful, automated YouTube data extraction tool that uses browser automation with advanced anti-detection features to scrape data from YouTube videos, shorts, and channels.

## 🚀 Features

- **Multi-Content Support**: Extract data from videos, shorts, and channels
- **Advanced Anti-Detection**: Browser fingerprinting evasion and behavioral mimicking
- **Network Request Capture**: Captures and analyzes YouTube API responses
- **Social Media Extraction**: Automatically extracts social media handles and email addresses from descriptions
- **Multiple Input Methods**: Single URL, multiple URLs, file input, or interactive mode
- **Clean Output Format**: Structured JSON output with clean, organized data
- **Human-like Behavior**: Implements mouse movements, scrolling, and realistic delays
- **Headless & GUI Modes**: Run with or without browser window

## 📋 Requirements

```bash
pip install playwright beautifulsoup4 zstandard asyncio
```

## 🛠️ Installation

1. **Clone or download the project files**
2. **Install dependencies**:
   ```bash
   pip install playwright beautifulsoup4 zstandard
   ```
3. **Install Playwright browsers**:
   ```bash
   playwright install
   ```

## 📁 Project Structure

```
youtube-scraper/
├── main.py                 # Main interface and CLI
├── yt_data_extractor.py    # Core extraction engine
├── browser_manager.py      # Browser automation (required)
└── README.md              # This file
```

## 🎯 Quick Start

### 1. Single URL Extraction
```bash
python main.py --url "https://www.youtube.com/watch?v=VIDEO_ID"
```

### 2. Multiple URLs
```bash
python main.py --urls "url1,url2,url3" --output my_data.json
```

### 3. From File
```bash
python main.py --file urls.txt
```

### 4. Interactive Mode
```bash
python main.py --interactive
```

## 📖 Usage Examples

### Command Line Interface

#### Basic Video Extraction
```bash
# Extract single video data
python main.py --url "https://www.youtube.com/watch?v=dQw4w9WgXcQ"

# Extract with custom output file
python main.py --url "https://www.youtube.com/watch?v=dQw4w9WgXcQ" --output video_data.json
```

#### Shorts Extraction
```bash
# Extract YouTube Shorts data
python main.py --url "https://www.youtube.com/shorts/SHORTS_ID"
```

#### Channel Extraction
```bash
# Extract channel information
python main.py --url "https://www.youtube.com/@channelname"
```

#### Batch Processing
```bash
# Multiple URLs at once
python main.py --urls "https://youtube.com/watch?v=1,https://youtube.com/watch?v=2"

# From text file (one URL per line)
python main.py --file youtube_urls.txt --output batch_results.json
```

#### Browser Options
```bash
# Show browser window (non-headless)
python main.py --show-browser --url "https://youtube.com/watch?v=VIDEO_ID"

# Disable anti-detection features
python main.py --no-anti-detection --url "https://youtube.com/watch?v=VIDEO_ID"
```

### Python API Usage

#### Quick Functions
```python
import asyncio
from main import quick_scrape, quick_batch_scrape, quick_file_scrape

# Single URL
await quick_scrape("https://youtube.com/watch?v=VIDEO_ID")

# Multiple URLs
urls = ["url1", "url2", "url3"]
await quick_batch_scrape(urls, "batch_output.json")

# From file
await quick_file_scrape("urls.txt", "file_output.json")
```

#### Advanced Usage
```python
import asyncio
from yt_data_extractor import AdvancedYouTubeExtractor

async def extract_youtube_data():
    extractor = AdvancedYouTubeExtractor(headless=True, enable_anti_detection=True)
    
    try:
        await extractor.start()
        
        # Extract single URL
        data = await extractor.extract_youtube_data("https://youtube.com/watch?v=VIDEO_ID")
        
        # Extract and save multiple URLs
        urls = ["url1", "url2", "url3"]
        await extractor.extract_and_save_clean_data_from_urls(urls, "output.json")
        
    finally:
        await extractor.stop()

# Run the extraction
asyncio.run(extract_youtube_data())
```

## 📊 Output Format

### Clean Output Structure

#### Video Data
```json
{
  "url": "https://www.youtube.com/watch?v=VIDEO_ID",
  "content_type": "video",
  "title": "Video Title",
  "channel_name": "Channel Name",
  "upload_date": "2023-01-01",
  "views": "1.2M",
  "subscribers": "500K",
  "description": "Video description...",
  "social_media_handles": {
    "instagram": [
      {
        "username": "channelname",
        "url": "https://www.instagram.com/channelname/"
      }
    ],
    "twitter": [
      {
        "username": "channelname", 
        "url": "https://twitter.com/channelname"
      }
    ],
    "email": [
      {
        "username": "contact@example.com",
        "url": "mailto:contact@example.com"
      }
    ]
  },
  "email": ["contact@example.com"]
}
```

#### Channel Data
```json
{
  "url": "https://www.youtube.com/@channelname",
  "content_type": "channel",
  "channel_name": "Channel Name",
  "subscribers": "1.2M",
  "description": "Channel description...",
  "social_media_handles": {
    "instagram": [...],
    "twitter": [...],
    "tiktok": [...],
    "facebook": [...],
    "email": [...]
  },
  "email": ["contact@example.com"]
}
```

#### Shorts Data
```json
{
  "url": "https://www.youtube.com/shorts/SHORTS_ID",
  "content_type": "shorts",
  "title": "Shorts Title",
  "channel_name": "Channel Name",
  "upload_date": "2023-01-01",
  "views": "500K"
}
```

## 🎛️ Configuration Options

### CLI Arguments
```bash
--url              # Single YouTube URL to scrape
--urls             # Multiple URLs (comma-separated)
--file             # File containing URLs (one per line)
--interactive      # Start interactive mode
--output, -o       # Output file name (default: youtube_scraped_data.json)
--headless         # Run in headless mode (default: True)
--show-browser     # Show browser window
--no-anti-detection # Disable anti-detection features
```

### Extractor Options
```python
AdvancedYouTubeExtractor(
    headless=True,              # Run browser in headless mode
    enable_anti_detection=True, # Enable stealth features
    is_mobile=False            # Use mobile browser simulation
)
```

## 🔧 Advanced Features

### Social Media Handle Extraction
The scraper automatically extracts social media handles and email addresses from video/channel descriptions:

- **Instagram**: @username, instagram.com/username
- **Twitter/X**: @username, twitter.com/username, x.com/username  
- **TikTok**: @username, tiktok.com/@username
- **Facebook**: facebook.com/username
- **LinkedIn**: linkedin.com/in/username
- **Email**: any valid email address format

### Anti-Detection Features
- Browser fingerprint randomization
- Human-like mouse movements and scrolling
- Realistic typing delays
- User-Agent rotation
- Viewport randomization
- Network request obfuscation

### Network Monitoring
The scraper captures and analyzes:
- YouTube API requests and responses
- Video player data
- Channel metadata
- Comments and engagement data

## 📝 Example Workflows

### Workflow 1: Single Video Analysis
```bash
# Extract detailed video information
python main.py --url "https://www.youtube.com/watch?v=dQw4w9WgXcQ" --output rick_roll.json

# Output will contain:
# - Video title, views, upload date
# - Channel name and subscriber count  
# - Full description with social media links
# - Extracted email addresses
```

### Workflow 2: Channel Research
```bash
# Research a YouTube channel
python main.py --url "https://www.youtube.com/@MrBeast" --output mrbeast_channel.json

# Output will contain:
# - Channel name and subscriber count
# - Channel description and social links
# - Contact information (emails, social media)
```

### Workflow 3: Batch Video Processing
```bash
# Create urls.txt with one URL per line:
# https://www.youtube.com/watch?v=video1
# https://www.youtube.com/watch?v=video2
# https://www.youtube.com/shorts/short1

python main.py --file urls.txt --output batch_analysis.json
```

### Workflow 4: Interactive Research Session
```bash
# Start interactive mode for exploratory analysis
python main.py --interactive

# Follow prompts to:
# 1. Enter URLs one by one
# 2. Process multiple URLs
# 3. Load URLs from file
# 4. Get real-time results
```

## 🐛 Troubleshooting

### Common Issues

**1. Browser Automation Errors**
```bash
# Install/reinstall Playwright browsers
playwright install chromium
```

**2. Anti-Detection Bypass**
```bash
# Try with anti-detection disabled
python main.py --no-anti-detection --url "YOUR_URL"
```

**3. Rate Limiting**
```bash
# Use longer delays between requests
# The scraper includes automatic delays, but you can add manual delays
```

**4. JavaScript Errors**
```bash
# Try showing browser to debug
python main.py --show-browser --url "YOUR_URL"
```

### Debug Mode
```python
# Enable verbose logging in your Python script
import logging
logging.basicConfig(level=logging.DEBUG)
```

## ⚠️ Important Notes

### Ethical Usage
- **Respect YouTube's Terms of Service**
- **Use reasonable request delays**
- **Don't overload YouTube's servers**
- **Respect robots.txt guidelines**

### Rate Limiting
- The scraper includes automatic delays between requests
- For large batch jobs, consider additional delays
- Monitor for rate limiting responses

### Legal Considerations
- Only scrape publicly available data
- Respect copyright and intellectual property rights
- Consider YouTube's API for commercial usage

## 📈 Performance Tips

1. **Use headless mode** for better performance:
   ```bash
   python main.py --headless --url "YOUR_URL"
   ```

2. **Batch process URLs** instead of individual requests:
   ```bash
   python main.py --file urls.txt
   ```

3. **Enable anti-detection selectively**:
   ```bash
   # Disable for faster scraping (higher detection risk)
   python main.py --no-anti-detection --url "YOUR_URL"
   ```

## 🔄 Example Output Files

After running the scraper, you'll get these files:

- `youtube_scraped_data.json` - Complete raw extracted data
- `youtube_final_output.json` - Clean, structured data
- `youtube_batch_data.json` - Batch processing results

## 📞 Support

For issues or questions:

1. Check the troubleshooting section
2. Ensure all dependencies are installed correctly  
3. Verify YouTube URLs are accessible
4. Test with `--show-browser` flag for debugging

## 🔮 Future Enhancements

- [ ] Playlist support
- [ ] Comments extraction
- [ ] Video thumbnail downloads
- [ ] Database integration
- [ ] API endpoint wrapper
- [ ] Docker containerization

---

**Happy Scraping! 🚀**

*Remember to use this tool responsibly and in accordance with YouTube's Terms of Service.*