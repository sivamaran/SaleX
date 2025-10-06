# Facebook Scraper

This directory contains the Facebook scraper, designed to extract publicly available data from Facebook profiles, pages, and posts.

## Features:
- Browser automation using Puppeteer (via `ScraperDynamic`).
- JSON-LD and meta-tag extraction for structured data.
- Basic anti-detection measures.

## Usage:

### From `facebook_data_extractor.py`:

```python
import asyncio
from facebook_scraper.facebook_data_extractor import FacebookDataExtractor

async def main():
    extractor = FacebookDataExtractor(headless=False) # Set headless=True for background operation
    try:
        await extractor.start()
        
        # Example: Scrape a Facebook Page
        page_url = "https://www.facebook.com/facebook"
        page_data = await extractor.extract_facebook_data(page_url)
        print(f"Scraped Page Data: {page_data.get('extracted_data', {})}")
        await extractor.save_facebook_data_to_json(page_data, "facebook_page_data.json")

        # Example: Scrape a Facebook Profile (replace with a public profile URL)
        profile_url = "https://www.facebook.com/profile.php?id=100044577884813" # Example ID, replace with a real one
        profile_data = await extractor.extract_facebook_data(profile_url)
        print(f"Scraped Profile Data: {profile_data.get('extracted_data', {})}")
        await extractor.save_facebook_data_to_json(profile_data, "facebook_profile_data.json")

        # Example: Scrape a Facebook Post (replace with a public post URL)
        post_url = "https://www.facebook.com/facebook/posts/pfbid0213123123123123123123123123123123123123123123123123123123" # Example URL, replace with a real one
        post_data = await extractor.extract_facebook_data(post_url)
        print(f"Scraped Post Data: {post_data.get('extracted_data', {})}")
        await extractor.save_facebook_data_to_json(post_data, "facebook_post_data.json")

    finally:
        await extractor.stop()

if __name__ == "__main__":
    asyncio.run(main())
```

## Setup:

1.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
2.  **Ensure Chrome/Chromium is installed** for Puppeteer to function.

## Directory Structure:
- `facebook_data_extractor.py`: Contains the core logic for extracting Facebook data.
- `requirements.txt`: Python dependencies.
- `README.md`: This file.