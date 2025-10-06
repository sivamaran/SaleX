# Universal Scraper - Fully Dynamic

This folder contains the **fully dynamic** universal scraper that works with ANY business directory website.

## 🚀 Key Changes - Now Fully Dynamic!

**REMOVED:**
- ❌ Hardcoded `SCRAPER_CONFIGS` for specific sites
- ❌ Site-specific detection logic (`_detect_directory_type`)
- ❌ Hardcoded scraping methods (`_scrape_thomasnet_directory`, etc.)

**ADDED:**
- ✅ **Universal AI-powered scraping** that works with ANY website
- ✅ **Command-line interface** for any URL + service
- ✅ **Dynamic page type detection** based on content patterns
- ✅ **Fallback extraction** when AI is unavailable

## 📋 Usage

### Basic Usage
```bash
python universal_scraper_complete.py
```
Uses default demo: ThomasNet + "ac services"

### Custom Usage
```bash
python universal_scraper_complete.py [URL] [SERVICE]
```

### Examples
```bash
# ThomasNet
python universal_scraper_complete.py "https://www.thomasnet.com/search.html?q=ac+services" "ac services"

# Kompass
python universal_scraper_complete.py "https://www.kompass.com/en/search/" "software development"

# Crunchbase
python universal_scraper_complete.py "https://crunchbase.com/search/organizations" "fintech"

# YellowPages
python universal_scraper_complete.py "https://www.yellowpages.com/search" "restaurants"

# ANY business directory
python universal_scraper_complete.py "https://any-directory.com/search?q=your-service" "your service"
```

## 🎯 How It Works Now

1. **No Hardcoded Logic**: Doesn't check for specific domain names
2. **Universal Detection**: Uses content patterns to detect page types
3. **AI-Powered**: Google Gemini extracts data from any website
4. **Fallback Ready**: Works even without AI (uses regex patterns)
5. **Dynamic URLs**: Accepts any search URL format

## 📁 Files

- `universal_scraper_complete.py` - Main dynamic scraper
- `ai_integration/` - AI extraction modules
- `data_models/` - Lead data structures
- `utils/` - Contact validation, anti-detection
- `requirements.txt` - Dependencies
- `demo.sh` - Usage examples

## 🔧 Technical Details

- **Page Types Detected**: company_directory, company_profile, search_results, generic
- **Contact Validation**: Filters invalid emails/phones automatically
- **Anti-Detection**: Randomized user agents, stealth browsing
- **Output Format**: Comprehensive lead profiles with business intelligence

The scraper is now **truly universal** - just give it any business directory URL and service, and it will extract leads!