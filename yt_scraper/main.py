"""
YouTube Scraper Main Interface
"""

import asyncio
import argparse
import sys
import os
from typing import Dict, List,Any, Optional
import time

# Add parent directory to path to import database module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from yt_scraper.yt_data_extractor import AdvancedYouTubeExtractor
# Orchestrator will handle MongoDB persistence; scraper avoids direct DB usage

class YouTubeScraperInterface:
    """Simple interface for YouTube data extraction"""
    
    def __init__(self, headless: bool = True, enable_anti_detection: bool = True, use_mongodb: bool = True):
        """Initialize the scraper interface"""
        self.headless = headless
        self.enable_anti_detection = enable_anti_detection
        self.use_mongodb = use_mongodb
        self.extractor = None
        
        # No direct DB initialization
    
    async def scrape_single_url(self, url: str, output_file: str = "youtube_data.json") -> bool:
        """
        Scrape a single YouTube URL and save to file
        
        Args:
            url: YouTube URL to scrape
            output_file: Output file name
            
        Returns:
            bool: Success status
        """
        print(f"🎯 Scraping single URL: {url}")
        
        try:
            self.extractor = AdvancedYouTubeExtractor(
                headless=self.headless, 
                enable_anti_detection=self.enable_anti_detection
            )
            
            await self.extractor.start()
            
            # Extract data
            data = await self.extractor.extract_youtube_data(url)
            
            if data.get('error'):
                print(f"❌ Failed to extract data: {data['error']}")
                return False
            
            # Prepare unified lead for orchestrator-level persistence
            unified_lead = self._transform_youtube_to_unified(data)
            if unified_lead:
                data['unified_lead'] = unified_lead
            
            # Save clean output to file as backup
            await self.extractor.save_clean_final_output([data], output_file)
            
            print(f"✅ Successfully scraped and saved to {output_file}")
            return True
            
        except Exception as e:
            print(f"❌ Error scraping URL: {e}")
            return False
        finally:
            if self.extractor:
                await self.extractor.stop()
    
    async def scrape_multiple_urls(self, urls: List[str], output_file: str = "youtube_batch_data.json", icp_identifier: str = "default") -> dict:
        """
        Scrape multiple YouTube URLs and save to file
        
        Args:
            urls: List of YouTube URLs to scrape
            output_file: Output file name
            icp_identifier: ICP identifier for unified leads
            
        Returns:
            dict: Structured results with data, unified_leads, and metadata
        """
        print(f"🎯 Scraping {len(urls)} URLs...")
        results = {
            'data': [],
            'unified_leads': [],
            'summary': {
                'total_urls': len(urls),
                'successful_scrapes': 0,
                'failed_scrapes': 0,
                'total_time_seconds': 0
            },
            'error': None
        }
        start_time = time.time()
        try:
            self.extractor = AdvancedYouTubeExtractor(
                headless=self.headless, 
                enable_anti_detection=self.enable_anti_detection
            )
            
            await self.extractor.start()
            
            # Extract data first
            all_data = []
            for url in urls:
                try:
                    data = await self.extractor.extract_youtube_data(url)
                    if not data.get('error'):
                        all_data.append(data)
                        results['summary']['successful_scrapes'] += 1
                    else:
                        results['summary']['failed_scrapes'] += 1
                        print(f"⚠️ Skipped {url} due to error: {data.get('error')}")
                except Exception as e:
                    results['summary']['failed_scrapes'] += 1
                    print(f"❌ Error extracting data from {url}: {e}")
            
            # Save to file as backup
            if all_data:
                final_output = await self.extractor.save_clean_final_output(all_data, output_file)
                results['data'] = final_output
            else:
                results['data'] = []

            # Prepare unified leads for orchestrator-level persistence
            if final_output:
                unified_batch = []
                for item in final_output:
                    try:
                        u = self._transform_youtube_to_unified(item, icp_identifier)
                        if u:
                            unified_batch.append(u)
                    except Exception as e:
                        print(f"❌ Error transforming YouTube data to unified: {e}")
                # Attach as metadata in return path by updating output file content already contains data
            
            results['unified_leads'] = unified_batch
            results['summary']['total_time_seconds'] = time.time() - start_time

            print(f"✅ Successfully scraped {results['summary']['successful_scrapes']} URLs")
            print(f"   - Generated {len(unified_batch)} unified leads")
            
            return results
            
        except Exception as e:
            error_msg = f"Error scraping URLs: {e}"
            print(f"❌ {error_msg}")
            results['error'] = error_msg
            results['summary']['total_time_seconds'] = time.time() - start_time
            return results
        
        finally:
            if self.extractor:
                await self.extractor.stop()

    def _transform_youtube_to_unified(self, youtube_data: Dict[str, Any], icp_identifier: str) -> Optional[Dict[str, Any]]:
        """Transform YouTube data to unified schema (local to scraper). Only profile-type saved."""
        try:
            content_type = (youtube_data.get('content_type') or '').lower()
            if content_type != 'profile':
                return None
            social_media_data = youtube_data.get('social_media_handles', {}) or {}
            def get_first_handle(handles_list):
                if handles_list and isinstance(handles_list, list) and len(handles_list) > 0:
                    return handles_list[0].get('username', '') if isinstance(handles_list[0], dict) else handles_list[0]
                return ""
            def get_bio_links():
                links = []
                for platform, handles in social_media_data.items():
                    if handles and isinstance(handles, list):
                        for handle in handles:
                            if isinstance(handle, dict) and 'url' in handle:
                                links.append(handle['url'])
                return links
            unified = {
                "url": youtube_data.get('url', ""),
                "platform": "youtube",
                "content_type": "profile",
                'icp_identifier': icp_identifier,
                "source": "youtube-scraper",
                "profile": {
                    "username": "",
                    "full_name": youtube_data.get('channel_name', ""),
                    "bio": youtube_data.get('description', ""),
                    "location": "",
                    "job_title": "",
                    "employee_count": ""
                },
                "contact": {
                    "emails": [youtube_data.get('email')] if youtube_data.get('email') else [],
                    "phone_numbers": [],
                    "address": "",
                    "websites": [],
                    "social_media_handles": {
                        "instagram": get_first_handle(social_media_data.get('instagram')),
                        "twitter": get_first_handle(social_media_data.get('twitter')),
                        "facebook": get_first_handle(social_media_data.get('facebook')),
                        "linkedin": get_first_handle(social_media_data.get('linkedin')),
                        "youtube": youtube_data.get('channel_name') or youtube_data.get('username'),
                        "tiktok": get_first_handle(social_media_data.get('tiktok')),
                        "other": []
                    },
                    "bio_links": get_bio_links()
                },
                "content": {
                    "caption": youtube_data.get('title', ''),
                    "upload_date": youtube_data.get('upload_date', ''),
                    "channel_name": youtube_data.get('channel_name', ""),
                    "author_name": ""
                },
                "metadata": {
                    "scraped_at": datetime.utcnow().isoformat(),
                    "data_quality_score": "0.45"
                },
                "industry": None,
                "revenue": None,
                "lead_category": None,
                "lead_sub_category": None,
                "company_name": youtube_data.get('channel_name', ""),
                "company_type": None,
                "decision_makers": None,
                "bdr": "AKG",
                "product_interests": None,
                "timeline": None,
                "interest_level": None
            }
            return unified
        except Exception:
            return None
    
    async def scrape_from_file(self, file_path: str, output_file: str = "youtube_file_data.json") -> bool:
        """
        Scrape URLs from a text file
        
        Args:
            file_path: Path to file containing URLs (one per line)
            output_file: Output file name
            
        Returns:
            bool: Success status
        """
        try:
            if not os.path.exists(file_path):
                print(f"❌ File not found: {file_path}")
                return False
            
            with open(file_path, 'r', encoding='utf-8') as f:
                urls = [line.strip() for line in f.readlines() if line.strip() and line.strip().startswith('http')]
            
            if not urls:
                print(f"❌ No valid YouTube URLs found in {file_path}")
                return False
            
            print(f"📄 Found {len(urls)} URLs in {file_path}")
            return await self.scrape_multiple_urls(urls, output_file)
            
        except Exception as e:
            print(f"❌ Error reading file: {e}")
            return False
    
    async def interactive_mode(self):
        """Interactive mode for easy URL input"""
        print("🔥 YouTube Scraper - Interactive Mode")
        print("=" * 50)
        
        while True:
            print("\nOptions:")
            print("1. Scrape single URL")
            print("2. Scrape multiple URLs (comma-separated)")
            print("3. Scrape from file")
            print("4. Exit")
            
            choice = input("\nEnter your choice (1-4): ").strip()
            
            if choice == '1':
                url = input("Enter YouTube URL: ").strip()
                if url:
                    output = input("Output file name (press Enter for default): ").strip() or "youtube_data.json"
                    await self.scrape_single_url(url, output)
                
            elif choice == '2':
                urls_input = input("Enter URLs (comma-separated): ").strip()
                if urls_input:
                    urls = [url.strip() for url in urls_input.split(',') if url.strip()]
                    output = input("Output file name (press Enter for default): ").strip() or "youtube_batch_data.json"
                    await self.scrape_multiple_urls(urls, output)
                
            elif choice == '3':
                file_path = input("Enter file path: ").strip()
                if file_path:
                    output = input("Output file name (press Enter for default): ").strip() or "youtube_file_data.json"
                    await self.scrape_from_file(file_path, output)
                
            elif choice == '4':
                print("👋 Goodbye!")
                break
                
            else:
                print("❌ Invalid choice. Please enter 1-4.")


# Convenience functions for 1-2 line usage
async def quick_scrape(url: str, output: str = "yt_scraper/youtube_data.json", headless: bool = True) -> bool:
    """
    Quick single URL scraping in 1 line
    
    Usage:
        await quick_scrape("https://youtube.com/watch?v=VIDEO_ID")
    """
    scraper = YouTubeScraperInterface(headless=headless)
    return await scraper.scrape_single_url(url, output)

async def quick_batch_scrape(urls: List[str], output: str = "yt_scraper/youtube_batch_data.json", headless: bool = True) -> bool:
    """
    Quick multiple URLs scraping in 1 line
    
    Usage:
        await quick_batch_scrape(["url1", "url2", "url3"])
    """
    scraper = YouTubeScraperInterface(headless=headless)
    return await scraper.scrape_multiple_urls(urls, output)

async def quick_file_scrape(file_path: str, output: str = "yt_scraper/youtube_file_data.json", headless: bool = True) -> bool:
    """
    Quick file-based scraping in 1 line
    
    Usage:
        await quick_file_scrape("urls.txt")
    """
    scraper = YouTubeScraperInterface(headless=headless)
    return await scraper.scrape_from_file(file_path, output)


def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(
        description="YouTube Data Scraper - Extract data from YouTube videos, shorts, and channels",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --url "https://www.youtube.com/watch?v=VIDEO_ID"
  python main.py --urls "url1,url2,url3" --output my_data.json
  python main.py --file urls.txt --headless
  python main.py --interactive
        """
    )
    
    # Mutually exclusive group for input methods
    input_group = parser.add_mutually_exclusive_group(required=False)
    input_group.add_argument('--url', help='Single YouTube URL to scrape')
    input_group.add_argument('--urls', help='Multiple YouTube URLs (comma-separated)')
    input_group.add_argument('--file', help='File containing YouTube URLs (one per line)')
    input_group.add_argument('--interactive', action='store_true', help='Start interactive mode')
    
    # Optional arguments
    parser.add_argument('--output', '-o', default='youtube_scraped_data.json', 
                       help='Output file name (default: youtube_scraped_data.json)')
    parser.add_argument('--headless', action='store_true', default=True,
                       help='Run in headless mode (default: True)')
    parser.add_argument('--show-browser', action='store_true',
                       help='Show browser window (opposite of headless)')
    parser.add_argument('--no-anti-detection', action='store_true',
                       help='Disable anti-detection features')
    
    args = parser.parse_args()
    
    # Handle show-browser flag
    headless_mode = args.headless and not args.show_browser
    anti_detection = not args.no_anti_detection
    
    async def run_scraper():
        scraper = YouTubeScraperInterface(headless=headless_mode, enable_anti_detection=anti_detection)
        
        if args.interactive:
            await scraper.interactive_mode()
        elif args.url:
            success = await scraper.scrape_single_url(args.url, args.output)
            sys.exit(0 if success else 1)
        elif args.urls:
            urls = [url.strip() for url in args.urls.split(',') if url.strip()]
            success = await scraper.scrape_multiple_urls(urls, args.output)
            sys.exit(0 if success else 1)
        elif args.file:
            success = await scraper.scrape_from_file(args.file, args.output)
            sys.exit(0 if success else 1)
        else:
            # No arguments provided, show help and start interactive mode
            parser.print_help()
            print("\n🔥 Starting interactive mode...")
            await scraper.interactive_mode()
    
    # Run the async function
    try:
        asyncio.run(run_scraper())
    except KeyboardInterrupt:
        print("\n👋 Scraping interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()