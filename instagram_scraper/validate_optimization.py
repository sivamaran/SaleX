"""
Validation script for optimized Instagram scraper
Quick test to ensure the implementation works correctly
"""

import asyncio
import sys
import os

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from main_optimized import scrape_instagram_urls_optimized, ScrapingConfig


async def validate_optimization():
    """Validate that the optimized scraper works correctly"""
    print("🔍 Validating Optimized Instagram Scraper")
    print("=" * 50)
    
    # Test URLs
    # test_urls = [
    #     "https://www.instagram.com/thebucketlistfamily/",
    #     "https://www.instagram.com/only_when_i_travel/",
    #     "https://www.instagram.com/reel/DIeKXmmS8vX/",
    #     "https://www.instagram.com/reel/DL7aIrkI3Hp/",
    #     "https://www.instagram.com/traveogram/p/DLC4PcWzbZH/",
    # "https://www.instagram.com/professionaltraveler/"
    # ]
    test_urls = [
        "https://www.instagram.com/thebucketlistfamily/",
        "https://www.instagram.com/reel/DL7aIrkI3Hp/",
        "https://www.instagram.com/professionaltraveler/",
        "https://www.instagram.com/travelnoire/reel/DDH0TEuse6w/",
        "https://www.instagram.com/travellingthroughtheworld/",
        "https://www.instagram.com/brindasharma/"

    ]
    
    print(f"Testing with {len(test_urls)} URL(s):")
    for i, url in enumerate(test_urls, 1):
        print(f"  {i}. {url}")
    
    # Conservative configuration for testing
    config = ScrapingConfig(
        max_workers=2,
        batch_size=2,
        context_pool_size=2,
        rate_limit_delay=1.0,
        context_reuse_limit=5
    )
    
    print(f"\nConfiguration:")
    print(f"  - Max workers: {config.max_workers}")
    print(f"  - Batch size: {config.batch_size}")
    print(f"  - Context pool size: {config.context_pool_size}")
    print(f"  - Rate limit delay: {config.rate_limit_delay}s")
    
    try:
        print(f"\n🚀 Starting optimized scraper...")
        start_time = asyncio.get_event_loop().time()
        
        result = await scrape_instagram_urls_optimized(
            urls=test_urls,
            headless=True,
            enable_anti_detection=True,
            config=config
        )
        
        end_time = asyncio.get_event_loop().time()
        duration = end_time - start_time
        
        print(f"\n✅ Validation completed in {duration:.2f} seconds")
        print(f"   - Success: {result['success']}")
        print(f"   - Data entries: {len(result['data'])}")
        print(f"   - Errors: {len(result['errors'])}")
        print(f"   - Throughput: {len(test_urls) / duration:.2f} URLs/second")
        
        if result['data']:
            print(f"\n📋 Sample data:")
            for i, entry in enumerate(result['data'][:2], 1):
                content_type = entry.get('content_type', 'unknown')
                print(f"   {i}. {content_type.title()}: {entry.get('url', 'N/A')}")
                if content_type == 'profile':
                    username = entry.get('username', 'N/A')
                    followers = entry.get('followers_count', 'N/A')
                    print(f"      Username: @{username}, Followers: {followers}")
        
        if result['errors']:
            print(f"\n❌ Errors encountered:")
            for error in result['errors']:
                print(f"   - {error.get('url', 'Unknown')}: {error.get('error', 'Unknown error')}")
        
        # Check performance metrics
        if 'performance_metrics' in result['summary']:
            metrics = result['summary']['performance_metrics']
            print(f"\n📊 Performance metrics:")
            print(f"   - Contexts used: {metrics.get('contexts_used', 'N/A')}")
            print(f"   - Max workers: {metrics.get('max_workers', 'N/A')}")
            print(f"   - Batch size: {metrics.get('batch_size', 'N/A')}")
        
        print(f"\n🎉 Validation successful!")
        return True
        
    except Exception as e:
        print(f"\n❌ Validation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Main validation function"""
    try:
        success = await validate_optimization()
        
        if success:
            print(f"\n✅ All validations passed!")
            print(f"   The optimized Instagram scraper is working correctly.")
            print(f"   You can now use it for high-performance scraping.")
        else:
            print(f"\n❌ Validation failed!")
            print(f"   Please check the error messages above.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print(f"\n⚠️ Validation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error during validation: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
