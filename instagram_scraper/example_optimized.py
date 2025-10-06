"""
Example Usage of Optimized Instagram Scraper
Demonstrates the performance improvements and new features
"""

import asyncio
from main_optimized import OptimizedInstagramScraper, scrape_instagram_urls_optimized, ScrapingConfig


async def example_1_basic_usage():
    """Example 1: Basic optimized usage"""
    print("=" * 60)
    print("EXAMPLE 1: Basic Optimized Usage")
    print("=" * 60)
    
    # List of Instagram URLs to scrape
    urls = [
        "https://www.instagram.com/90svogue.__",
        "https://www.instagram.com/codehype_",
        "https://www.instagram.com/p/DMsercXMVeZ/",
        "https://www.instagram.com/reel/CSb6-Rap2Ip/"
    ]
    
    # One-line optimized scraping call
    result = await scrape_instagram_urls_optimized(urls)
    
    print(f"✅ Optimized scraping completed!")
    print(f"   Success: {result['success']}")
    print(f"   Data entries: {len(result['data'])}")
    print(f"   Success rate: {result['summary']['success_rate']:.1f}%")
    print(f"   Throughput: {result['summary']['performance_metrics']['throughput_per_second']:.2f} URLs/second")
    
    return result


async def example_2_high_performance():
    """Example 2: High-performance configuration"""
    print("\n" + "=" * 60)
    print("EXAMPLE 2: High-Performance Configuration")
    print("=" * 60)
    
    urls = [
        "https://www.instagram.com/90svogue.__",
        "https://www.instagram.com/codehype_",
        "https://www.instagram.com/p/DMsercXMVeZ/",
        "https://www.instagram.com/reel/CSb6-Rap2Ip/",
        "https://www.instagram.com/another_user/",
        "https://www.instagram.com/p/another_post/",
        "https://www.instagram.com/reel/another_reel/",
        "https://www.instagram.com/yet_another_user/"
    ]
    
    # High-performance configuration
    config = ScrapingConfig(
        max_workers=6,           # More concurrent workers
        batch_size=8,            # Larger batches
        context_pool_size=5,     # More browser contexts
        rate_limit_delay=0.3,    # Faster rate limiting
        context_reuse_limit=30   # Reuse contexts more
    )
    
    result = await scrape_instagram_urls_optimized(
        urls=urls,
        headless=True,
        enable_anti_detection=True,
        output_file="instagram_scraper/high_performance_results.json",
        config=config
    )
    
    print(f"✅ High-performance scraping completed!")
    print(f"   Success: {result['success']}")
    print(f"   Data entries: {len(result['data'])}")
    print(f"   Throughput: {result['summary']['performance_metrics']['throughput_per_second']:.2f} URLs/second")
    print(f"   Total time: {result['summary']['total_time_seconds']:.2f} seconds")
    
    return result


async def example_3_conservative_mode():
    """Example 3: Conservative mode for stability"""
    print("\n" + "=" * 60)
    print("EXAMPLE 3: Conservative Mode")
    print("=" * 60)
    
    urls = [
        "https://www.instagram.com/90svogue.__",
        "https://www.instagram.com/codehype_"
    ]
    
    # Conservative configuration for stability
    config = ScrapingConfig(
        max_workers=2,           # Fewer workers
        batch_size=2,            # Smaller batches
        context_pool_size=2,     # Fewer contexts
        rate_limit_delay=2.0,    # Slower rate limiting
        context_reuse_limit=10   # Less context reuse
    )
    
    result = await scrape_instagram_urls_optimized(
        urls=urls,
        headless=True,
        enable_anti_detection=True,
        output_file="instagram_scraper/conservative_results.json",
        config=config
    )
    
    print(f"✅ Conservative scraping completed!")
    print(f"   Success: {result['success']}")
    print(f"   Data entries: {len(result['data'])}")
    print(f"   Throughput: {result['summary']['performance_metrics']['throughput_per_second']:.2f} URLs/second")
    
    return result


async def example_4_large_batch():
    """Example 4: Large batch processing"""
    print("\n" + "=" * 60)
    print("EXAMPLE 4: Large Batch Processing")
    print("=" * 60)
    
    # Simulate a large batch by repeating URLs
    base_urls = [
        "https://www.instagram.com/90svogue.__",
        "https://www.instagram.com/codehype_",
        "https://www.instagram.com/p/DMsercXMVeZ/",
        "https://www.instagram.com/reel/CSb6-Rap2Ip/"
    ]
    
    # Create a larger batch
    urls = (base_urls * 3)[:12]  # 12 URLs total
    
    # Configuration optimized for large batches
    config = ScrapingConfig(
        max_workers=5,
        batch_size=6,
        context_pool_size=5,
        rate_limit_delay=0.5,
        context_reuse_limit=25
    )
    
    print(f"Processing {len(urls)} URLs in batches of {config.batch_size}...")
    
    result = await scrape_instagram_urls_optimized(
        urls=urls,
        headless=True,
        enable_anti_detection=True,
        output_file="instagram_scraper/large_batch_results.json",
        config=config
    )
    
    print(f"✅ Large batch processing completed!")
    print(f"   Success: {result['success']}")
    print(f"   Data entries: {len(result['data'])}")
    print(f"   Throughput: {result['summary']['performance_metrics']['throughput_per_second']:.2f} URLs/second")
    print(f"   Total time: {result['summary']['total_time_seconds']:.2f} seconds")
    print(f"   Average time per URL: {result['summary']['average_time_per_url']:.2f} seconds")
    
    return result


async def example_5_class_usage():
    """Example 5: Using the OptimizedInstagramScraper class directly"""
    print("\n" + "=" * 60)
    print("EXAMPLE 5: Class Usage with Custom Configuration")
    print("=" * 60)
    
    urls = [
        "https://www.instagram.com/90svogue.__",
        "https://www.instagram.com/codehype_"
    ]
    
    # Create custom configuration
    config = ScrapingConfig(
        max_workers=3,
        batch_size=2,
        context_pool_size=3,
        rate_limit_delay=1.0,
        context_reuse_limit=15
    )
    
    # Create scraper instance
    scraper = OptimizedInstagramScraper(
        headless=True,
        enable_anti_detection=True,
        is_mobile=False,
        output_file="instagram_scraper/class_usage_results.json",
        config=config
    )
    
    # Use the scraper
    result = await scraper.scrape(urls)
    
    print(f"✅ Class-based scraping completed!")
    print(f"   Success: {result['success']}")
    print(f"   Data entries: {len(result['data'])}")
    print(f"   Throughput: {result['summary']['performance_metrics']['throughput_per_second']:.2f} URLs/second")
    print(f"   Contexts used: {result['summary']['performance_metrics']['contexts_used']}")
    
    return result


async def example_6_error_handling():
    """Example 6: Error handling and graceful degradation"""
    print("\n" + "=" * 60)
    print("EXAMPLE 6: Error Handling and Graceful Degradation")
    print("=" * 60)
    
    # Mix of valid and invalid URLs
    urls = [
        "https://www.instagram.com/90svogue.__",  # Valid
        "https://www.instagram.com/invalid_user_12345/",  # Invalid
        "https://www.instagram.com/p/DMsercXMVeZ/",  # Valid
        "https://www.instagram.com/p/invalid_post_id/",  # Invalid
        "https://www.instagram.com/reel/CSb6-Rap2Ip/",  # Valid
        "https://about.instagram.com/about-us/careers",  # Non-Instagram
    ]
    
    config = ScrapingConfig(
        max_workers=3,
        batch_size=3,
        context_pool_size=3,
        rate_limit_delay=0.5
    )
    
    result = await scrape_instagram_urls_optimized(
        urls=urls,
        headless=True,
        enable_anti_detection=True,
        output_file="instagram_scraper/error_handling_results.json",
        config=config
    )
    
    print(f"✅ Error handling test completed!")
    print(f"   Success: {result['success']}")
    print(f"   Data entries: {len(result['data'])}")
    print(f"   Errors: {len(result['errors'])}")
    print(f"   Success rate: {result['summary']['success_rate']:.1f}%")
    
    if result['errors']:
        print(f"\n❌ Errors encountered:")
        for error in result['errors']:
            print(f"   - {error['url']}: {error['error']}")
    
    return result


async def main():
    """Run all examples"""
    print("Instagram Scraper - Optimized Implementation Examples")
    print("=" * 80)
    
    try:
        # Run examples
        await example_1_basic_usage()
        await example_2_high_performance()
        await example_3_conservative_mode()
        await example_4_large_batch()
        await example_5_class_usage()
        await example_6_error_handling()
        
        print("\n" + "=" * 80)
        print("ALL EXAMPLES COMPLETED!")
        print("=" * 80)
        print("\n📝 Usage Summary:")
        print("   1. Basic: result = await scrape_instagram_urls_optimized(urls)")
        print("   2. High-performance: Use ScrapingConfig with more workers/contexts")
        print("   3. Conservative: Use ScrapingConfig with fewer workers for stability")
        print("   4. Large batches: Process many URLs efficiently")
        print("   5. Class usage: Direct control over scraper instance")
        print("   6. Error handling: Graceful degradation with mixed valid/invalid URLs")
        print("\n🚀 Key Optimizations:")
        print("   - Concurrent processing with worker pools")
        print("   - Browser context pooling and reuse")
        print("   - Batch processing for better throughput")
        print("   - Coordinated rate limiting")
        print("   - Resource management and cleanup")
        print("   - Fail-fast strategy with graceful degradation")
        
    except Exception as e:
        print(f"\n❌ Examples failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
