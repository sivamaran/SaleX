"""
YouTube Scraper - Simple Usage Examples
These examples show how to use the YouTube scraper with just 1-2 lines of code
"""

import asyncio
from yt_scraper.main import quick_scrape, quick_batch_scrape, quick_file_scrape

async def example_1_single_url():
    """Example 1: Scrape a single YouTube URL"""
    print("Example 1: Single URL scraping")
    
    # 1 line to scrape any YouTube URL
    success = await quick_scrape("https://www.youtube.com/watch?v=dQw4w9WgXcQ")
    
    if success:
        print("✅ Data saved to youtube_data.json")
    else:
        print("❌ Scraping failed")

async def example_2_multiple_urls():
    """Example 2: Scrape multiple YouTube URLs"""
    print("Example 2: Multiple URLs scraping")
    
    # urls = [
    #     "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
    #     "https://www.youtube.com/shorts/YIe4jPsvv5g",
    #     "https://www.youtube.com/@stillwatchingnetflix"
    # ]

    # urls = [
    #     "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
    #     "https://www.youtube.com/shorts/YIe4jPsvv5g",
    #     "https://www.youtube.com/@stillwatchingnetflix",
    #     "https://www.youtube.com/watch?v=IDjvwIoIqQY&pp=ygUII25vcnRoM24%3D"
    # ]
    urls = [
        "https://www.youtube.com/watch?v=-ExNTLn3DK4&pp=ygUGI3RoZ29h",
"https://www.youtube.com/watch?v=IDjvwIoIqQY&pp=ygUII25vcnRoM24%3D",
"https://www.youtube.com/watch?v=qaa9IJIW4go",
"https://www.youtube.com/watch?v=5nnoVRUZA-c"

    ]
    # 1 line to scrape multiple URLs
    success = await quick_batch_scrape(urls, "yt_scraper/batch_results.json")
    
    if success:
        print("✅ Batch data saved to batch_results.json")
    else:
        print("❌ Batch scraping failed")

async def example_3_from_file():
    """Example 3: Scrape URLs from a file"""
    print("Example 3: File-based scraping")
    
    # First create a sample URLs file
#     sample_urls = [
#         "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
#         "https://www.youtube.com/shorts/YIe4jPsvv5g",
#         "https://www.youtube.com/@stillwatchingnetflix",
#         "https://www.youtube.com/watch?v=IDjvwIoIqQY&pp=ygUII25vcnRoM24%3D",
#         "https://www.youtube.com/watch?v=G_Y9zqjLL9o",
#         "https://www.youtube.com/watch?v=gGogLqLtZPw",
#         "https://www.youtube.com/watch?v=OId7NfbCfm8",
#         "https://www.youtube.com/watch?v=-ExNTLn3DK4&pp=ygUGI3RoZ29h",
# "https://www.youtube.com/watch?v=qaa9IJIW4go"
#     ]
    sample_urls = [
        "https://www.youtube.com/watch?v=-ExNTLn3DK4&pp=ygUGI3RoZ29h",
"https://www.youtube.com/watch?v=IDjvwIoIqQY&pp=ygUII25vcnRoM24%3D",
"https://www.youtube.com/watch?v=qaa9IJIW4go",
"https://www.youtube.com/watch?v=5nnoVRUZA-c"
    ]
    
    with open("sample_urls.txt", "w") as f:
        for url in sample_urls:
            f.write(url + "\n")
    
    # 1 line to scrape from file
    success = await quick_file_scrape("sample_urls.txt", "file_results.json")
    
    if success:
        print("✅ File data saved to file_results.json")
    else:
        print("❌ File scraping failed")

async def example_4_custom_output():
    """Example 4: Custom output file and settings"""
    print("Example 4: Custom settings")
    
    # 1 line with custom output file and visible browser
    success = await quick_scrape(
        "https://www.youtube.com/@stillwatchingnetflix", 
        output="my_channel_data.json", 
        headless=False  # Show browser window
    )
    
    if success:
        print("✅ Custom data saved to my_channel_data.json")
    else:
        print("❌ Custom scraping failed")

async def run_all_examples():
    """Run all examples"""
    print("🔥 YouTube Scraper Usage Examples")
    print("=" * 50)
    
    # await example_1_single_url()
    # print("\n" + "-" * 30 + "\n")
    
    await example_2_multiple_urls()
    print("\n" + "-" * 30 + "\n")
    
    # await example_3_from_file()
    # print("\n" + "-" * 30 + "\n")
    
    # await example_4_custom_output()
    
    print("\n✅ All examples completed!")

if __name__ == "__main__":
    # Run all examples
    asyncio.run(run_all_examples())