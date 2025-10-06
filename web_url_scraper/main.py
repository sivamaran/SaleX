import sys

from web_url_scraper.config import validate_config, get_config_summary 
from web_url_scraper.google_service import search_multiple_pages, filter_valid_urls, detect_url_type
from web_url_scraper.database_service import test_database_connection, setup_database_indexes, save_multiple_urls, initialize_database, get_urls_by_query, get_database_stats

def main(search_query, icp_identifier='default'):
    """
    Main execution function for the Google URL scraper.
    
    Args:
        search_query (str): The search query to process
        icp_identifier (str): ICP identifier for tracking
    """
    try:
        # Input validation
        if not search_query or len(search_query.strip()) == 0:
            print("Error: Search query cannot be empty")
            return False
        
        if len(search_query) > 200:
            print("Error: Search query is too long (max 200 characters)")
            return False
        
        # Clean search query
        search_query = search_query.strip()
        print(f"Starting search for: {search_query}")
        
        # Search execution: get list of url data dictionaries {url, title, snippet}
        print("Executing Google search...")
        all_results = search_multiple_pages(search_query)
        
        if not all_results:
            print("No search results found")
            return False
        
        print(f"Found {len(all_results)} total URLs")
        
        # URL processing - filter valid URLs
        print("Filtering valid URLs...")
        valid_urls = filter_valid_urls(all_results)
        
        if not valid_urls:
            print("No valid URLs found after filtering")
            return False
        
        print(f"Valid URLs to process: {len(valid_urls)}")
        
        # Database storage
        # Initialize ONCE at the start of your application
        initialize_database()
        print("Database ready!")
        print("Saving URLs to database...")
        stats = save_multiple_urls(valid_urls, search_query, icp_identifier)
        
        # Get URL type breakdown for the current search
        url_type_breakdown = {}
        for url_data in valid_urls:
            url_type = url_data.get('url_type', 'general')
            url_type_breakdown[url_type] = url_type_breakdown.get(url_type, 0) + 1
        
        # Results summary
        print("\n" + "="*50)
        print("SEARCH COMPLETED SUCCESSFULLY")
        print("="*50)
        print(f"Search Query: {search_query}")
        print(f"Total URLs Found: {len(all_results)}")
        print(f"Valid URLs: {len(valid_urls)}")
        print(f"New URLs Added: {stats['new_inserted']}")
        print(f"Duplicates Skipped: {stats['duplicates_skipped']}")
        
        # Display URL type breakdown
        if url_type_breakdown:
            print("\nURL Type Breakdown:")
            for url_type, count in url_type_breakdown.items():
                print(f"  {url_type.capitalize()}: {count}")
        
        print("="*50)
        
        return True
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

def initialize_application():
    """
    Initialize the application by validating configuration and testing connections.
    
    Returns:
        bool: True if initialization successful, False otherwise
    """
    print("Initializing Google URL Scraper...")
    print("-" * 40)
    
    # Configuration validation
    if not validate_config():
        print("Configuration validation failed. Please check your .env file.")
        return False
    
    # Test database connection
    print("\nTesting database connection...")
    if not test_database_connection():
        print("Database connection test failed.")
        return False
    
    # Setup database indexes
    print("\nSetting up database indexes...")
    setup_database_indexes()
    
    # Show configuration summary
    print("\nConfiguration Summary:")
    config_summary = get_config_summary()
    for key, value in config_summary.items():
        if 'key' not in key.lower() and 'id' not in key.lower():
            print(f"  {key}: {value}")
    
    # Show database statistics
    print("\nDatabase Statistics:")
    db_stats = get_database_stats()
    print(f"  Total URLs: {db_stats['total_urls']}")
    print(f"  Unique Search Queries: {db_stats['unique_search_queries']}")
    print(f"  Unique URL Types: {db_stats['unique_url_types']}")
    
    if db_stats['url_type_breakdown']:
        print("  URL Type Breakdown:")
        for url_type, count in db_stats['url_type_breakdown'].items():
            print(f"    {url_type.capitalize()}: {count}")
    
    print("\nApplication initialized successfully!")
    return True

def display_database_statistics():
    """
    Display comprehensive database statistics including URL type breakdown.
    """
    print("\n" + "="*50)
    print("DATABASE STATISTICS")
    print("="*50)
    
    try:
        stats = get_database_stats()
        
        print(f"Total URLs: {stats['total_urls']}")
        print(f"Unique Search Queries: {stats['unique_search_queries']}")
        print(f"Unique URL Types: {stats['unique_url_types']}")
        
        if stats['url_type_breakdown']:
            print("\nURL Type Breakdown:")
            for url_type, count in sorted(stats['url_type_breakdown'].items()):
                percentage = (count / stats['total_urls'] * 100) if stats['total_urls'] > 0 else 0
                print(f"  {url_type.capitalize()}: {count} ({percentage:.1f}%)")
        else:
            print("\nNo URLs found in database.")
            
    except Exception as e:
        print(f"Error getting database statistics: {e}")
    
    print("="*50)

def run_command_line_interface():
    """
    Handle command line interface and user input.
    """
    # Check if search query provided as command line argument
    if len(sys.argv) > 1:
        search_query = ' '.join(sys.argv[1:])
        print(f"Using search query from command line: {search_query}")
        
        # Run the main application
        success = main(search_query)
        
        if success:
            print("\nOperation completed successfully!")
        else:
            print("\nOperation failed. Please check the error messages above.")
            sys.exit(1)
    else:
        # Interactive mode
        while True:
            print("\nGoogle URL Scraper")
            print("=" * 30)
            print("1. Search for URLs")
            print("2. View Database Statistics")
            print("3. Exit")
            
            choice = input("\nSelect an option (1-3): ").strip()
            
            if choice == '1':
                search_query = input("Enter search query: ").strip()
                
                if not search_query:
                    print("No search query provided. Please try again.")
                    continue
                
                # Run the main application
                success = main(search_query)
                
                if success:
                    print("\nOperation completed successfully!")
                else:
                    print("\nOperation failed. Please check the error messages above.")
                    
            elif choice == '2':
                display_database_statistics()
                
            elif choice == '3':
                print("Exiting application...")
                break
                
            else:
                print("Invalid option. Please select 1, 2, or 3.")

if __name__ == "__main__":
    try:
        # Initialize application
        if not initialize_application():
            print("Application initialization failed. Exiting.")
            sys.exit(1)
        
        # Run command line interface
        run_command_line_interface()
        
    except KeyboardInterrupt:
        print("\nApplication interrupted by user. Exiting.")
        sys.exit(0)
    except Exception as e:
        print(f"Critical error: {e}")
        sys.exit(1) 