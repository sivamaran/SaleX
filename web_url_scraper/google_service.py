import requests
import time
import re
from urllib.parse import quote, urlparse
from web_url_scraper.config import (
    GOOGLE_API_KEY, 
    GOOGLE_SEARCH_ENGINE_ID, 
    MAX_PAGES, 
    RESULTS_PER_PAGE
)

def search_google(query, start_index=1):
    """
    Search Google using Custom Search API and return results.
    
    Args:
        query (str): Search query
        start_index (int): Starting result index (1, 11, 21, etc.)
    
    Returns:
        list: List of dictionaries containing URL, title, and snippet
    """
    try:
        # Build request URL
        base_url = "https://www.googleapis.com/customsearch/v1"
        
        # Prepare query parameters
        params = {
            'key': GOOGLE_API_KEY,
            'cx': GOOGLE_SEARCH_ENGINE_ID,
            'q': query,
            'start': start_index,
            'num': RESULTS_PER_PAGE
        }
        
        # Add User-Agent header to avoid blocking
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        print(f"Searching Google for: {query} (start: {start_index})")
        
        # Make HTTP request
        response = requests.get(
            base_url, 
            params=params, 
            headers=headers, 
            timeout=30
        )
        
        # Check response status
        if response.status_code != 200:
            print(f"Google API error: {response.status_code} - {response.text}")
            return []
        
        # Parse JSON response
        data = response.json()
        
        # Check if 'items' key exists
        if 'items' not in data:
            print("No search results found")
            return []
        
        # Extract relevant data from each item
        results = []
        for item in data['items']:
            result = {
                'url': item.get('link', ''),
                'title': item.get('title', ''),
                'snippet': item.get('snippet', '')
            }
            results.append(result)
        
        print(f"Found {len(results)} results")
        return results
        
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error: {e}")
        return []

def search_multiple_pages(query, max_pages=MAX_PAGES):
    """
    Search Google across multiple pages and return all results.
    
    Args:
        query (str): Search query
        max_pages (int): Maximum number of pages to search
    
    Returns:
        list: Combined list of all results from all pages
    """
    all_results = []
    current_page = 1
    start_index = 1
    
    print(f"Starting multi-page search for: {query} (max pages: {max_pages})")
    
    while current_page <= max_pages:
        print(f"Processing page {current_page}...")
        
        # Search current page
        page_results = search_google(query, start_index)
        
        # If no results, break the loop
        if not page_results:
            print(f"No more results found on page {current_page}")
            break
        
        # Add results to collection
        all_results.extend(page_results)
        
        # Calculate next start index
        start_index = (current_page * RESULTS_PER_PAGE) + 1
        current_page += 1
        
        # Add delay between requests to be respectful
        if current_page <= max_pages:
            print("Waiting 1 second before next request...")
            time.sleep(2)
    
    print(f"Total results found: {len(all_results)}")
    return all_results

def is_valid_url(url):
    """
    Validate if a URL is properly formatted and reasonable.
    
    Args:
        url (str): URL to validate
    
    Returns:
        bool: True if valid, False otherwise
    """
    if not url:
        return False
    
    # Check if URL starts with http:// or https://
    if not url.startswith(('http://', 'https://')):
        return False
    
    # Check if URL length is reasonable
    if len(url) > 2000:
        return False
    
    # Basic URL regex pattern
    url_pattern = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    
    return bool(url_pattern.match(url))

def detect_url_type(url):
    """
    Detect the type of URL based on the domain.
    
    Args:
        url (str): URL to analyze
    
    Returns:
        str: URL type ('instagram', 'facebook', 'reddit', 'quora', 'twitter', 'linkedin', 'youtube', 'company_directory', 'general')
    """
    if not url:
        return 'general'
    
    try:
        # Parse the URL to get the domain
        parsed_url = urlparse(url)
        domain = parsed_url.netloc.lower()
        
        # Remove 'www.' prefix if present
        if domain.startswith('www.'):
            domain = domain[4:]
        
        # Check for exact domain matches or subdomains
        if domain == 'instagram.com' or domain.endswith('.instagram.com'):
            return 'instagram'
        elif domain == 'facebook.com' or domain.endswith('.facebook.com'):
            return 'facebook'
        elif domain == 'reddit.com' or domain.endswith('.reddit.com'):
            return 'reddit'
        elif domain == 'quora.com' or domain.endswith('.quora.com'):
            return 'quora'
        elif domain == 'twitter.com' or domain.endswith('.twitter.com') or domain == 'x.com' or domain.endswith('.x.com'):
            return 'twitter'
        elif domain == 'linkedin.com' or domain.endswith('.linkedin.com'):
            return 'linkedin'
        elif domain == 'youtube.com' or domain.endswith('.youtube.com'):
            return 'youtube'
        # Company directory domains
        elif any(cd_domain in domain for cd_domain in [
            'thomasnet.com', 'indiamart.com', 'kompass.com', 'yellowpages.com',
            'yelp.com', 'crunchbase.com', 'opencorporates.com', 'manta.com',
            'dexknows.com', 'superpages.com', 'bizdir.com', 'businessdirectory.com',
            'local.com', 'bbb.org', 'angieslist.com', 'houzz.com', 'thumbtack.com',
            'homeadvisor.com', 'angi.com', 'cylex.net', 'tuugo.us', 'hotfrog.com',
            'brownbook.net', 'citysearch.com', 'insiderpages.com', 'showmelocal.com',
            'getthedata.co', 'companycheck.co.uk', 'duedil.com', 'thesunbusinessdirectory.com',
            'yell.com', 'touchlocal.com', 'cylex-uk.co.uk', 'ukindex.co.uk',
            'findopen.co.uk', 'thesun.co.uk', 'scotsman.com', 'telegraph.co.uk',
            'independent.co.uk'
        ]):
            return 'company_directory'
        else:
            return 'general'
            
    except Exception as e:
        print(f"Error detecting URL type for {url}: {e}")
        return 'general'

def filter_valid_urls(urls_list):
    """
    Filter a list of URL dictionaries to only include valid URLs and add URL type.
    
    Args:
        urls_list (list): List of URL dictionaries
    
    Returns:
        list: Filtered list with only valid URLs and added url_type field
    """
    valid_urls = []
    invalid_count = 0
    
    for url_data in urls_list:
        url = url_data.get('url', '')
        if is_valid_url(url):
            # Add URL type to the data
            url_data['url_type'] = detect_url_type(url)
            valid_urls.append(url_data)
        else:
            invalid_count += 1
    
    if invalid_count > 0:
        print(f"Filtered out {invalid_count} invalid URLs")
    
    print(f"Valid URLs remaining: {len(valid_urls)}")
    return valid_urls 