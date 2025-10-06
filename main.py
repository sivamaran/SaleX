#!/usr/bin/env python3
"""
Lead Generation Application Orchestrator
Coordinates all scrapers based on ICP (Ideal Customer Profile) and user preferences.

Flow:
1. Define ICP (hardcoded for now)
2. User selects which scrapers to use
3. Generate queries using Gemini AI
4. Collect URLs using web_url_scraper
5. Classify URLs and route to appropriate scrapers
"""

import asyncio
import json
import os
import sys
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse
import logging
import re
import random
# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import scrapers
from web_url_scraper.main import main as web_url_scraper_main, initialize_application
from web_url_scraper.database_service import get_urls_by_type, get_url_type_statistics, get_urls_by_type_and_icp
from web_scraper.main_app import WebScraperOrchestrator
from instagram_scraper.main_optimized import OptimizedInstagramScraper, ScrapingConfig
from linkedin_scraper.main import LinkedInScraperMain, OptimizedLinkedInScraper
from yt_scraper.main import YouTubeScraperInterface
from facebook_scraper.main_optimized import OptimizedFacebookScraper, FacebookScrapingConfig
# from Company_directory.company_scraper_complete import UniversalScraper  # Commented out - company scraper disabled
from database.mongodb_manager import get_mongodb_manager
from filter_web_lead import MongoDBLeadProcessor
from contact_scraper import run_optimized_contact_scraper
from web.crl import run_web_crawler_async, get_mongodb_manager  # Commented out - crl.py removed from flow

# Scraper registry centralization
from scraper_registry import (
    get_available_scrapers,
    get_scraper_names,
    get_site_filter,
    get_prompt_block,
    get_url_type_map,
)

# Import Gemini AI (assuming it's available)
try:
    import google.generativeai as genai
    from dotenv import load_dotenv
    load_dotenv()
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    print("⚠️ Gemini AI not available. Install google-generativeai package.")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class LeadGenerationOrchestrator:
    """Main orchestrator for the lead generation application"""
    
    def __init__(self):
        """Initialize the orchestrator"""
        self.mongodb_manager = None
        # Centralized available scrapers
        self.available_scrapers = get_available_scrapers()
        
        # Instagram scraper performance configuration
        self.instagram_config = ScrapingConfig(
            max_workers=4,
            batch_size=5,
            context_pool_size=4,
            rate_limit_delay=1.0,
            context_reuse_limit=20
        )
        
        # Initialize MongoDB
        try:
            self.mongodb_manager = get_mongodb_manager()
            logger.info("✅ MongoDB connection initialized")
        except Exception as e:
            logger.warning(f"⚠️ Failed to initialize MongoDB: {e}")
        
        # Initialize Gemini AI if available
        if GEMINI_AVAILABLE and os.getenv('GEMINI_API_KEY'):
            try:
                genai.configure(api_key=os.getenv('GEMINI_API_KEY'))
                self.gemini_model = genai.GenerativeModel('gemini-2.0-flash')
                logger.info("✅ Gemini AI initialized")
            except Exception as e:
                logger.warning(f"⚠️ Failed to initialize Gemini AI: {e}")
                self.gemini_model = None
        else:
            self.gemini_model = None
            logger.warning("⚠️ Gemini AI not available")
    
    def generate_icp_identifier(self, icp_data: Dict[str, Any]) -> str:
        """
        Generate a unique identifier for the ICP data
        
        Args:
            icp_data: ICP data dictionary
            
        Returns:
            str: Unique ICP identifier
        """
        import hashlib
        import json
        
        # Create a hash of the ICP data for uniqueness
        icp_string = json.dumps(icp_data, sort_keys=True)
        icp_hash = hashlib.md5(icp_string.encode()).hexdigest()[:8]
        
        # Get product name for readability
        product_name = icp_data.get('product_details', {}).get('product_name', 'Unknown')
        product_slug = ''.join(c.lower() for c in product_name if c.isalnum() or c.isspace()).replace(' ', '-')[:20]
        
        # Create timestamp for uniqueness
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        
        return f"{product_slug}_{timestamp}_{icp_hash}"

    def get_hardcoded_icp(self) -> Dict[str, Any]:
        """
        Get hardcoded ICP (Ideal Customer Profile) data
        In future versions, this will come from user forms
        """
        return {
            "product_details": {
                "product_name": "Premium Bus Travel & Group Tour Services",
                "product_category": "Travel & Tourism/Transportation Services",
                "usps": [
                    "Luxury bus fleet with premium amenities",
                    "Custom corporate group travel packages",
                    "Exclusive high-end travel experiences",
                    "Professional tour planning and coordination",
                    "Cost-effective group travel solutions",
                    "24/7 customer support during travel"
                ],
                "pain_points_solved": [
                    "Complicated group travel logistics",
                    "Expensive individual travel arrangements",
                    "Lack of customized corporate travel options",
                    "Poor coordination for large group events",
                    "Safety concerns in group transportation",
                    "Time-consuming travel planning process"
                ]
            },
            "icp_information": {
                "target_industry": [
                    "Corporate Companies",
                    "Educational Institutions",
                    "Wedding Planners",
                    "Event Management",
                    "Religious Organizations",
                    "Sports Teams/Clubs",
                    "Family Reunion Organizers",
                    "Travel Influencers"
                ],
                "competitor_companies": [
                    "RedBus",
                    "MakeMyTrip",
                    "Yatra",
                    "Local tour operators",
                    "Private bus operators",
                    "Luxury Bus Company", 
                    "Premium Tour Operator", 
                    "Corporate Travel Agency"
                ],
                "company_size": "10-1000+ employees/members",
                "decision_maker_persona": [
                    "HR Manager",
                    "Event Coordinator",
                    "Travel Manager",
                    "Family Head/Organizer",
                    "Wedding Planner",
                    "School/College Administrator",
                    "Corporate Executive",
                    "Travel Influencer",
                    "Religious Leader/Organizer"
                ],
                "region": ["India", "Major Cities", "Tourist Destinations"],
                "budget_range": "$5,000-$50,000 annually",
                "occasions": [
                    "Corporate offsites",
                    "Wedding functions",
                    "Family vacations",
                    "Educational tours",
                    "Religious pilgrimages",
                    "Adventure trips",
                    "Destination weddings",
                    "Sports events"
                ]
            }
        }
    
    def get_user_scraper_selection(self) -> List[str]:
        """
        Get user's scraper selection
        For now, returns default selection with web_scraper
        """
        print("\n🎯 SCRAPER SELECTION")
        print("=" * 50)
        print("Available scrapers:")
        print("1. web_scraper (default) - General web scraping")
        print("2. instagram - Instagram profiles and posts")
        print("3. linkedin - LinkedIn profiles and companies")
        print("4. youtube - YouTube channels and videos")
        print("5. facebook - Facebook profiles, pages, and posts")
        
        selection = input("\nEnter scrapers to use (comma-separated, or press Enter for default): ").strip()
        
        if not selection:
            return ['web_scraper']
        
        selected = []
        scraper_map = {
            '1': 'web_scraper',
            '2': 'instagram', 
            '3': 'linkedin',
            '4': 'youtube',
            '5': 'facebook',
            'web_scraper': 'web_scraper',
            'instagram': 'instagram',
            'linkedin': 'linkedin',
            'youtube': 'youtube',
            'facebook': 'facebook'
        }
        
        for item in selection.split(','):
            item = item.strip().lower()
            if item in scraper_map:
                scraper_name = scraper_map[item]
                if scraper_name not in selected:
                    selected.append(scraper_name)
        
        return selected if selected else ['web_scraper']
    
    # def configure_instagram_performance(self):
    #     """Configure Instagram scraper performance settings"""
    #     print("\n⚡ INSTAGRAM SCRAPER PERFORMANCE CONFIGURATION")
    #     print("=" * 60)
    #     print("Current settings:")
    #     print(f"  - Max workers: {self.instagram_config.max_workers}")
    #     print(f"  - Batch size: {self.instagram_config.batch_size}")
    #     print(f"  - Context pool size: {self.instagram_config.context_pool_size}")
    #     print(f"  - Rate limit delay: {self.instagram_config.rate_limit_delay}s")
    #     print(f"  - Context reuse limit: {self.instagram_config.context_reuse_limit}")
        
    #     choice = input("\nConfigure Instagram performance? (y/n, default: n): ").strip().lower()
        
    #     if choice == 'y':
    #         try:
    #             max_workers = input(f"Max concurrent workers (current: {self.instagram_config.max_workers}): ").strip()
    #             if max_workers.isdigit():
    #                 self.instagram_config.max_workers = int(max_workers)
                
    #             batch_size = input(f"Batch size (current: {self.instagram_config.batch_size}): ").strip()
    #             if batch_size.isdigit():
    #                 self.instagram_config.batch_size = int(batch_size)
                
    #             context_pool_size = input(f"Context pool size (current: {self.instagram_config.context_pool_size}): ").strip()
    #             if context_pool_size.isdigit():
    #                 self.instagram_config.context_pool_size = int(context_pool_size)
                
    #             rate_limit_delay = input(f"Rate limit delay in seconds (current: {self.instagram_config.rate_limit_delay}): ").strip()
    #             if rate_limit_delay.replace('.', '').isdigit():
    #                 self.instagram_config.rate_limit_delay = float(rate_limit_delay)
                
    #             print(f"\n✅ Instagram performance settings updated!")
    #             print(f"  - Max workers: {self.instagram_config.max_workers}")
    #             print(f"  - Batch size: {self.instagram_config.batch_size}")
    #             print(f"  - Context pool size: {self.instagram_config.context_pool_size}")
    #             print(f"  - Rate limit delay: {self.instagram_config.rate_limit_delay}s")
                
    #         except Exception as e:
    #             logger.warning(f"⚠️ Error configuring Instagram performance: {e}")
    #             print("Using default settings.")
    
    async def generate_search_queries(self, icp_data: Dict[str, Any], selected_scrapers: List[str]) -> List[str]:
        """
        Generate search queries using Gemini AI based on ICP data
        Then add platform-specific queries based on selected scrapers
        """
        if not self.gemini_model:
            # Fallback queries if Gemini is not available
            logger.warning("Using fallback queries - Gemini AI not available")
            return self._get_fallback_queries(icp_data)
        
        try:
            # Create prompt for Gemini
            prompt = self._create_gemini_prompt(icp_data)
            
            logger.info("🤖 Generating search queries with Gemini AI...")
            response = await asyncio.to_thread(self.gemini_model.generate_content, prompt)
            
            # Parse the response to extract queries
            base_queries = self._parse_gemini_response(response.text)
            print('*' * 80)
            print(base_queries)
            print('*' * 80)

            queries = base_queries[:2]  # Limit to 2 queries
            # Add platform-specific queries based on selected scrapers
            all_queries = self._add_platform_specific_queries(queries, selected_scrapers)
        
            logger.info(f"✅ Generated {len(all_queries)} total search queries ({len(queries)} base + {len(all_queries) - len(queries)} platform-specific)")
            return all_queries
            
        except Exception as e:
            logger.error(f"❌ Error generating queries with Gemini: {e}")
            return self._get_fallback_queries(icp_data)

    async def generate_platform_queries(self, icp_data: Dict[str, Any], platform: str) -> List[str]:
        """
        Generate platform-specific search queries only (no general queries).
        Ensures queries include strict site: filters and helpful operators.
        """
        platform = platform.strip().lower()
        if not self.gemini_model:
            return self._get_fallback_platform_queries(icp_data, platform)

        try:
            prompt = self._create_platform_prompt(icp_data, platform)
            logger.info(f"🤖 Generating platform-specific queries for {platform} with Gemini AI...")
            response = await asyncio.to_thread(self.gemini_model.generate_content, prompt)
            raw_lines = (response.text or '').split('\n')
            # Clean and keep only non-empty lines
            queries = []
            for line in raw_lines:
                q = line.strip().lstrip('0123456789.-• "\'"\'"').rstrip('"\'"\'"')
                if q and len(q) > 10:
                    queries.append(q)
            # De-duplicate while preserving order
            seen = set()
            deduped = []
            for q in queries:
                if q not in seen:
                    seen.add(q)
                    deduped.append(q)
            return deduped[:8]
        except Exception as e:
            logger.error(f"❌ Error generating platform queries with Gemini: {e}")
            return self._get_fallback_platform_queries(icp_data, platform)
    
    def _add_platform_specific_queries(self, base_queries: List[str], selected_scrapers: List[str]) -> List[str]:
        """
        Add platform-specific versions of base queries based on selected scrapers
        """
        all_queries = base_queries.copy()
        
        # Platform keywords mapping
        # Obtain site filter per scraper from registry
        
        # Add platform-specific queries
        for scraper in selected_scrapers:
            platform_keyword = get_site_filter(scraper)
            if platform_keyword:
                logger.info(f"🔍 Adding {platform_keyword} specific queries...")
                
                for query in base_queries:
                    # Strengthen with intitle and exact persona/industry signals if present
                    enhanced_query = query
                    if 'director' in query.lower() or 'manager' in query.lower() or 'head' in query.lower():
                        enhanced_query = f'intitle:("director" OR "manager" OR "head") {query}'
                    # Add platform site filter
                    platform_query = f"{enhanced_query} {platform_keyword}".strip()
                    all_queries.append(platform_query)
        
        logger.info(f"📊 Query breakdown:")
        logger.info(f"  - Base queries: {len(base_queries)}")
        for scraper in selected_scrapers:
            if get_site_filter(scraper):
                logger.info(f"  - {scraper} queries: {len(base_queries)}")
        
        return all_queries
    def _create_gemini_prompt(self, icp_data: Dict[str, Any]) -> str:
        """Create a prompt for Gemini AI to generate search queries"""
        product = icp_data.get("product_details", {})
        icp = icp_data.get("icp_information", {})
        
        # Process regions to filter out generic/invalid location terms
        regions = icp.get("region", [])
        valid_regions = []
        
        # List of generic terms that should not be used as location filters
        generic_terms = [
            "major cities", "metropolitan areas", "urban areas", "rural areas",
            "tourist destinations", "business districts", "commercial areas",
            "developed countries", "developing countries", "emerging markets",
            "tier 1 cities", "tier 2 cities", "suburbs", "downtown areas"
        ]
        
        for region in regions:
            # Convert to lowercase for comparison
            region_lower = region.lower().strip()
            
            # Skip empty or generic terms
            if region_lower and region_lower not in generic_terms:
                # Additional check: skip very short terms that are likely generic
                if len(region_lower) > 3:
                    valid_regions.append(region)
        
        # Create location context for the prompt
        location_instruction = ""
        if valid_regions:
            location_instruction = f"""
            IMPORTANT: Include location-specific search queries using these valid regions: {', '.join(valid_regions)}
            - Incorporate these location terms naturally into the search queries
            - Use variations like "in [location]", "[location] based", "[location] companies"
            """
        else:
            location_instruction = """
            IMPORTANT: Keep all search queries generic without location-specific terms since no valid regions were specified.
            """

        prompt = f"""
        Based on the following Ideal Customer Profile (ICP), generate 6 specific Google search queries that would help find potential customers:

        BUSINESS DETAILS:
        - Product/Service: {product.get("product_name", "Not specified")}
        - Category: {product.get("product_category", "Not specified")}
        - Key Benefits: {', '.join(product.get("usps", []))}
        - Problems Solved: {', '.join(product.get("pain_points_solved", []))}

        TARGET CUSTOMER PROFILE:
        - Target Industries: {', '.join(icp.get("target_industry", []))}
        - Company Size: {icp.get("company_size", "Not specified")}
        - Decision Makers: {', '.join(icp.get("decision_maker_persona", []))}
        - Geographic Regions: {', '.join(icp.get("region", []))}
        - Budget Range: {icp.get("budget_range", "Not specified")}
        - Occasions: {', '.join(icp.get("occasions", []))}
        
        {location_instruction}

        Generate search queries that would help identify potential customers who:
        1. Are actively looking for solutions to the problems this product/service solves
        2. Belong to the target industries mentioned above
        3. Match the company size and decision maker profiles
        4. Are located in the specified regions
        5. Have budget considerations that align with the offering

        Focus on search terms that indicate:
        - Active problem-solving or solution-seeking behavior
        - Industry-specific pain points and needs
        - Decision-making activities and budget planning
        - Geographic and demographic indicators
        - Timeline indicators ("2024", "2025", "looking for", "need", "planning")

        Format: Return only the search queries, one per line, without numbering or additional text.
        """
        return prompt

    def _create_platform_prompt(self, icp_data: Dict[str, Any], platform: str) -> str:
        """Create a strict platform-specific prompt for Gemini queries."""
        product = icp_data.get("product_details", {})
        icp = icp_data.get("icp_information", {})

        platform = platform.lower()
        platform_block = get_prompt_block(platform)

        prompt = f"""
        Based on the following Ideal Customer Profile (ICP), generate 4 platform-specific Google queries ONLY for {platform}.

        BUSINESS DETAILS:
        - Product/Service: {product.get("product_name", "Not specified")}
        - Category: {product.get("product_category", "Not specified")}
        - Key Benefits: {', '.join(product.get("usps", []))}
        - Problems Solved: {', '.join(product.get("pain_points_solved", []))}

        TARGET CUSTOMER PROFILE:
        - Target Industries: {', '.join(icp.get("target_industry", []))}
        - Decision Makers: {', '.join(icp.get("decision_maker_persona", []))}
        - Regions: {', '.join(icp.get("region", []))}

        REQUIREMENTS:
        {platform_block}
        - Reflect ICP signals (industries, personas, occasions, competitors) in phrasing.
        - Use boolean operators (OR) and quotes for exact titles when useful.
        - Return queries ONLY (one per line). Do NOT include any commentary or numbering.

        EXAMPLE STYLE (do not copy textually):
        - intitle:"marketing director" "SaaS" site:linkedin.com/in
        - ("Sports Team Manager" OR "Club Organizer") "team travel" site:instagram.com
        - intitle:"travel agency" (corporate OR group) india -site:instagram.com -site:linkedin.com -site:youtube.com
        """
        return prompt
    
    def _parse_gemini_response(self, response_text: str) -> List[str]:
        """Parse Gemini response to extract search queries"""
        queries = []
        lines = response_text.strip().split('\n')
        
        for line in lines:
            line = line.strip()
            # Remove numbering, bullets, quotation marks, etc.
            line = line.lstrip('0123456789.-• "\'')
            line = line.rstrip('"\'')
            
            # Basic validation - check for minimum length and travel-related keywords
            # travel_keywords = [
            #     'travel', 'trip', 'tour', 'vacation', 'holiday', 'outing', 'wedding',
            #     'corporate', 'group', 'family', 'pilgrimage', 'destination', 'bus',
            #     'transport', 'planning', 'organizing', 'visiting', 'visit', 'travelling',
            #     'journey', 'excursion', 'adventure','sightseeing', 'backpacking', 'trekking', 'hiking',
            #     'roadtrip', 'road trip', 'picnic', 'camping', 'booking', 'reservation', 'package', 'deal', 'offer',
            #     'explore', 'exploring', 'discover', 'discovering', 'wanderlust','company trip', 'staff outing',
            #     'event', 'gathering', 'yatra','reunion', 'get-together', 'meetup'
            # ]
            
            # if line and len(line) > 15:  # Increased minimum length
            #     # Check if the query contains at least one travel-related keyword
            #     if any(keyword.lower() in line.lower() for keyword in travel_keywords):
            #         queries.append(line)
            
            if line and len(line) > 15:  # Increased minimum length
                queries.append(line)

        return queries
    
    def _get_fallback_queries(self, icp_data: Dict[str, Any]) -> List[str]:
        """Fallback search queries when Gemini is not available"""
        logger.info("Using fallback queries - Gemini AI not available")
        industries = icp_data["icp_information"]["target_industry"]
        
        base_queries = [
            "Corporations planning team outings",
            "Families organizing reunions or vacations"
        ]
        
        return base_queries

    def _get_fallback_platform_queries(self, icp_data: Dict[str, Any], platform: str) -> List[str]:
        """Fallback queries tailored to a specific platform."""
        platform = platform.lower()
        icp = icp_data.get('icp_information', {})
        industries = icp.get('target_industry', [])
        personas = icp.get('decision_maker_persona', [])
        regions = icp.get('region', [])

        def pick_first(items: List[str]) -> str:
            return items[0] if items else ''

        industry = pick_first(industries)
        persona = pick_first(personas)
        region = pick_first(regions)

        if platform == 'linkedin':
            return [
                f'intitle:"{persona or "marketing director"}" "{industry or "SaaS"}" {region or "India"} site:linkedin.com/in'.strip(),
                f'("{persona or "travel manager"}" OR "event coordinator") "group travel" {region or "India"} site:linkedin.com/company',
                f'intitle:"operations head" "corporate travel" {region or "India"} site:linkedin.com/in'
            ]
        if platform == 'instagram':
            return [
                f'("{persona or "club organizer"}" OR "sports team manager") "team travel" {region or "India"} site:instagram.com',
                f'intitle:"travel agency" (corporate OR group) {region or "India"} site:instagram.com',
                f'("wedding planner" OR "event manager") "guest transport" {region or "India"} site:instagram.com'
            ]
        if platform == 'youtube':
            return [
                f'intitle:"corporate travel" {region or "India"} site:youtube.com/@',
                f'("travel agency" OR "tour operator") "group travel" {region or "India"} site:youtube.com/channel',
                f'intitle:"team travel" {region or "India"} site:youtube.com'
            ]
        # web_scraper/general
        return [
            f'intitle:"travel agency" (corporate OR group) {region or "India"} -site:instagram.com -site:linkedin.com -site:youtube.com',
            f'("bus charter" OR "coach hire") {region or "India"} (corporate OR wedding) -site:instagram.com -site:linkedin.com -site:youtube.com',
            f'intitle:"tour operator" "group travel" {region or "India"} -site:instagram.com -site:linkedin.com -site:youtube.com'
        ]
    
    async def collect_urls_from_queries(self, queries: List[str], icp_identifier: str = 'default') -> Dict[str, List[str]]:
        """
        Use web_url_scraper to collect URLs for each query
        """
        logger.info(f"🔍 Collecting URLs for {len(queries)} queries...")
        
        # Initialize web_url_scraper
        success = initialize_application()
        if not success:
            logger.warning("⚠️ web_url_scraper initialization failed, but continuing...")
        # Always continue even if initialization fails
        
        all_urls = []
        
        for i, query in enumerate(queries, 1):
            logger.info(f"[{i}/{len(queries)}] Processing query: {query}")
            
            try:
                # Run web_url_scraper for this query
                success = web_url_scraper_main(query, icp_identifier)
                if success:
                    logger.info(f"✅ Successfully processed query: {query}")
                else:
                    logger.warning(f"⚠️ Failed to process query: {query}")
                    # Ensure collection exists even if query processing fails
                    try:
                        from web_url_scraper.database_service import ensure_collection_exists
                        ensure_collection_exists()
                    except Exception as e:
                        logger.error(f"❌ Failed to ensure collection exists: {e}")
                
                # Add delay between queries to avoid rate limiting
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"❌ Error processing query '{query}': {e}")
                # Ensure collection exists even if query processing fails
                try:
                    from web_url_scraper.database_service import ensure_collection_exists
                    ensure_collection_exists()
                except Exception as e:
                    logger.error(f"❌ Failed to ensure collection exists: {e}")

        try:
            # Get URL type statistics first to see what's available
            stats = get_url_type_statistics()
            logger.info(f"📊 Database contains {stats['total_urls']} URLs across {stats['unique_url_types']} types")
            
            # Initialize classified_urls dictionary from registry url types
            url_types = set(get_url_type_map().values()) or {'general'}
            classified_urls = {url_type: [] for url_type in url_types}
            
            # Get URLs for each type directly from database
            for url_type in classified_urls.keys():
                try:
                    urls_data = get_urls_by_type_and_icp(url_type, icp_identifier)
                    # Extract just the URLs from the database documents
                    urls = [doc['url'] for doc in urls_data if 'url' in doc]
                    classified_urls[url_type] = urls
                    
                    if urls:
                        logger.info(f"📊 {url_type.title()}: {len(urls)} URLs")
                        
                except Exception as e:
                    logger.error(f"❌ Error getting {url_type} URLs: {e}")
                    classified_urls[url_type] = []
            
            total_urls = sum(len(urls) for urls in classified_urls.values())
            logger.info(f"✅ Collected and classified {total_urls} URLs")
            return classified_urls
            
        except Exception as e:
            logger.error(f"❌ Error retrieving URLs from database: {e}")
            return {
                'instagram': [],
                'linkedin': [],
                'youtube': [],
                'general': []
            }
    
    def _classify_urls(self, urls_data: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """
        Classify URLs by type (instagram, linkedin, youtube, company_directory, general)
        """
        classified = {
            'instagram': [],
            'linkedin': [],
            'youtube': [],
            'facebook': [],
            'company_directory': [],
            'general': []
        }
        
        # Known company directory domains
        company_directory_domains = [
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
        ]
        
        for url_data in urls_data:
            url = url_data.get('url', '')
            domain = urlparse(url).netloc.lower()
            
            if 'instagram.com' in domain:
                classified['instagram'].append(url)
            elif 'linkedin.com' in domain:
                classified['linkedin'].append(url)
            elif 'youtube.com' in domain or 'youtu.be' in domain:
                classified['youtube'].append(url)
            elif 'facebook.com' in domain:
                classified['facebook'].append(url)
            elif any(cd_domain in domain for cd_domain in company_directory_domains):
                classified['company_directory'].append(url)
            else:
                classified['general'].append(url)
        
        # Log classification results
        for url_type, urls in classified.items():
            if urls:
                logger.info(f"📊 {url_type.title()}: {len(urls)} URLs")
        
        return classified
    
    def filter_valid_linkedin_urls(self, urls: List[str]) -> List[str]:
        """
        Filter and validate LinkedIn URLs to only include scrapeable profile, company, post, and newsletter URLs.
        
        Args:
            urls (List[str]): List of LinkedIn URLs to filter
            
        Returns:
            List[str]: List of valid LinkedIn URLs that can be scraped
        """
        valid_urls = []
        invalid_urls = []
        
        # Define valid LinkedIn URL patterns (including country-specific domains)
        valid_patterns = [
            # Profile URLs: /in/username or /in/username/ (www and country-specific domains)
            r'^https://(?:www|[a-z]{2})\.linkedin\.com/in/[a-zA-Z0-9\-_%]+/?(\?.*)?$',
            
            # Company URLs: /company/company-name or /company/company-name/ (www and country-specific domains)
            r'^https://(?:www|[a-z]{2})\.linkedin\.com/company/[a-zA-Z0-9\-_%]+/?(\?.*)?$',
            
            # Post URLs: /posts/username_post-activity-id or /feed/update/urn:li:activity:id
            r'^https://(?:www|[a-z]{2})\.linkedin\.com/posts/[a-zA-Z0-9\-_%]+-activity-\d+-[a-zA-Z0-9]+/?(\?.*)?$',
            r'^https://(?:www|[a-z]{2})\.linkedin\.com/feed/update/urn:li:activity:\d+/?(\?.*)?$',
            
            # Newsletter URLs: /newsletters/newsletter-name-id
            r'^https://(?:www|[a-z]{2})\.linkedin\.com/newsletters/[a-zA-Z0-9\-_%]+-\d+/?(\?.*)?$'
        ]
        
        # Define invalid URL patterns to explicitly exclude
        invalid_patterns = [
            r'^https://economicgraph\.linkedin\.com/',
            r'^https://careers\.linkedin\.com/',
            r'^https://.*\.linkedin\.com/legal/',
            r'^https://news\.linkedin\.com/',
            r'^https://.*\.linkedin\.com/learning/',
            r'^https://business\.linkedin\.com/',
            r'^https://.*\.linkedin\.com/pulse/',
            r'^https://help\.linkedin\.com/',
            r'^https://developer\.linkedin\.com/',
            r'^https://.*\.linkedin\.com/jobs/',
            r'^https://.*\.linkedin\.com/sales/',
            r'^https://.*\.linkedin\.com/talent/',
            r'^https://.*\.linkedin\.com/marketing/',
            r'^https://.*\.linkedin\.com/business/learning/',
            r'^https://.*\.linkedin\.com/checkpoint/',
            r'^https://.*\.linkedin\.com/authwall/',
            r'^https://.*\.linkedin\.com/signup/',
            r'^https://.*\.linkedin\.com/login/',
            r'^https://.*\.linkedin\.com/start/',
            r'^https://.*\.linkedin\.com/home/?$',
            r'^https://.*\.linkedin\.com/?$',
            r'^https://.*\.linkedin\.com/feed/?$'
        ]
        
        for url in urls:
            if not url or not isinstance(url, str):
                invalid_urls.append(url)
                continue
                
            url = url.strip()
            
            # Check if URL is in the invalid patterns first
            is_invalid = any(re.match(pattern, url, re.IGNORECASE) for pattern in invalid_patterns)
            
            if is_invalid:
                invalid_urls.append(url)
                continue
            
            # Check if URL matches any valid pattern
            is_valid = any(re.match(pattern, url, re.IGNORECASE) for pattern in valid_patterns)
            
            if is_valid:
                # Additional validation: ensure it's a proper LinkedIn domain (www or country-specific)
                parsed_url = urlparse(url)
                netloc_lower = parsed_url.netloc.lower()
                
                # Check for www.linkedin.com or country-specific domains (like ie.linkedin.com, in.linkedin.com, etc.)
                if (netloc_lower == 'www.linkedin.com' or 
                    (netloc_lower.endswith('.linkedin.com') and 
                    len(netloc_lower.split('.')[0]) == 2)):  # 2-letter country codes
                    valid_urls.append(url)
                else:
                    invalid_urls.append(url)
            else:
                invalid_urls.append(url)
        
        # Log results
        logger.info(f"📊 URL Filtering Results:")
        logger.info(f"   - Total URLs processed: {len(urls)}")
        logger.info(f"   - Valid URLs found: {len(valid_urls)}")
        logger.info(f"   - Invalid URLs filtered out: {len(invalid_urls)}")
        
        if invalid_urls:
            logger.debug(f"❌ Filtered out invalid URLs: {invalid_urls[:5]}{'...' if len(invalid_urls) > 5 else ''}")
        
        if valid_urls:
            logger.debug(f"✅ Valid URLs to process: {valid_urls[:5]}{'...' if len(valid_urls) > 5 else ''}")
        
        return valid_urls
    
    async def run_selected_scrapers(self, classified_urls: Dict[str, List[str]], 
                                  selected_scrapers: List[str], icp_data: Dict[str, Any], icp_identifier: str = 'default') -> Dict[str, Any]:
        """
        Run the selected scrapers on their respective URL collections
        """
        results = {}
        
        logger.info(f"🚀 Running {len(selected_scrapers)} selected scrapers...")
        
        # Run web_scraper (general URLs)
        if 'web_scraper' in selected_scrapers and classified_urls.get('general'):
            logger.info("🌐 Running web_scraper...")
            try:
                web_scraper = WebScraperOrchestrator(
                    enable_ai=False,
                    enable_quality_engine=False,
                    use_mongodb=True
                )
                urls_general = classified_urls['general']
                random.shuffle(urls_general)
                web_results = web_scraper.run_complete_pipeline(
                    urls=urls_general[:5],  # Limit to 5 URLs
                    export_format="json",
                    generate_final_leads=True,
                    icp_identifier=icp_identifier
                )
                
                # Transform and store web scraper results in unified collection
                # Collect unified leads from web scraper results if provided
                unified_leads_web = []
                if web_results.get('successful_leads'):
                    try:
                        # Get the successful leads data
                        leads_data = web_results['unified_leads']
                        unified_stats = self.mongodb_manager.insert_batch_unified_leads(leads_data) if leads_data else {
                            'success_count': 0,'duplicate_count':0,'failure_count':0,'total_processed':0
                        }
                        
                        # Update results with unified storage stats
                        web_results['unified_storage'] = unified_stats
                        logger.info(f"✅ Web scraper leads stored in unified collection: {unified_stats['success_count']} leads")
                        
                    except Exception as e:
                        logger.error(f"❌ Error storing web scraper leads in unified collection: {e}")
                        web_results['unified_storage_error'] = str(e)
                
                results['web_scraper'] = web_results
                logger.info(f"✅ Web scraper completed: {web_results.get('summary', {}).get('successful_leads', 0)} leads")
                
            except Exception as e:
                logger.error(f"❌ Web scraper failed: {e}")
                results['web_scraper'] = {'error': str(e)}

        # Run crl.py crawler (Google-search-driven lead extraction)
    # if 'crl_scraper' in selected_scrapers:
        logger.info("🔍 Running CRL web crawler...")
        try:
            if not icp_data:
                raise ValueError("ICP data not provided for CRL scraper")
            
            crl_results = await run_web_crawler_async(icp_data, icp_identifier=icp_identifier)
            
            # Store summary in results
            results['crl_scraper'] = crl_results
            logger.info(f"✅ CRL crawler completed: {crl_results['summary']['total_leads_found']} leads found")
            
        except Exception as e:
            logger.error(f"❌ CRL crawler failed: {e}")
            results['crl_scraper'] = {'error': str(e)}

        
        # # Run company_directory scraper (advanced business directory scraping) - COMMENTED OUT
        # if 'company_directory' in selected_scrapers and classified_urls.get('company_directory'):
        #     logger.info("🏢 Running advanced company directory scraper...")
        #     try:
        #         # Get ICP data for the scraper - use provided data or fallback to hardcoded
        #         if icp_data is None:
        #             icp_data = self.get_hardcoded_icp()
        #         
        #         # Extract service/product name from ICP data
        #         product_details = icp_data.get('product_details', {})
        #         service_name = product_details.get('product_name', 'services')
        #         
        #         # If product_name is too generic, try product_category
        #         if service_name == 'services' or len(service_name) < 3:
        #             service_name = product_details.get('product_category', 'services')
        #         
        #         # Clean up service name for search
        #         service_name = service_name.replace('Premium ', '').replace(' Services', '').strip()
        #         
        #         logger.info(f"🔍 Searching company directories for: {service_name}")
        #         
        #         company_directory_results = []
        #         urls_company_dir = classified_urls['company_directory']
        #         random.shuffle(urls_company_dir)
        #         
        #         # Process up to 3 company directory URLs
        #         for i, directory_url in enumerate(urls_company_dir[:3]):
        #             logger.info(f"📋 Processing company directory {i+1}/3: {directory_url}")
        #             
        #             try:
        #                 # Create UniversalScraper for this directory
        #                 scraper = UniversalScraper(url=directory_url)
        #                 
        #                 # Perform search on this directory
        #                 async with scraper:
        #                 results_data = await scraper.perform_search_on_directory(service_name)
        #                     
        #                 # Extract leads from results
        #                 extracted_leads = results_data.get("extracted_data", [])
        #                 company_directory_results.extend(extracted_leads)
        #                 
        #                 logger.info(f"✅ Extracted {len(extracted_leads)} leads from {directory_url}")
        #                 
        #                 # Respectful delay between directories
        #                 if i < 2:  # Don't delay after the last one
        #                     await asyncio.sleep(random.uniform(3, 7))
        #                     
        #             except Exception as e:
        #                 logger.warning(f"❌ Failed to scrape company directory {directory_url}: {e}")
        #                 continue
        #         
        #         # Transform and store company directory results in unified collection
        #         if company_directory_results:
        #             try:
        #                 # Transform leads to unified format
        #                 unified_leads_cd = []
        #                 for lead in company_directory_results:
        #                     try:
        #                         # Convert company directory format to unified format
        #                         unified_lead = {
        #                             "name": lead.get("name", ""),
        #                             "contact_info": {
        #                                 "email": lead.get("email", []),
        #                                 "phone": lead.get("phone", []),
        #                             "website": lead.get("websites", []),
        #                             "linkedin": lead.get("social_media", {}).get("linkedin"),
        #                             "address": lead.get("address", "")
        #                         },
        #                         "company_name": lead.get("organization", lead.get("company_name", "")),
        #                         "time": datetime.now().isoformat(),
        #                         "link_details": f"Company directory extraction from {lead.get('source', 'unknown')}",
        #                         "type": "lead",
        #                         "what_we_can_offer": "Business directory services",
        #                         "source_url": lead.get("source", ""),
        #                         "source_platform": lead.get("source_platform", "Company Directory"),
        #                         "location": lead.get("location", ""),
        #                         "industry": lead.get("industry", ""),
        #                         "company_type": lead.get("company_type", ""),
        #                         "bio": lead.get("bio", ""),
        #                         "icp_identifier": icp_identifier,
        #                         "lead_category": "B2B",
        #                         "lead_sub_category": lead.get("industry", ""),
        #                         "status": "active"
        #                     }
        #                     unified_leads_cd.append(unified_lead)
        #                 except Exception as e:
        #                     logger.debug(f"Failed to transform company directory lead: {e}")
        #                     continue
        #             
        #             # Store in unified collection
        #             unified_stats = self.mongodb_manager.insert_batch_unified_leads(unified_leads_cd) if unified_leads_cd else {
        #                 'success_count': 0, 'duplicate_count': 0, 'failure_count': 0, 'total_processed': 0
        #             }
        #             
        #             company_directory_final_results = {
        #                 'extracted_data': company_directory_results,
        #                 'unified_leads': unified_leads_cd,
        #                 'unified_storage': unified_stats,
        #                 'total_directories_processed': len(urls_company_dir[:3]),
        #                 'total_leads_extracted': len(company_directory_results)
        #             }
        #             
        #             logger.info(f"✅ Company directory leads stored in unified collection: {unified_stats['success_count']} leads")
        #             
        #         except Exception as e:
        #             logger.error(f"❌ Error storing company directory leads in unified collection: {e}")
        #             company_directory_final_results = {
        #                 'extracted_data': company_directory_results,
        #                 'unified_storage_error': str(e),
        #                 'total_directories_processed': len(urls_company_dir[:3]),
        #                 'total_leads_extracted': len(company_directory_results)
        #             }
        #     else:
        #         company_directory_final_results = {
        #             'extracted_data': [],
        #             'total_directories_processed': len(urls_company_dir[:3]),
        #             'total_leads_extracted': 0
        #         }
        #     
        #     results['company_directory'] = company_directory_final_results
        #     logger.info(f"✅ Company directory scraper completed: {len(company_directory_results)} leads from {len(urls_company_dir[:3])} directories")
        #     
        # except Exception as e:
        #     logger.error(f"❌ Company directory scraper failed: {e}")
        #     results['company_directory'] = {'error': str(e)}
        
        # Run Instagram scraper (optimized)
        if 'instagram' in selected_scrapers and classified_urls.get('instagram'):
            logger.info("📸 Running optimized Instagram scraper...")
            try:
                # Use configured Instagram scraper settings
                instagram_scraper = OptimizedInstagramScraper(
                    headless=True,
                    enable_anti_detection=True,
                    is_mobile=False,
                    use_mongodb=True,
                    config=self.instagram_config,
                    icp_identifier=icp_identifier
                )
                urls_instagram = classified_urls['instagram']
                random.shuffle(urls_instagram)
                instagram_urls = urls_instagram[:5]  # Limit to 5 URLs for better performance
                logger.info(f"Processing {len(instagram_urls)} Instagram URLs with optimized scraper...")
                logger.info(f"Instagram scraper config: {self.instagram_config.max_workers} workers, batch size {self.instagram_config.batch_size}, {self.instagram_config.context_pool_size} contexts")
                
                instagram_results = await instagram_scraper.scrape(instagram_urls)
                
                # Transform and store Instagram results in unified collection
                if instagram_results.get('data'):
                    try:
                        # Get the Instagram data
                        leads_data = instagram_results['data']
                        
                        # Use unified leads from scraper if provided; otherwise transform ALL types here
                        unified_leads = instagram_results.get('unified_leads') or []
                        if not unified_leads:
                            unified_leads = [instagram_scraper._transform_instagram_to_unified(entry, icp_identifier) for entry in leads_data]
                            unified_leads = [u for u in unified_leads if u]
                        unified_stats = self.mongodb_manager.insert_batch_unified_leads(unified_leads) if unified_leads else {
                            'success_count': 0,'duplicate_count':0,'failure_count':0,'total_processed':0
                        }
                        
                        # Update results with unified storage stats
                        instagram_results['unified_storage'] = unified_stats
                        logger.info(f"✅ Instagram leads stored in unified collection: {unified_stats['success_count']} leads")
                        
                        # Log validation statistics
                        valid_leads = unified_stats['success_count'] + unified_stats['duplicate_count']
                        invalid_leads = unified_stats['failure_count']
                        total_leads = unified_stats['total_processed']
                        
                        if total_leads > 0:
                            validation_rate = (valid_leads / total_leads) * 100
                            logger.info(f"📊 Instagram validation rate: {validation_rate:.1f}% ({valid_leads}/{total_leads} leads passed validation)")
                            logger.info(f"   - Valid leads: {valid_leads}")
                            logger.info(f"   - Invalid leads (skipped): {invalid_leads}")
                            logger.info(f"   - Duplicates: {unified_stats['duplicate_count']}")
                        
                    except Exception as e:
                        logger.error(f"❌ Error storing Instagram leads in unified collection: {e}")
                        instagram_results['unified_storage_error'] = str(e)
                
                results['instagram'] = instagram_results
                
                # Log performance metrics
                if instagram_results.get('summary', {}).get('performance_metrics'):
                    metrics = instagram_results['summary']['performance_metrics']
                    logger.info(f"✅ Instagram scraper completed: {len(instagram_results.get('data', []))} profiles")
                    logger.info(f"   - Throughput: {metrics.get('throughput_per_second', 0):.2f} URLs/second")
                    logger.info(f"   - Total time: {instagram_results['summary'].get('total_time_seconds', 0):.2f} seconds")
                    logger.info(f"   - Success rate: {instagram_results['summary'].get('success_rate', 0):.1f}%")
                else:
                    logger.info(f"✅ Instagram scraper completed: {len(instagram_results.get('data', []))} profiles")
                
            except Exception as e:
                logger.error(f"❌ Instagram scraper failed: {e}")
                results['instagram'] = {'error': str(e)}
        
        # Run LinkedIn scraper (optimized)
        if 'linkedin' in selected_scrapers and classified_urls.get('linkedin'):
            logger.info("💼 Running optimized LinkedIn scraper...")
            try:
                # Use optimized LinkedIn scraper with rate limit delay
                linkedin_scraper = OptimizedLinkedInScraper(
                    headless=True,
                    enable_anti_detection=True,
                    use_mongodb=True,
                    max_workers=3,
                    batch_size=5,
                    rate_limit_delay=1.0,
                    icp_identifier=icp_identifier
                )
                # Filter valid LinkedIn URLs before processing
                raw_linkedin_urls = classified_urls['linkedin']
                valid_linkedin_urls = self.filter_valid_linkedin_urls(raw_linkedin_urls)
                
                if not valid_linkedin_urls:
                    logger.warning("⚠️ No valid LinkedIn URLs found after filtering")
                    results['linkedin'] = {'error': 'No valid LinkedIn URLs to process'}

                random.shuffle(valid_linkedin_urls)
                linkedin_urls = valid_linkedin_urls[:5]  # Limit to 5 URLs
                logger.info(f"Processing {len(linkedin_urls)} LinkedIn URLs with optimized scraper...")
                logger.info(f"LinkedIn scraper config: {linkedin_scraper.max_workers} workers, batch size {linkedin_scraper.batch_size}, rate limit delay {linkedin_scraper.rate_limit_delay}s")
                
                linkedin_results = await linkedin_scraper.scrape_async(
                    linkedin_urls,
                    "linkedin_orchestrator_results.json"
                )
                
                # Transform and store LinkedIn results in unified collection
                if linkedin_results.get('scraped_data'):
                    try:
                        # Use unified leads from scraper if provided; otherwise transform here
                        unified_leads = linkedin_results.get('unified_leads') or []
                        if not unified_leads:
                            leads_data = linkedin_results['scraped_data']
                            unified_leads = [linkedin_scraper._transform_linkedin_to_unified(item, icp_identifier) for item in leads_data]
                            unified_leads = [u for u in unified_leads if u]
                        unified_stats = self.mongodb_manager.insert_batch_unified_leads(unified_leads) if unified_leads else {
                            'success_count': 0,'duplicate_count':0,'failure_count':0,'total_processed':0
                        }
                        
                        # Update results with unified storage stats
                        linkedin_results['unified_storage'] = unified_stats
                        logger.info(f"✅ LinkedIn leads stored in unified collection: {unified_stats['success_count']} leads")
                        
                    except Exception as e:
                        logger.error(f"❌ Error storing LinkedIn leads in unified collection: {e}")
                        linkedin_results['unified_storage_error'] = str(e)
                
                results['linkedin'] = linkedin_results
                
                # Log performance metrics
                metadata = linkedin_results.get('scraping_metadata', {})
                logger.info(f"✅ LinkedIn scraper completed: {metadata.get('successful_scrapes', 0)} profiles")
                logger.info(f"   - Max workers: {metadata.get('max_workers', 'N/A')}")
                logger.info(f"   - Batch size: {metadata.get('batch_size', 'N/A')}")
                logger.info(f"   - Sign-up pages detected: {metadata.get('signup_pages_detected', 0)}")
                logger.info(f"   - Sign-up pages retried: {metadata.get('signup_pages_retried', 0)}")
                
            except Exception as e:
                logger.error(f"❌ LinkedIn scraper failed: {e}")
                results['linkedin'] = {'error': str(e)}
        
        # Run YouTube scraper
        if 'youtube' in selected_scrapers and classified_urls.get('youtube'):
            logger.info("🎥 Running YouTube scraper...")
            try:
                youtube_scraper = YouTubeScraperInterface(
                    headless=True,
                    enable_anti_detection=True,
                    use_mongodb=True
                )
                youtube_urls = classified_urls['youtube'][:5]  # Limit to 5 URLs
                random.shuffle(youtube_urls)
                logger.info(f"Processing {len(youtube_urls)} YouTube URLs...")

                youtube_results = await youtube_scraper.scrape_multiple_urls(
                    youtube_urls, 
                    "youtube_orchestrator_results.json",
                    icp_identifier
                )
                # Transform and store YouTube results in unified collection
                if youtube_results.get('data'):
                    try:
                        # Use unified leads from scraper if provided; otherwise transform here
                        unified_leads = youtube_results.get('unified_leads') or []
                        if not unified_leads:
                            leads_data = youtube_results['data']
                            unified_leads = [youtube_scraper._transform_youtube_to_unified(item, icp_identifier) for item in leads_data]
                            unified_leads = [u for u in unified_leads if u]
                        
                        unified_stats = self.mongodb_manager.insert_batch_unified_leads(unified_leads) if unified_leads else {
                            'success_count': 0, 'duplicate_count': 0, 'failure_count': 0, 'total_processed': 0
                        }
                        
                        # Update results with unified storage stats
                        youtube_results['unified_storage'] = unified_stats
                        logger.info(f"✅ YouTube leads stored in unified collection: {unified_stats['success_count']} leads")
                        
                        # Log validation statistics
                        valid_leads = unified_stats['success_count'] + unified_stats['duplicate_count']
                        invalid_leads = unified_stats['failure_count']
                        total_leads = unified_stats['total_processed']
                        
                        if total_leads > 0:
                            validation_rate = (valid_leads / total_leads) * 100
                            logger.info(f"📊 YouTube validation rate: {validation_rate:.1f}% ({valid_leads}/{total_leads} leads passed validation)")
                            logger.info(f"   - Valid leads: {valid_leads}")
                            logger.info(f"   - Invalid leads (skipped): {invalid_leads}")
                            logger.info(f"   - Duplicates: {unified_stats['duplicate_count']}")
                        
                    except Exception as e:
                        logger.error(f"❌ Error storing YouTube leads in unified collection: {e}")
                        youtube_results['unified_storage_error'] = str(e)
                
                results['youtube'] = youtube_results
                
                # Log performance metrics
                summary = youtube_results.get('summary', {})
                logger.info(f"✅ YouTube scraper completed: {summary.get('successful_scrapes', 0)} channels/videos")
                logger.info(f"   - Total URLs processed: {summary.get('total_urls', 0)}")
                logger.info(f"   - Failed scrapes: {summary.get('failed_scrapes', 0)}")
                logger.info(f"   - Total time: {summary.get('total_time_seconds', 0):.2f} seconds")
                if summary.get('total_urls', 0) > 0:
                    success_rate = (summary.get('successful_scrapes', 0) / summary.get('total_urls', 1)) * 100
                    logger.info(f"   - Success rate: {success_rate:.1f}%")
            except Exception as e:
                logger.error(f"❌ YouTube scraper failed: {e}")
                results['youtube'] = {'error': str(e)}
        
        # Run Facebook scraper
        if 'facebook' in selected_scrapers and classified_urls.get('facebook'):
            logger.info("📘 Running optimized Facebook scraper...")
            try:
                # Use configured Facebook scraper settings
                facebook_config = FacebookScrapingConfig(
                    max_workers=3,
                    batch_size=3,  # Facebook is more restrictive
                    context_pool_size=3,
                    rate_limit_delay=3.0,  # Facebook needs more delay
                    context_reuse_limit=10
                )
                
                facebook_scraper = OptimizedFacebookScraper(
                    headless=True,
                    enable_anti_detection=True,
                    use_mongodb=True,
                    config=facebook_config,
                    icp_identifier=icp_identifier
                )
                urls_facebook = classified_urls['facebook']
                random.shuffle(urls_facebook)
                facebook_urls = urls_facebook[:3]  # Limit to 3 URLs for Facebook (more restrictive)
                logger.info(f"Processing {len(facebook_urls)} Facebook URLs with optimized scraper...")
                logger.info(f"Facebook scraper config: {facebook_config.max_workers} workers, batch size {facebook_config.batch_size}, {facebook_config.context_pool_size} contexts")
                
                facebook_results = await facebook_scraper.scrape(facebook_urls)
                
                # Transform and store Facebook results in unified collection
                if facebook_results.get('data'):
                    try:
                        # Get the Facebook data
                        leads_data = facebook_results['data']
                        
                        # Use unified leads from scraper if provided; otherwise transform here
                        unified_leads = facebook_results.get('unified_leads') or []
                        if not unified_leads:
                            unified_leads = [facebook_scraper._transform_facebook_to_unified(entry, icp_identifier) for entry in leads_data]
                            unified_leads = [u for u in unified_leads if u]
                        unified_stats = self.mongodb_manager.insert_batch_unified_leads(unified_leads) if unified_leads else {
                            'success_count': 0,'duplicate_count':0,'failure_count':0,'total_processed':0
                        }
                        
                        # Update results with unified storage stats
                        facebook_results['unified_storage'] = unified_stats
                        logger.info(f"✅ Facebook leads stored in unified collection: {unified_stats['success_count']} leads")
                        
                        # Log validation statistics
                        valid_leads = unified_stats['success_count'] + unified_stats['duplicate_count']
                        invalid_leads = unified_stats['failure_count']
                        total_leads = unified_stats['total_processed']
                        
                        if total_leads > 0:
                            validation_rate = (valid_leads / total_leads) * 100
                            logger.info(f"📊 Facebook validation rate: {validation_rate:.1f}% ({valid_leads}/{total_leads} leads passed validation)")
                            logger.info(f"   - Valid leads: {valid_leads}")
                            logger.info(f"   - Invalid leads (skipped): {invalid_leads}")
                            logger.info(f"   - Duplicates: {unified_stats['duplicate_count']}")
                        
                    except Exception as e:
                        logger.error(f"❌ Error storing Facebook leads in unified collection: {e}")
                        facebook_results['unified_storage_error'] = str(e)
                
                results['facebook'] = facebook_results
                
                # Log performance metrics
                if facebook_results.get('summary', {}).get('performance_metrics'):
                    metrics = facebook_results['summary']['performance_metrics']
                    logger.info(f"✅ Facebook scraper completed: {len(facebook_results.get('data', []))} profiles/pages")
                    logger.info(f"   - Throughput: {metrics.get('throughput_per_second', 0):.2f} URLs/second")
                    logger.info(f"   - Total time: {facebook_results['summary'].get('total_time_seconds', 0):.2f} seconds")
                    logger.info(f"   - Success rate: {facebook_results['summary'].get('success_rate', 0):.1f}%")
                else:
                    logger.info(f"✅ Facebook scraper completed: {len(facebook_results.get('data', []))} profiles/pages")
                
            except Exception as e:
                logger.error(f"❌ Facebook scraper failed: {e}")
                results['facebook'] = {'error': str(e)}

        return results
    
    def generate_final_report(self, icp_data: Dict[str, Any], selected_scrapers: List[str], 
                            results: Dict[str, Any]) -> str:
        """
        Generate a final report of the orchestration results
        """
        report_data = {
            "orchestration_metadata": {
                "timestamp": datetime.now().isoformat(),
                "icp_data": icp_data,
                "selected_scrapers": selected_scrapers,
                "total_scrapers_run": len([r for r in results.values() if not r.get('error')])
            },
            "results_summary": {},
            "detailed_results": results
        }
        
        # Generate summary for each scraper
        for scraper, result in results.items():
            """
            if scraper == 'lead_filtering':
                # Handle lead filtering results separately
                if result.get('error'):
                    report_data["results_summary"][scraper] = {"status": "failed", "error": result['error']}
                else:
                    filtering_stats = result.get('filtering_stats', {})
                    report_data["results_summary"][scraper] = {
                        "status": "success",
                        "leads_processed": filtering_stats.get('total', 0),
                        "leads_filtered": filtering_stats.get('filtered', 0),
                        "leads_extracted": filtering_stats.get('extracted', 0),
                        "leads_inserted": filtering_stats.get('inserted', 0),
                        "email_based_leads": filtering_stats.get('email_based', 0),
                        "phone_based_leads": filtering_stats.get('phone_based', 0)
                    }
            """
            if scraper == 'contact_enhancement':
                # Handle contact enhancement results separately
                if result.get('error'):
                    report_data["results_summary"][scraper] = {"status": "failed", "error": result['error']}
                else:
                    report_data["results_summary"][scraper] = {
                        "status": "success",
                        "enhanced_leads": result.get('enhanced_leads', 0),
                        "leads_with_emails": result.get('leads_with_emails', 0),
                        "leads_with_phones": result.get('leads_with_phones', 0)
                    }
            elif result.get('error'):
                report_data["results_summary"][scraper] = {"status": "failed", "error": result['error']}
            else:
                if scraper == 'web_scraper':
                    summary = result.get('summary', {})
                    report_data["results_summary"][scraper] = {
                        "status": "success",
                        "leads_found": summary.get('successful_leads', 0),
                        "urls_processed": summary.get('urls_processed', 0)
                    }
                elif scraper == 'instagram':
                    summary = result.get('summary', {})
                    performance_metrics = summary.get('performance_metrics', {})
                    report_data["results_summary"][scraper] = {
                        "status": "success",
                        "profiles_found": len(result.get('data', [])),
                        "success_rate": summary.get('success_rate', 0),
                        "total_time_seconds": summary.get('total_time_seconds', 0),
                        "throughput_per_second": performance_metrics.get('throughput_per_second', 0),
                        "max_workers": performance_metrics.get('max_workers', 0),
                        "batch_size": performance_metrics.get('batch_size', 0),
                        "contexts_used": performance_metrics.get('contexts_used', 0),
                        "additional_profiles_extracted": summary.get('additional_profiles_extracted', 0)
                    }
                elif scraper == 'linkedin':
                    metadata = result.get('scraping_metadata', {})
                    report_data["results_summary"][scraper] = {
                        "status": "success",
                        "profiles_found": metadata.get('successful_scrapes', 0),
                        "failed_scrapes": metadata.get('failed_scrapes', 0)
                    }
                elif scraper == 'youtube':
                    summary = result.get('summary', {})
                    unified_storage = result.get('unified_storage', {})
                    report_data["results_summary"][scraper] = {
                        "status": "success" if not result.get('error') else "failed",
                        "channels_videos_found": len(result.get('data', [])),
                        "successful_scrapes": summary.get('successful_scrapes', 0),
                        "failed_scrapes": summary.get('failed_scrapes', 0),
                        "total_urls_processed": summary.get('total_urls', 0),
                        "success_rate": (summary.get('successful_scrapes', 0) / summary.get('total_urls', 1)) * 100 if summary.get('total_urls', 0) > 0 else 0,
                        "total_time_seconds": summary.get('total_time_seconds', 0),
                        "unified_leads_stored": unified_storage.get('success_count', 0),
                        "duplicate_leads": unified_storage.get('duplicate_count', 0),
                        "failed_leads": unified_storage.get('failure_count', 0),
                        "validation_rate": ((unified_storage.get('success_count', 0) + unified_storage.get('duplicate_count', 0)) / unified_storage.get('total_processed', 1)) * 100 if unified_storage.get('total_processed', 0) > 0 else 0
                    }
                elif scraper == 'facebook':
                    summary = result.get('summary', {})
                    performance_metrics = summary.get('performance_metrics', {})
                    unified_storage = result.get('unified_storage', {})
                    report_data["results_summary"][scraper] = {
                        "status": "success",
                        "profiles_found": len(result.get('data', [])),
                        "success_rate": summary.get('success_rate', 0),
                        "total_time_seconds": summary.get('total_time_seconds', 0),
                        "throughput_per_second": performance_metrics.get('throughput_per_second', 0),
                        "max_workers": performance_metrics.get('max_workers', 0),
                        "batch_size": performance_metrics.get('batch_size', 0),
                        "contexts_used": performance_metrics.get('contexts_used', 0),
                        "unified_leads_stored": unified_storage.get('success_count', 0),
                        "duplicate_leads": unified_storage.get('duplicate_count', 0),
                        "failed_leads": unified_storage.get('failure_count', 0)
                    }
                # COMMENTED OUT - crl.py removed from flow
                # elif scraper == 'web_crawler':
                #     if result.get('success'):
                #         summary = result.get('summary', {})
                #         report_data["results_summary"][scraper] = {
                #             "status": "success",
                #             "leads_found": summary.get('total_leads_found', 0),
                #             "leads_stored": summary.get('leads_stored', 0),
                #             "duplicates_found": summary.get('duplicates_found', 0),
                #             "urls_crawled": summary.get('urls_crawled', 0),
                #             "execution_time_seconds": summary.get('execution_time_seconds', 0)
                #         }
                #     else:
                #         report_data["results_summary"][scraper] = {
                #             "status": "failed",
                #             "error": result.get('error', 'Unknown error')
                #         }
        
        # Save report
        report_filename = f"orchestration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        try:
            with open(report_filename, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"📊 Final report saved: {report_filename}")
            return report_filename
            
        except Exception as e:
            logger.error(f"❌ Failed to save report: {e}")
            return ""

    async def run_complete_orchestration(self):
        """
        Run the complete lead generation orchestration
        """
        print("\n" + "=" * 80)
        print("🚀 LEAD GENERATION ORCHESTRATOR")
        print("=" * 80)
        
        try:
            # Step 1: Get ICP data (hardcoded for now)
            logger.info("📋 Step 1: Loading ICP data...")
            icp_data = self.get_hardcoded_icp()
            
            # Generate ICP identifier
            icp_identifier = self.generate_icp_identifier(icp_data)
            logger.info(f"🏷️ Generated ICP identifier: {icp_identifier}")
            
            print(f"\n📊 ICP SUMMARY:")
            print(f"Product: {icp_data['product_details']['product_name']}")
            print(f"Target Industries: {', '.join(icp_data['icp_information']['target_industry'])}")
            print(f"Company Size: {icp_data['icp_information']['company_size']}")
            print(f"ICP Identifier: {icp_identifier}")
            
            # Step 1.5: Run web crawler for direct URL generation
            # COMMENTED OUT - crl.py removed from flow
            # logger.info("🕷️ Step 1.5: Running web crawler for direct URL generation...")
            # web_crawler_results = None
            # try:
            #     web_crawler_results = await run_web_crawler_async(icp_data, icp_identifier)
            #     
            #     if web_crawler_results['success']:
            #         summary = web_crawler_results['summary']
            #         print(f"\n🕷️ WEB CRAWLER RESULTS:")
            #         print(f"✅ URLs crawled: {summary['urls_crawled']}")
            #         print(f"✅ Leads found: {summary['total_leads_found']}")
            #         print(f"✅ Leads stored: {summary['leads_stored']}")
            #         print(f"✅ Duplicates found: {summary['duplicates_found']}")
            #         print(f"✅ Execution time: {summary['execution_time_seconds']:.2f}s")
            #     else:
            #         print(f"\n❌ Web crawler failed: {web_crawler_results.get('error', 'Unknown error')}")
            #         
            # except Exception as e:
            #     logger.error(f"❌ Error in web crawler: {e}")
            #     print(f"\n❌ Web crawler error: {e}")
            #     web_crawler_results = {'success': False, 'error': str(e)}
            
            # Step 2: Get user scraper selection
            logger.info("🎯 Step 2: Getting scraper selection...")
            selected_scrapers = self.get_user_scraper_selection()
            print(f"\n✅ Selected scrapers: {', '.join(selected_scrapers)}")
            
            # Step 3: Generate search queries with Gemini AI
            logger.info("🤖 Step 3: Generating search queries...")
            queries = await self.generate_search_queries(icp_data, selected_scrapers)
            print(f"\n📝 Generated {len(queries)} search queries:")
            print(queries)
            print("\n")
            
            # Step 4: Collect URLs using web_url_scraper
            logger.info("🔍 Step 4: Collecting URLs...")
            classified_urls = await self.collect_urls_from_queries(queries, icp_identifier)
            
            total_urls = sum(len(urls) for urls in classified_urls.values())
            print(f"\n📊 URL COLLECTION SUMMARY:")
            print(f"Total URLs collected: {total_urls}")
            
            if total_urls == 0:
                logger.warning("⚠️ No URLs collected. Exiting.")
                return
            
            # Step 5: Run selected scrapers
            logger.info("🚀 Step 5: Running scrapers...")
            results = await self.run_selected_scrapers(classified_urls, selected_scrapers, icp_identifier, icp_data)
            
            """
            # Step 6: Filter and process leads using MongoDBLeadProcessor
            logger.info("🧹 Step 6: Filtering and processing leads...")
            try:
                lead_processor = MongoDBLeadProcessor()
                
                # Create indexes for the target collection
                lead_processor.create_indexes()
                
                # Process all leads from web_leads collection to leadgen_leads collection
                filtering_results = lead_processor.process_leads(batch_size=50)
                
                # Get processing statistics
                processing_stats = lead_processor.get_processing_stats()
                
                print(f"\n📊 LEAD FILTERING SUMMARY:")
                print(f"Total web_leads processed: {filtering_results['total']}")
                print(f"Leads with valid emails or phones: {filtering_results['filtered']}")
                print(f"Individual leads extracted: {filtering_results['extracted']}")
                print(f"Leads inserted to leadgen_leads: {filtering_results['inserted']}")
                print(f"Email-based leads: {filtering_results.get('email_based', 0)}")
                print(f"Phone-based leads: {filtering_results.get('phone_based', 0)}")
                print(f"Unique companies: {processing_stats.get('unique_companies', 'N/A')}")
                print(f"Unique industries: {processing_stats.get('unique_industries', 'N/A')}")
                
                # Add filtering results to the main results
                results['lead_filtering'] = {
                    'filtering_stats': filtering_results,
                    'processing_stats': processing_stats
                }
                
                lead_processor.close_connection()
                
            except Exception as e:
                logger.error(f"❌ Error in lead filtering: {e}")
                results['lead_filtering'] = {'error': str(e)}
"""
            # Step 7: Enhance leads with contact information using contact scraper
            logger.info("📞 Step 7: Enhancing leads with contact information...")
            try:
                contact_enhancement_results = await run_optimized_contact_scraper(
                    limit=0,  # Process all leads without contact info
                    batch_size=20
                )
                
                print(f"\n📞 CONTACT ENHANCEMENT SUMMARY:")
                print(f"Total leads enhanced: {len(contact_enhancement_results)}")
                
                # Count leads with emails and phone numbers
                leads_with_emails = sum(1 for lead in contact_enhancement_results if lead.get('emails'))
                leads_with_phones = sum(1 for lead in contact_enhancement_results if lead.get('phone_numbers'))
                
                print(f"Leads with emails found: {leads_with_emails}")
                print(f"Leads with phone numbers found: {leads_with_phones}")
                
                # Add contact enhancement results to the main results
                results['contact_enhancement'] = {
                    'enhanced_leads': len(contact_enhancement_results),
                    'leads_with_emails': leads_with_emails,
                    'leads_with_phones': leads_with_phones,
                    'enhancement_data': contact_enhancement_results
                }
                
            except Exception as e:
                logger.error(f"❌ Error in contact enhancement: {e}")
                results['contact_enhancement'] = {'error': str(e)}

            # Step 8: Generate final report
            logger.info("📊 Step 8: Generating final report...")
            # Add web crawler results to the main results
            # COMMENTED OUT - crl.py removed from flow
            # if web_crawler_results:
            #     results['web_crawler'] = web_crawler_results
            report_file = self.generate_final_report(icp_data, selected_scrapers, results)
            
            # Final summary
            print(f"\n" + "=" * 80)
            print("🎉 ORCHESTRATION COMPLETED")
            print("=" * 80)
            print(f"\n📊 URL COLLECTION SUMMARY:")
            print(f"Total URLs collected: {total_urls}")
            for key, urls in classified_urls.items():
                print(f"{key}: {len(urls)}")

            successful_scrapers = len([r for r in results.values() if not r.get('error') and r != results.get('lead_filtering')])
            print(f"✅ Successful scrapers: {successful_scrapers}/{len(selected_scrapers)}")
            
            """
            # Show lead filtering results if available
            if 'lead_filtering' in results and not results['lead_filtering'].get('error'):
                filtering_stats = results['lead_filtering']['filtering_stats']
                print(f"\n🧹 LEAD FILTERING RESULTS:")
                print(f"✅ Leads processed: {filtering_stats['inserted']} leads extracted and stored")
                print(f"📧 Email-based leads: {filtering_stats.get('email_based', 0)}")
                print(f"📞 Phone-based leads: {filtering_stats.get('phone_based', 0)}")
            """
            # Show contact enhancement results if available
            if 'contact_enhancement' in results and not results['contact_enhancement'].get('error'):
                contact_stats = results['contact_enhancement']
                print(f"\n📞 CONTACT ENHANCEMENT RESULTS:")
                print(f"✅ Leads enhanced: {contact_stats['enhanced_leads']} leads with contact info added")
                print(f"📧 Leads with emails: {contact_stats['leads_with_emails']}")
                print(f"📞 Leads with phone numbers: {contact_stats['leads_with_phones']}")
            
            # Show web crawler results if available
            # COMMENTED OUT - crl.py removed from flow
            # if 'web_crawler' in results and results['web_crawler'].get('success'):
            #     web_stats = results['web_crawler']['summary']
            #     print(f"\n🕷️ WEB CRAWLER RESULTS:")
            #     print(f"✅ URLs crawled: {web_stats['urls_crawled']}")
            #     print(f"✅ Leads found: {web_stats['total_leads_found']}")
            #     print(f"✅ Leads stored: {web_stats['leads_stored']}")
            #     print(f"✅ Duplicates found: {web_stats['duplicates_found']}")
            #     print(f"⏱️ Execution time: {web_stats['execution_time_seconds']:.2f}s")
            
            if report_file:
                print(f"📊 Final report: {report_file}")
            
            print("=" * 80)
            
        except KeyboardInterrupt:
            logger.info("⚠️ Orchestration interrupted by user")
        except Exception as e:
            logger.error(f"❌ Critical error in orchestration: {e}")
            raise


async def main():
    """Main entry point"""
    orchestrator = LeadGenerationOrchestrator()
    await orchestrator.run_complete_orchestration()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        print(f"❌ Critical error: {e}")
        sys.exit(1)