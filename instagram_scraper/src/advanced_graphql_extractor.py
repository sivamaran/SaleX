"""
Advanced GraphQL Extractor - Task: Basic Data Extraction
Uses browser automation with network request capture to extract GraphQL data

This module extends the browser manager to capture network requests
and extract GraphQL data from Instagram's actual API calls.
"""

import asyncio
import json
import re
import time
from typing import Dict, Any, Optional, List
from bs4 import BeautifulSoup
from instagram_scraper.src.browser_manager import BrowserManager
from instagram_scraper.src.error_handler import ErrorHandler, ErrorType


class AdvancedGraphQLExtractor:
    """Advanced GraphQL extractor with network request capture"""
    
    def __init__(self, headless: bool = True, enable_anti_detection: bool = True, is_mobile: bool = False):
        self.browser_manager = BrowserManager(headless=headless, enable_anti_detection=enable_anti_detection, is_mobile=is_mobile)
        self.network_requests = []
        self.graphql_responses = {}
        self.error_handler = ErrorHandler(max_retries=2, base_delay=2.0)
        
    async def start(self) -> None:
        """Initialize browser manager with network monitoring"""
        print("Starting browser manager...")
        await self.browser_manager.start()
        print("✓ Browser manager started")
        
        # Ensure page is available
        if not self.browser_manager.page:
            raise RuntimeError("Browser page not available after start")
        
        print(f"✓ Browser page available: {self.browser_manager.page}")
        
        # Set up network request monitoring
        await self._setup_network_monitoring()
        
    async def stop(self) -> None:
        """Clean up browser resources"""
        await self.browser_manager.stop()
        
    async def _setup_network_monitoring(self) -> None:
        """Set up network request monitoring"""
        if not self.browser_manager.page:
            raise RuntimeError("Browser page not available")
            
        print(f"✓ Setting up network monitoring for page: {self.browser_manager.page}")
        
        # Listen for network requests using proper event handling
        self.browser_manager.page.on("request", self._on_request)
        self.browser_manager.page.on("response", self._on_response)
        
        print("✓ Network monitoring setup completed")
        
    async def _on_request(self, request) -> None:
        """Handle network requests"""
        url = request.url
        
        # Filter for GraphQL and API requests - expanded to catch more endpoints
        if any(keyword in url for keyword in [
            '/graphql/', '/api/', 'graphql.instagram.com', 'graphql/query',
            'instagram.com/api/v1', 'instagram.com/api/v2',
            'instagram.com/graphql/query', 'instagram.com/graphql/ig_app_id'
        ]):
            req_data = {
                'type': 'request',
                'url': url,
                'method': request.method,
                'headers': dict(request.headers),
                'post_data': request.post_data,
                'timestamp': time.time()
            }
            #print(f"Request Data = Url: {req_data['url']}, request post data:{req_data['post_data']}")
            self.network_requests.append(req_data)
            
    async def _on_response(self, response) -> None:
        """Handle network responses"""
        url = response.url
        
        # Filter for GraphQL and API responses - expanded to catch more endpoints
        if any(keyword in url for keyword in [
            '/graphql/', '/api/', 'graphql.instagram.com', 'graphql/query',
            'instagram.com/api/v1', 'instagram.com/api/v2',
            'instagram.com/graphql/query', 'instagram.com/graphql/ig_app_id'
        ]):
            try:
                # Try to get response body
                body = await response.body()
                content_type = response.headers.get('content-type', '')
                
                response_data = {
                    'type': 'response',
                    'url': url,
                    'status': response.status,
                    'headers': dict(response.headers),
                    'content_type': content_type,
                    'body': body,
                    'timestamp': time.time()
                }
                #print(f"Response Data = Url: {response_data['url']}, response body:{response_data['body']}")
                
                # Only process successful responses (status 200)
                if response.status == 200:
                    # Try to parse JSON responses
                    if 'application/json' in content_type or 'text/javascript' in content_type or 'text/plain' in content_type:
                        try:
                            if body:
                                # Handle potential zstd compression
                                if 'zstd' in content_type or 'zstd' in response.headers.get('content-encoding', ''):
                                    import zstandard as zstd
                                    dctx = zstd.ZstdDecompressor()
                                    decompressed = dctx.decompress(body)
                                    text_body = decompressed.decode('utf-8')
                                else:
                                    text_body = body.decode('utf-8')
                                
                                # Remove potential "for (;;);" prefix
                                if text_body.startswith('for (;;);'):
                                    text_body = text_body[9:]
                                
                                try:
                                    json_data = json.loads(text_body)
                                    response_data['json_data'] = json_data
                                    
                                    # Check for errors in the response
                                    if 'errors' in json_data:
                                        print(f"❌ API Error: {json_data['errors']}")
                                    else:
                                        print(f"✅ Successful API Response: {url}")
                                    
                                    # Store GraphQL responses - expanded to catch more types
                                    if any(keyword in url for keyword in ['/graphql/', 'graphql.instagram.com']):
                                        self.graphql_responses[url] = json_data
                                    
                                    # Store API responses (especially web_profile_info)
                                    if '/api/v1/' in url:
                                        self.api_responses = getattr(self, 'api_responses', {})
                                        self.api_responses[url] = json_data
                                        
                                except json.JSONDecodeError:
                                    response_data['text_body'] = text_body[:1000]  # Store first 1000 chars
                                    
                        except Exception as e:
                            response_data['parse_error'] = str(e)
                else:
                    print(f"❌ Failed Response: {url} - Status: {response.status}")
                
                self.network_requests.append(response_data)
                
            except Exception as e:
                print(f"Error processing response: {e}")
    
    async def extract_graphql_data(self, url: str) -> Dict[str, Any]:
        """Extract GraphQL data from a specific Instagram page with retry logic"""
        print(f"Extracting GraphQL data from: {url}")
        
        # Use error handler with retry logic and enhanced error handling
        try:
            return await self.error_handler.retry_with_backoff(
                self._extract_graphql_data_internal, url
            )
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Error extracting data from {url}: {error_msg}")
            
            # Return structured error response
            return {
                'url': url,
                'error': error_msg,
                'success': False,
                'error_type': self.error_handler.classify_error(e).value,
                'html_length': 0,
                'text_length': 0,
                'network_requests': 0,
                'graphql_responses': 0,
                'api_responses': 0
            }
    
    async def _extract_graphql_data_internal(self, url: str) -> Dict[str, Any]:
        """Internal method to extract GraphQL data from a specific Instagram page"""
        # Clear previous requests
        self.network_requests = []
        self.graphql_responses = {}
        self.api_responses = {}
        
        try:
            # Navigate to the page and close popup
            popup_closed = await self.browser_manager.navigate_to_with_popup_close(url)
            print(f"✓ Navigation completed, popup closed: {popup_closed}")
            
            # Wait for page to load and network requests to complete
            await asyncio.sleep(10)
            
            # Get page content
            html_content = await self.browser_manager.get_page_content()
            rendered_text = await self.browser_manager.get_rendered_text()
            
            # Extract data from different sources
            extracted_data = {
                'url': url,
                'popup_closed': popup_closed,
                'html_length': len(html_content),
                'text_length': len(rendered_text),
                'network_requests': len(self.network_requests),
                'graphql_responses': len(self.graphql_responses),
                'api_responses': len(getattr(self, 'api_responses', {})),
                'graphql_data': {},
                'api_data': {},
                'user_data': {},
                'meta_data': {},
                'script_data': {},
                'page_analysis': {}
            }
            
            # 1. Extract GraphQL data from network responses
            extracted_data['graphql_data'] = self.graphql_responses
            
            # 2. Extract API data from network responses
            extracted_data['api_data'] = getattr(self, 'api_responses', {})
            
            # 3. Extract user data from successful API responses
            user_data = await self._extract_user_data_from_api()
            extracted_data['user_data'] = user_data
            
            # 4. Extract meta data
            meta_data = await self._extract_meta_data(html_content)
            extracted_data['meta_data'] = meta_data
            
            # 5. Extract data from scripts
            script_data = await self._extract_script_data(html_content)
            extracted_data['script_data'] = script_data
            
            # 6. Analyze page content
            page_analysis = await self._analyze_page_content(rendered_text, html_content)
            extracted_data['page_analysis'] = page_analysis
            
            # 7. Analyze network requests
            network_analysis = await self._analyze_network_requests()
            extracted_data['network_analysis'] = network_analysis
            # print("=" * 50)
            # print("\n Extracted Data graphql_data: ", extracted_data.get('graphql_data'))
            # print("\n Extracted Data api_data: ", extracted_data.get('api_data'))
            # print("\n Extracted Data user_data: ", extracted_data.get('user_data'))
            # print("\n Extracted Data meta_data: ", extracted_data.get('meta_data'))
            # print("\n Extracted Data script_data: ", extracted_data.get('script_data'))
            # print("=" * 50)
            return extracted_data
            
        except Exception as e:
            print(f"❌ Error extracting data from {url}: {e}")
            return {
                'url': url,
                'error': str(e),
                'success': False
            }
    
    async def _extract_user_data_from_api(self) -> Dict[str, Any]:
        """Extract user data from successful API responses"""
        user_data = {}
        
        # Look for web_profile_info API response
        for url, response in getattr(self, 'api_responses', {}).items():
            if 'web_profile_info' in url and 'data' in response:
                user_info = response.get('data', {}).get('user', {})
                if user_info:
                    user_data.update({
                        'username': user_info.get('username'),
                        'full_name': user_info.get('full_name'),
                        'biography': user_info.get('biography'),
                        'profile_pic_url': user_info.get('profile_pic_url'),
                        'profile_pic_url_hd': user_info.get('profile_pic_url_hd'),
                        'is_private': user_info.get('is_private'),
                        'is_verified': user_info.get('is_verified'),
                        'is_business_account': user_info.get('is_business_account'),
                        'followers_count': user_info.get('edge_followed_by', {}).get('count'),
                        'following_count': user_info.get('edge_follow', {}).get('count'),
                        'posts_count': user_info.get('edge_owner_to_timeline_media', {}).get('count'),
                        'user_id': user_info.get('id'),
                        'external_url': user_info.get('external_url'),
                        'has_public_story': user_info.get('has_public_story'),
                        'is_live': user_info.get('is_live'),
                        # Add business-related fields
                        'business_email': user_info.get('business_email'),
                        'business_phone_number': user_info.get('business_phone_number'),
                        'business_category_name': user_info.get('business_category_name'),
                        'is_professional_account': user_info.get('is_professional_account'),
                        'bio_links': user_info.get('bio_links', [])
                    })
                    print(f"✅ Extracted user data for: {user_data.get('username')}")
                    break
        
        # Fallback: Look for user data in GraphQL responses if API response failed
        if not user_data.get('username'):
            for url, response in getattr(self, 'graphql_responses', {}).items():
                if 'data' in response and 'user' in response.get('data', {}):
                    user_info = response.get('data', {}).get('user', {})
                    if user_info and user_info.get('username'):
                        user_data.update({
                            'username': user_info.get('username'),
                            'full_name': user_info.get('full_name'),
                            'biography': user_info.get('biography'),
                            'is_private': user_info.get('is_private'),
                            'is_verified': user_info.get('is_verified'),
                            'is_business_account': user_info.get('is_business_account'),
                            'followers_count': user_info.get('edge_followed_by', {}).get('count'),
                            'following_count': user_info.get('edge_follow', {}).get('count'),
                            'posts_count': user_info.get('edge_owner_to_timeline_media', {}).get('count'),
                            'user_id': user_info.get('id'),
                            'is_professional_account': user_info.get('is_professional_account'),
                            'bio_links': user_info.get('bio_links', [])
                        })
                        print(f"✅ Extracted user data from GraphQL for: {user_data.get('username')}")
                        break
        
        return user_data
    
    async def _extract_meta_data(self, html_content: str) -> Dict[str, Any]:
        """Extract meta data from HTML content with enhanced parsing"""
        soup = BeautifulSoup(html_content, 'html.parser')
        meta_data = {}
        
        # Extract all meta tags
        meta_tags = soup.find_all('meta')
        for meta in meta_tags:
            name = meta.get('name') or meta.get('property')
            content = meta.get('content')
            if name and content:
                meta_data[name] = content
        
        # Extract Open Graph data
        og_meta = {}
        og_tags = soup.find_all('meta', property=lambda x: x and x.startswith('og:'))
        for og_tag in og_tags:
            property_name = og_tag.get('property')
            content = og_tag.get('content')
            if property_name and content:
                og_meta[property_name] = content
        meta_data['open_graph'] = og_meta
        
        # Extract Twitter Card data
        twitter_meta = {}
        twitter_tags = soup.find_all('meta', attrs={'name': lambda x: x and x.startswith('twitter:')})
        for twitter_tag in twitter_tags:
            name = twitter_tag.get('name')
            content = twitter_tag.get('content')
            if name and content:
                twitter_meta[name] = content
        meta_data['twitter'] = twitter_meta
        
        # Extract title
        title_tag = soup.find('title')
        if title_tag:
            meta_data['title'] = title_tag.text
        
        # Enhanced parsing for Instagram-specific data
        enhanced_data = self._parse_instagram_meta_data(meta_data)
        meta_data.update(enhanced_data)
        
        return meta_data
    
    def _parse_instagram_meta_data(self, meta_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse Instagram-specific data from meta_data"""
        enhanced_data = {}
        
        # Extract data from description field
        description = meta_data.get('description', '')
        if description:
            # Parse likes and comments from description
            # Pattern: "76K likes, 7,967 comments - username on date"
            import re
            
            # Extract likes
            likes_match = re.search(r'(\d+(?:\.\d+)?[KMB]?)\s*likes?', description, re.IGNORECASE)
            if likes_match:
                enhanced_data['likes_count'] = likes_match.group(1)
            
            # Extract comments
            comments_match = re.search(r'(\d+(?:,\d+)*)\s*comments?', description, re.IGNORECASE)
            if comments_match:
                enhanced_data['comments_count'] = comments_match.group(1)
            
            # Extract username
            # Pattern: "87 likes, 45 comments - travelsapiens.in el July 18, 2025"
            # or "76K likes, 7,967 comments - realdonaldtrump on July 29, 2025"
            username_match = re.search(r'- ([a-zA-Z0-9._]+)\s+(?:el|on)\s+', description)
            if username_match:
                enhanced_data['username'] = username_match.group(1)
            
            # Extract date
            date_match = re.search(r'on (\w+ \d+, \d{4})', description)
            if date_match:
                enhanced_data['post_date'] = date_match.group(1)
        
        # Extract data from Open Graph
        og_data = meta_data.get('open_graph', {})
        
        # Extract username from og:title (more specific pattern)
        og_title = og_data.get('og:title', '')
        if og_title:
            # Pattern: "Awesome Himachal (@awesomehimachal) • Instagram video"
            # Look for username in parentheses before "• Instagram"
            username_match = re.search(r'\(@([a-zA-Z0-9._]+)\)\s*•\s*Instagram', og_title)
            if username_match:
                enhanced_data['username_from_title'] = username_match.group(1)
            
            # Extract full name
            full_name_match = re.search(r'^([^@]+?)\s*\(@', og_title)
            if full_name_match:
                enhanced_data['full_name'] = full_name_match.group(1).strip()
        
        # Extract username from Twitter title (more reliable)
        twitter_title = meta_data.get('twitter:title', '')
        if twitter_title:
            # Pattern: "Awesome Himachal (@awesomehimachal) • Instagram video"
            username_match = re.search(r'\(@([a-zA-Z0-9._]+)\)\s*•\s*Instagram', twitter_title)
            if username_match:
                enhanced_data['username_from_twitter'] = username_match.group(1)
        
        # Extract post/reel type from og:type
        og_type = og_data.get('og:type', '')
        if og_type:
            enhanced_data['content_type'] = og_type  # 'article' for posts, 'video' for reels
        
        # Extract URL from og:url
        og_url = og_data.get('og:url', '')
        if og_url:
            enhanced_data['post_url'] = og_url
            
            # Extract shortcode from URL
            shortcode_match = re.search(r'/([A-Za-z0-9_-]+)/?$', og_url)
            if shortcode_match:
                enhanced_data['shortcode'] = shortcode_match.group(1)
        
        # Extract caption from og:description
        og_description = og_data.get('og:description', '')
        if og_description:
            # Remove the likes/comments part to get just the caption
            caption_match = re.search(r':\s*"([^"]+)"', og_description)
            if caption_match:
                enhanced_data['caption'] = caption_match.group(1)
        
        return enhanced_data
    
    async def _extract_script_data(self, html_content: str) -> Dict[str, Any]:
        """Extract data from script tags"""
        soup = BeautifulSoup(html_content, 'html.parser')
        script_data = {}
        
        scripts = soup.find_all('script')
        
        for i, script in enumerate(scripts):
            if script.string:
                script_content = script.string
                
                # Look for various data patterns with more specific matching
                patterns = [
                    # Profile-specific patterns (more specific)
                    (r'"username"\s*:\s*"([^"]+)"', 'username'),
                    (r'"full_name"\s*:\s*"([^"]+)"', 'full_name'),
                    (r'"biography"\s*:\s*"([^"]+)"', 'biography'),
                    (r'"profile_pic_url"\s*:\s*"([^"]+)"', 'profile_pic_url'),
                    (r'"profile_pic_url_hd"\s*:\s*"([^"]+)"', 'profile_pic_url_hd'),
                    (r'"is_private"\s*:\s*(true|false)', 'is_private'),
                    (r'"is_verified"\s*:\s*(true|false)', 'is_verified'),
                    (r'"is_business_account"\s*:\s*(true|false)', 'is_business_account'),
                    (r'"is_professional_account"\s*:\s*(true|false)', 'is_professional_account'),
                    (r'"business_email"\s*:\s*"([^"]*)"', 'business_email'),
                    (r'"business_phone_number"\s*:\s*"([^"]*)"', 'business_phone_number'),
                    (r'"business_category_name"\s*:\s*"([^"]*)"', 'business_category_name'),
                    (r'"edge_followed_by"\s*:\s*{\s*"count"\s*:\s*(\d+)', 'followers'),
                    (r'"edge_follow"\s*:\s*{\s*"count"\s*:\s*(\d+)', 'following'),
                    (r'"edge_owner_to_timeline_media"\s*:\s*{\s*"count"\s*:\s*(\d+)', 'posts'),
                    
                    # Post-specific patterns
                    (r'"shortcode"\s*:\s*"([^"]+)"', 'shortcode'),
                    (r'"display_url"\s*:\s*"([^"]+)"', 'display_url'),
                    (r'"thumbnail_src"\s*:\s*"([^"]+)"', 'thumbnail_src'),
                    (r'"is_video"\s*:\s*(true|false)', 'is_video'),
                    (r'"caption"\s*:\s*"([^"]+)"', 'caption'),
                    (r'"taken_at_timestamp"\s*:\s*(\d+)', 'taken_at_timestamp'),
                    (r'"edge_media_preview_like"\s*:\s*{\s*"count"\s*:\s*(\d+)', 'likes'),
                    (r'"edge_media_to_comment"\s*:\s*{\s*"count"\s*:\s*(\d+)', 'comments'),
                    (r'"video_url"\s*:\s*"([^"]+)"', 'video_url'),
                    (r'"video_view_count"\s*:\s*(\d+)', 'video_view_count'),
                    
                    # Generic patterns (fallback)
                    (r'"followers"\s*:\s*(\d+)', 'followers'),
                    (r'"following"\s*:\s*(\d+)', 'following'),
                    (r'"posts"\s*:\s*(\d+)', 'posts'),
                    (r'"likes"\s*:\s*(\d+)', 'likes'),
                    (r'"comments"\s*:\s*(\d+)', 'comments'),
                    (r'"media_type"\s*:\s*"([^"]+)"', 'media_type'),
                ]
                
                for pattern, key in patterns:
                    matches = re.findall(pattern, script_content)
                    if matches:
                        # For most fields, take the first match
                        if key in ['username', 'full_name', 'biography', 'profile_pic_url', 'profile_pic_url_hd', 
                                 'shortcode', 'display_url', 'thumbnail_src', 'caption', 'video_url',
                                 'business_email', 'business_phone_number', 'business_category_name']:
                            script_data[key] = matches[0]
                        # For boolean fields, convert to boolean
                        elif key in ['is_private', 'is_verified', 'is_business_account', 'is_professional_account', 'is_video']:
                            script_data[key] = matches[0].lower() == 'true'
                        # For numeric fields, convert to int
                        elif key in ['followers', 'following', 'posts', 'likes', 'comments', 'taken_at_timestamp', 'video_view_count']:
                            try:
                                script_data[key] = int(matches[0])
                            except ValueError:
                                continue
                        # For other fields, take first match
                        else:
                            script_data[key] = matches[0]
        
        return script_data
    
    async def _analyze_page_content(self, rendered_text: str, html_content: str) -> Dict[str, Any]:
        """Analyze page content for Instagram-specific data"""
        analysis = {
            'instagram_keywords': [],
            'content_type': 'unknown',
            'has_profile_info': False,
            'has_post_info': False,
            'has_stories': False,
            'has_feed': False,
            'text_summary': ''
        }
        
        # Check for Instagram keywords
        instagram_keywords = [
            'followers', 'following', 'posts', 'likes', 'comments', 'share',
            'profile', 'bio', 'caption', 'story', 'reel', 'igtv', 'highlights',
            'instagram', 'follow', 'like', 'comment', 'save', 'bookmark'
        ]
        
        found_keywords = []
        for keyword in instagram_keywords:
            if keyword.lower() in rendered_text.lower():
                found_keywords.append(keyword)
        
        analysis['instagram_keywords'] = found_keywords
        
        # Determine content type
        if 'followers' in found_keywords and 'following' in found_keywords:
            analysis['content_type'] = 'profile_page'
            analysis['has_profile_info'] = True
        elif 'caption' in found_keywords or 'likes' in found_keywords:
            analysis['content_type'] = 'post_page'
            analysis['has_post_info'] = True
        elif 'story' in found_keywords:
            analysis['content_type'] = 'story_page'
            analysis['has_stories'] = True
        elif 'feed' in found_keywords or 'explore' in found_keywords:
            analysis['content_type'] = 'feed_page'
            analysis['has_feed'] = True
        
        # Create text summary
        lines = rendered_text.split('\n')
        non_empty_lines = [line.strip() for line in lines if line.strip()]
        analysis['text_summary'] = ' | '.join(non_empty_lines[:10])  # First 10 lines
        
        return analysis
    
    async def _analyze_network_requests(self) -> Dict[str, Any]:
        """Analyze captured network requests"""
        analysis = {
            'total_requests': len(self.network_requests),
            'graphql_requests': 0,
            'api_requests': 0,
            'successful_responses': 0,
            'failed_responses': 0,
            'request_types': {},
            'response_statuses': {}
        }
        
        for request in self.network_requests:
            url = request.get('url', '')
            
            if '/graphql/' in url:
                analysis['graphql_requests'] += 1
            elif '/api/' in url:
                analysis['api_requests'] += 1
            
            if request.get('type') == 'response':
                status = request.get('status', 0)
                analysis['response_statuses'][status] = analysis['response_statuses'].get(status, 0) + 1
                
                if 200 <= status < 300:
                    analysis['successful_responses'] += 1
                else:
                    analysis['failed_responses'] += 1
            
            method = request.get('method', 'GET')
            analysis['request_types'][method] = analysis['request_types'].get(method, 0) + 1
        
        return analysis
    
    async def extract_user_profile_data(self, username: str) -> Dict[str, Any]:
        """Extract user profile data"""
        url = f"https://www.instagram.com/{username}/"
        return await self.extract_graphql_data(url)
    
    async def extract_post_data(self, post_id: str) -> Dict[str, Any]:
        """Extract post data"""
        url = f"https://www.instagram.com/p/{post_id}/"
        return await self.extract_graphql_data(url)
    
    async def extract_reel_data(self, reel_id: str) -> Dict[str, Any]:
        """Extract reel data"""
        url = f"https://www.instagram.com/reel/{reel_id}/"
        return await self.extract_graphql_data(url)
    
    async def get_stealth_report(self) -> Dict[str, Any]:
        """Get comprehensive stealth report from browser manager"""
        return await self.browser_manager.get_stealth_report()
    
    async def execute_human_behavior(self, behavior_type: str, **kwargs) -> None:
        """Execute human-like behavior on the page"""
        if behavior_type == 'scroll':
            await self.browser_manager.execute_human_scroll(**kwargs)
        elif behavior_type == 'mousemove':
            await self.browser_manager.execute_human_mouse_move(**kwargs)
        elif behavior_type == 'click':
            await self.browser_manager.execute_human_click(**kwargs)
        else:
            raise ValueError(f"Unknown behavior type: {behavior_type}")

    async def save_scraped_data_to_json(self, profile_data: Dict[str, Any], post_data: Dict[str, Any], reel_data: Dict[str, Any], filename: str = "instagram_scraper/scraped_data.json") -> None:
        """Save all scraped data to a structured JSON file"""
        
        # Create structured data object
        scraped_data = {
            "metadata": {
                "scraping_timestamp": time.time(),
                "scraping_date": time.strftime("%Y-%m-%d %H:%M:%S"),
                "extractor_version": "advanced_graphql_extractor_v1.0",
                "total_extraction_time": "calculated_below"
            },
            "extraction_summary": {
                "profile_extraction": {
                    "success": bool(profile_data.get('user_data')),
                    "has_network_data": bool(profile_data.get('network_requests', 0) > 0),
                    "has_api_responses": bool(profile_data.get('api_data')),
                    "has_graphql_responses": bool(profile_data.get('graphql_data')),
                    "html_content_length": profile_data.get('html_length', 0),
                    "text_content_length": profile_data.get('text_length', 0)
                },
                "post_extraction": {
                    "success": bool(post_data.get('post_data', {}).get('extraction_success')),
                    "has_network_data": bool(post_data.get('network_requests', 0) > 0),
                    "has_api_responses": bool(post_data.get('api_data')),
                    "has_graphql_responses": bool(post_data.get('graphql_data')),
                    "html_content_length": post_data.get('html_length', 0),
                    "text_content_length": post_data.get('text_length', 0)
                },
                "reel_extraction": {
                    "success": bool(reel_data.get('reel_data', {}).get('extraction_success')),
                    "has_network_data": bool(reel_data.get('network_requests', 0) > 0),
                    "has_api_responses": bool(reel_data.get('api_data')),
                    "has_graphql_responses": bool(reel_data.get('graphql_data')),
                    "html_content_length": reel_data.get('html_length', 0),
                    "text_content_length": reel_data.get('text_length', 0)
                }
            },
            "profile_data": {
                "extracted_user_info": profile_data.get('user_data', {}),
                "network_analysis": profile_data.get('network_analysis', {}),
                "api_responses": profile_data.get('api_data', {}),
                "graphql_responses": profile_data.get('graphql_data', {}),
                "meta_data": profile_data.get('meta_data', {}),
                "script_data": profile_data.get('script_data', {}),
                "page_analysis": profile_data.get('page_analysis', {})
            },
            "post_data": {
                "extracted_post_info": post_data.get('post_data', {}),
                "network_analysis": post_data.get('network_analysis', {}),
                "api_responses": post_data.get('api_data', {}),
                "graphql_responses": post_data.get('graphql_data', {}),
                "meta_data": post_data.get('meta_data', {}),
                "script_data": post_data.get('script_data', {}),
                "page_analysis": post_data.get('page_analysis', {})
            },
            "reel_data": {
                "extracted_reel_info": reel_data.get('reel_data', {}),
                "network_analysis": reel_data.get('network_analysis', {}),
                "api_responses": reel_data.get('api_data', {}),
                "graphql_responses": reel_data.get('graphql_data', {}),
                "meta_data": reel_data.get('meta_data', {}),
                "script_data": reel_data.get('script_data', {}),
                "page_analysis": reel_data.get('page_analysis', {})
            },
            "raw_network_requests": {
                "profile_requests": profile_data.get('network_requests', []),
                "post_requests": post_data.get('network_requests', []),
                "reel_requests": reel_data.get('network_requests', [])
            }
        }
        
        # Add success indicators for each data type
        success_indicators = {
            "profile_success_indicators": {
                "username_extracted": bool(profile_data.get('user_data', {}).get('username')),
                "full_name_extracted": bool(profile_data.get('user_data', {}).get('full_name')),
                "followers_count_extracted": bool(profile_data.get('user_data', {}).get('followers_count')),
                "following_count_extracted": bool(profile_data.get('user_data', {}).get('following_count')),
                "posts_count_extracted": bool(profile_data.get('user_data', {}).get('posts_count')),
                "biography_extracted": bool(profile_data.get('user_data', {}).get('biography')),
                "profile_pic_extracted": bool(profile_data.get('user_data', {}).get('profile_pic_url')),
                "verification_status_extracted": bool(profile_data.get('user_data', {}).get('is_verified') is not None),
                "private_status_extracted": bool(profile_data.get('user_data', {}).get('is_private') is not None)
            },
            "post_success_indicators": {
                "caption_extracted": bool(post_data.get('post_data', {}).get('caption')) or bool(post_data.get('meta_data', {}).get('caption')),
                "author_extracted": bool(post_data.get('post_data', {}).get('author')) or bool(post_data.get('meta_data', {}).get('username')) or bool(post_data.get('meta_data', {}).get('username_from_title')),
                "shortcode_extracted": bool(post_data.get('post_data', {}).get('shortcode')) or bool(post_data.get('meta_data', {}).get('shortcode')),
                "post_id_extracted": bool(post_data.get('post_data', {}).get('post_id')),
                "likes_count_extracted": bool(post_data.get('post_data', {}).get('likes_count')) or bool(post_data.get('meta_data', {}).get('likes_count')),
                "comments_count_extracted": bool(post_data.get('post_data', {}).get('comments_count')) or bool(post_data.get('meta_data', {}).get('comments_count')),
                "media_urls_extracted": bool(post_data.get('post_data', {}).get('media_urls')),
                "is_video_detected": bool(post_data.get('post_data', {}).get('is_video') is not None) or (post_data.get('meta_data', {}).get('content_type') == 'video'),
                "video_url_extracted": bool(post_data.get('post_data', {}).get('video_url')),
                "thumbnail_extracted": bool(post_data.get('post_data', {}).get('thumbnail_url')),
                "full_name_extracted": bool(post_data.get('meta_data', {}).get('full_name')),
                "post_date_extracted": bool(post_data.get('meta_data', {}).get('post_date')),
                "post_url_extracted": bool(post_data.get('meta_data', {}).get('post_url'))
            },
            "reel_success_indicators": {
                "caption_extracted": bool(reel_data.get('reel_data', {}).get('caption')) or bool(reel_data.get('meta_data', {}).get('caption')),
                "author_extracted": bool(reel_data.get('reel_data', {}).get('author')) or bool(reel_data.get('meta_data', {}).get('username')) or bool(reel_data.get('meta_data', {}).get('username_from_title')),
                "shortcode_extracted": bool(reel_data.get('reel_data', {}).get('shortcode')) or bool(reel_data.get('meta_data', {}).get('shortcode')),
                "reel_id_extracted": bool(reel_data.get('reel_data', {}).get('reel_id')),
                "likes_count_extracted": bool(reel_data.get('reel_data', {}).get('likes_count')) or bool(reel_data.get('meta_data', {}).get('likes_count')),
                "comments_count_extracted": bool(reel_data.get('reel_data', {}).get('comments_count')) or bool(reel_data.get('meta_data', {}).get('comments_count')),
                "views_count_extracted": bool(reel_data.get('reel_data', {}).get('views_count')),
                "video_url_extracted": bool(reel_data.get('reel_data', {}).get('video_url')),
                "thumbnail_extracted": bool(reel_data.get('reel_data', {}).get('thumbnail_url')),
                "duration_extracted": bool(reel_data.get('reel_data', {}).get('duration')),
                "full_name_extracted": bool(reel_data.get('meta_data', {}).get('full_name')),
                "post_date_extracted": bool(reel_data.get('meta_data', {}).get('post_date')),
                "post_url_extracted": bool(reel_data.get('meta_data', {}).get('post_url')),
                "is_video_detected": bool(reel_data.get('reel_data', {}).get('is_video') is not None) or (reel_data.get('meta_data', {}).get('content_type') == 'video')
            }
        }
        
        scraped_data.update(success_indicators)
        
        # Add missing data analysis
        missing_data_analysis = {
            "missing_profile_data": [],
            "missing_post_data": [],
            "missing_reel_data": []
        }
        
        # Check for missing profile data
        profile_user_data = profile_data.get('user_data', {})
        if not profile_user_data.get('username'):
            missing_data_analysis["missing_profile_data"].append("username")
        if not profile_user_data.get('full_name'):
            missing_data_analysis["missing_profile_data"].append("full_name")
        if not profile_user_data.get('followers_count'):
            missing_data_analysis["missing_profile_data"].append("followers_count")
        if not profile_user_data.get('following_count'):
            missing_data_analysis["missing_profile_data"].append("following_count")
        if not profile_user_data.get('posts_count'):
            missing_data_analysis["missing_profile_data"].append("posts_count")
        if not profile_user_data.get('biography'):
            missing_data_analysis["missing_profile_data"].append("biography")
        if not profile_user_data.get('profile_pic_url'):
            missing_data_analysis["missing_profile_data"].append("profile_pic_url")
        
        # Check for missing post data
        post_extracted_data = post_data.get('post_data', {})
        post_meta_data = post_data.get('meta_data', {})
        
        if not post_extracted_data.get('caption') and not post_meta_data.get('caption'):
            missing_data_analysis["missing_post_data"].append("caption")
        if not post_extracted_data.get('author') and not post_meta_data.get('username') and not post_meta_data.get('username_from_title'):
            missing_data_analysis["missing_post_data"].append("author")
        if not post_extracted_data.get('shortcode') and not post_meta_data.get('shortcode'):
            missing_data_analysis["missing_post_data"].append("shortcode")
        if not post_extracted_data.get('likes_count') and not post_meta_data.get('likes_count'):
            missing_data_analysis["missing_post_data"].append("likes_count")
        if not post_extracted_data.get('comments_count') and not post_meta_data.get('comments_count'):
            missing_data_analysis["missing_post_data"].append("comments_count")
        if not post_extracted_data.get('media_urls'):
            missing_data_analysis["missing_post_data"].append("media_urls")
        if not post_meta_data.get('full_name'):
            missing_data_analysis["missing_post_data"].append("full_name")
        if not post_meta_data.get('post_date'):
            missing_data_analysis["missing_post_data"].append("post_date")
        
        # Check for missing reel data
        reel_extracted_data = reel_data.get('reel_data', {})
        reel_meta_data = reel_data.get('meta_data', {})
        
        if not reel_extracted_data.get('caption') and not reel_meta_data.get('caption'):
            missing_data_analysis["missing_reel_data"].append("caption")
        if not reel_extracted_data.get('author') and not reel_meta_data.get('username') and not reel_meta_data.get('username_from_title'):
            missing_data_analysis["missing_reel_data"].append("author")
        if not reel_extracted_data.get('shortcode') and not reel_meta_data.get('shortcode'):
            missing_data_analysis["missing_reel_data"].append("shortcode")
        if not reel_extracted_data.get('likes_count') and not reel_meta_data.get('likes_count'):
            missing_data_analysis["missing_reel_data"].append("likes_count")
        if not reel_extracted_data.get('comments_count') and not reel_meta_data.get('comments_count'):
            missing_data_analysis["missing_reel_data"].append("comments_count")
        if not reel_extracted_data.get('views_count'):
            missing_data_analysis["missing_reel_data"].append("views_count")
        if not reel_extracted_data.get('video_url'):
            missing_data_analysis["missing_reel_data"].append("video_url")
        if not reel_meta_data.get('full_name'):
            missing_data_analysis["missing_reel_data"].append("full_name")
        if not reel_meta_data.get('post_date'):
            missing_data_analysis["missing_reel_data"].append("post_date")
        
        scraped_data["missing_data_analysis"] = missing_data_analysis
        
        # Save to JSON file
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(scraped_data, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"\n✅ Scraped data saved to: {filename}")
            print(f"   - File size: {len(json.dumps(scraped_data, indent=2, ensure_ascii=False, default=str)):,} characters")
            
            # Print summary of what was extracted
            print(f"\n📊 EXTRACTION SUMMARY:")
            print(f"   Profile Data: {'✅' if scraped_data['extraction_summary']['profile_extraction']['success'] else '❌'}")
            print(f"   Post Data: {'✅' if scraped_data['extraction_summary']['post_extraction']['success'] else '❌'}")
            print(f"   Reel Data: {'✅' if scraped_data['extraction_summary']['reel_extraction']['success'] else '❌'}")
            
            # Print missing data summary
            print(f"\n❌ MISSING DATA:")
            if missing_data_analysis["missing_profile_data"]:
                print(f"   Profile: {', '.join(missing_data_analysis['missing_profile_data'])}")
            if missing_data_analysis["missing_post_data"]:
                print(f"   Post: {', '.join(missing_data_analysis['missing_post_data'])}")
            if missing_data_analysis["missing_reel_data"]:
                print(f"   Reel: {', '.join(missing_data_analysis['missing_reel_data'])}")
            
        except Exception as e:
            print(f"❌ Error saving data to JSON: {e}")
            # Try to save a simplified version
            try:
                simplified_data = {
                    "error": f"Failed to save full data: {e}",
                    "profile_user_data": profile_data.get('user_data', {}),
                    "post_extracted_data": post_data.get('post_data', {}),
                    "reel_extracted_data": reel_data.get('reel_data', {})
                }
                with open(f"error_{filename}", 'w', encoding='utf-8') as f:
                    json.dump(simplified_data, f, indent=2, ensure_ascii=False, default=str)
                print(f"✅ Simplified data saved to: error_{filename}")
            except Exception as e2:
                print(f"❌ Failed to save even simplified data: {e2}")

    async def save_clean_final_output(self, profile_data: Dict[str, Any], post_data: Dict[str, Any], reel_data: Dict[str, Any], filename: str = "instagram_scraper/instagram_final_output.json") -> None:
        """Save clean, structured data to a final output JSON file"""
        
        final_output = []
        
        # Process profile data
        if profile_data and not profile_data.get('error'):
            # Extract username from user_data or fallback to URL extraction
            username = profile_data.get('user_data', {}).get('username')
            if not username and profile_data.get('url'):
                # Fallback: extract username from the original URL
                import re
                url_match = re.search(r'instagram\.com/([^/?]+)', profile_data.get('url'))
                if url_match:
                    username = url_match.group(1)
            
            profile_url = f"https://www.instagram.com/{username or 'unknown'}/"
            content_type = "profile"
            
            profile_entry = {
                "url": profile_url,
                "content_type": content_type,
                "full_name": profile_data.get('user_data', {}).get('full_name'),
                "username": profile_data.get('user_data', {}).get('username'),
                "followers_count": self._format_count(profile_data.get('user_data', {}).get('followers_count')),
                "following_count": self._format_count(profile_data.get('user_data', {}).get('following_count')),
                "biography": profile_data.get('user_data', {}).get('biography', ''),
                "bio_links": profile_data.get('user_data', {}).get('bio_links', []),
                "is_private": profile_data.get('user_data', {}).get('is_private', False),
                "is_verified": profile_data.get('user_data', {}).get('is_verified', False),
                "is_business_account": profile_data.get('user_data', {}).get('is_business_account', False),
                "is_professional_account": profile_data.get('user_data', {}).get('is_professional_account', True),
                "business_email": profile_data.get('user_data', {}).get('business_email'),
                "business_phone_number": profile_data.get('user_data', {}).get('business_phone_number'),
                "business_category_name": profile_data.get('user_data', {}).get('business_category_name')
            }
            
            # Always include business fields, even if null
            business_fields = ['business_email', 'business_phone_number', 'business_category_name']
            for field in business_fields:
                if field not in profile_entry:
                    profile_entry[field] = None
                elif profile_entry[field] == '':
                    profile_entry[field] = None
            
            # Try to extract business email from biography if not found
            if not profile_entry.get('business_email') and profile_entry.get('biography'):
                import re
                email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', profile_entry['biography'])
                if email_match:
                    profile_entry['business_email'] = email_match.group(0)
            
            # Remove None values for non-business fields
            profile_entry = {k: v for k, v in profile_entry.items() if v is not None or k in business_fields}
            final_output.append(profile_entry)
        
        # Process post data
        if post_data and not post_data.get('error'):
            # Try to get shortcode from metadata, otherwise extract from original URL
            shortcode = post_data.get('meta_data', {}).get('shortcode')
            if not shortcode and post_data.get('url'):
                import re
                url_match = re.search(r'instagram\.com/p/([^/?]+)', post_data.get('url'))
                if url_match:
                    shortcode = url_match.group(1)
            
            post_url = f"https://www.instagram.com/p/{shortcode or 'unknown'}/"
            content_type = self._determine_content_type(post_data)
            
            post_entry = {
                "url": post_url,
                "content_type": content_type,
                "likes_count": self._format_count(post_data.get('meta_data', {}).get('likes_count') or post_data.get('script_data', {}).get('likes')),
                "comments_count": self._format_count(post_data.get('meta_data', {}).get('comments_count') or post_data.get('script_data', {}).get('comments')),
                "username": (post_data.get('script_data', {}).get('username') or
                           post_data.get('meta_data', {}).get('username_from_twitter') or
                           post_data.get('meta_data', {}).get('username') or 
                           post_data.get('meta_data', {}).get('username_from_title')),
                "post_date": post_data.get('meta_data', {}).get('post_date'),
                "caption": (post_data.get('meta_data', {}).get('caption') or 
                          post_data.get('script_data', {}).get('caption'))
            }
            
            # Remove None values
            post_entry = {k: v for k, v in post_entry.items() if v is not None}
            final_output.append(post_entry)
        
        # Process reel data
        if reel_data and not reel_data.get('error'):
            # Try to get shortcode from metadata, otherwise extract from original URL
            shortcode = reel_data.get('meta_data', {}).get('shortcode')
            if not shortcode and reel_data.get('url'):
                import re
                url_match = re.search(r'instagram\.com/reel/([^/?]+)', reel_data.get('url'))
                if url_match:
                    shortcode = url_match.group(1)
            
            reel_url = f"https://www.instagram.com/reel/{shortcode or 'unknown'}/"
            content_type = "video"  # Reels are always videos
            
            reel_entry = {
                "url": reel_url,
                "content_type": content_type,
                "likes_count": self._format_count(reel_data.get('meta_data', {}).get('likes_count') or reel_data.get('script_data', {}).get('likes')),
                "comments_count": self._format_count(reel_data.get('meta_data', {}).get('comments_count') or reel_data.get('script_data', {}).get('comments')),
                "username": (reel_data.get('script_data', {}).get('username') or
                           reel_data.get('meta_data', {}).get('username_from_twitter') or
                           reel_data.get('meta_data', {}).get('username') or 
                           reel_data.get('meta_data', {}).get('username_from_title')),
                "post_date": reel_data.get('meta_data', {}).get('post_date'),
                "caption": (reel_data.get('meta_data', {}).get('caption') or 
                          reel_data.get('script_data', {}).get('caption'))
            }
            
            # Remove None values
            reel_entry = {k: v for k, v in reel_entry.items() if v is not None}
            final_output.append(reel_entry)
        
        # Save to JSON file
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(final_output, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"\n✅ Clean final output saved to: {filename}")
            print(f"   - File size: {len(json.dumps(final_output, indent=2, ensure_ascii=False, default=str)):,} characters")
            print(f"   - Total entries: {len(final_output)}")
            
            # Print summary of what was extracted
            print(f"\n📊 CLEAN OUTPUT SUMMARY:")
            for entry in final_output:
                content_type = entry.get('content_type', 'unknown')
                url = entry.get('url', 'unknown')
                if content_type == 'profile':
                    username = entry.get('username', 'unknown')
                    followers = entry.get('followers_count', 'unknown')
                    print(f"   Profile: @{username} ({followers} followers)")
                elif content_type in ['article', 'video']:
                    username = entry.get('username', 'unknown')
                    likes = entry.get('likes_count', 'unknown')
                    comments = entry.get('comments_count', 'unknown')
                    print(f"   {content_type.title()}: @{username} ({likes} likes, {comments} comments)")
            
        except Exception as e:
            print(f"❌ Error saving clean output to JSON: {e}")
    
    def _determine_content_type(self, data: Dict[str, Any]) -> str:
        """Determine content type based on data analysis"""
        # Check if it's a video based on various indicators
        if (data.get('meta_data', {}).get('content_type') == 'video' or
            data.get('script_data', {}).get('is_video') or
            data.get('script_data', {}).get('video_url') or
            'video' in data.get('page_analysis', {}).get('instagram_keywords', [])):
            return "video"
        else:
            return "article"  # Default to article for posts
    
    def _format_count(self, count) -> str:
        """Format count numbers to readable format (e.g., 16000 -> 16K)"""
        if count is None:
            return None
        
        try:
            count = int(count)
            if count >= 1000000:
                return f"{count/1000000:.1f}M".replace('.0', '')
            elif count >= 1000:
                return f"{count/1000:.1f}K".replace('.0', '')
            else:
                return str(count)
        except (ValueError, TypeError):
            return str(count) if count else None

    async def extract_and_save_clean_data_from_urls(self, urls: List[str], filename: str = "instagram_scraper/instagram_final_output.json") -> None:
        """Extract data from a list of URLs and save in clean format"""
        print(f"Extracting data from {len(urls)} URLs...")
        
        all_extracted_data = []
        
        for i, url in enumerate(urls, 1):
            print(f"\n[{i}/{len(urls)}] Processing: {url}")
            
            try:
                # Extract data from the URL
                extracted_data = await self.extract_graphql_data(url)
                
                if extracted_data.get('error'):
                    print(f"❌ Failed to extract data from {url}: {extracted_data['error']}")
                    continue
                
                # Determine content type and create clean entry
                content_type = self._determine_content_type_from_url(url, extracted_data)
                
                clean_entry = {
                    "url": url,
                    "content_type": content_type
                }
                
                # Add data based on content type
                if content_type == "profile":
                    user_data = extracted_data.get('user_data', {})
                    clean_entry.update({
                        "full_name": user_data.get('full_name'),
                        "username": user_data.get('username'),
                        "followers_count": self._format_count(user_data.get('followers_count')),
                        "following_count": self._format_count(user_data.get('following_count')),
                        "biography": user_data.get('biography', ''),
                        "bio_links": user_data.get('bio_links', []),
                        "is_private": user_data.get('is_private', False),
                        "is_verified": user_data.get('is_verified', False),
                        "is_business_account": user_data.get('is_business_account', False),
                        "is_professional_account": user_data.get('is_professional_account', True),
                        "business_email": user_data.get('business_email'),
                        "business_phone_number": user_data.get('business_phone_number'),
                        "business_category_name": user_data.get('business_category_name')
                    })
                    
                elif content_type in ["article", "video"]:
                    meta_data = extracted_data.get('meta_data', {})
                    script_data = extracted_data.get('script_data', {})
                    
                    clean_entry.update({
                        "likes_count": self._format_count(meta_data.get('likes_count') or script_data.get('likes')),
                        "comments_count": self._format_count(meta_data.get('comments_count') or script_data.get('comments')),
                        "username": (script_data.get('username') or
                                   meta_data.get('username_from_twitter') or
                                   meta_data.get('username') or 
                                   meta_data.get('username_from_title')),
                        "post_date": meta_data.get('post_date'),
                        "caption": (meta_data.get('caption') or script_data.get('caption'))
                    })
                
                # Always include business fields, even if null
                business_fields = ['business_email', 'business_phone_number', 'business_category_name']
                for field in business_fields:
                    if field not in clean_entry:
                        clean_entry[field] = None
                    elif clean_entry[field] == '':
                        clean_entry[field] = None
                
                # Try to extract business email from biography if not found
                if not clean_entry.get('business_email') and clean_entry.get('biography'):
                    import re
                    email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', clean_entry['biography'])
                    if email_match:
                        clean_entry['business_email'] = email_match.group(0)
                
                # Remove None values for non-business fields
                clean_entry = {k: v for k, v in clean_entry.items() if v is not None or k in business_fields}
                all_extracted_data.append(clean_entry)
                
                print(f"✅ Successfully extracted {content_type} data")
                
            except Exception as e:
                print(f"❌ Error processing {url}: {e}")
                continue
        
        # Save to JSON file
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(all_extracted_data, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"\n✅ Clean final output saved to: {filename}")
            print(f"   - File size: {len(json.dumps(all_extracted_data, indent=2, ensure_ascii=False, default=str)):,} characters")
            print(f"   - Total entries: {len(all_extracted_data)}")
            
            # Print summary
            print(f"\n📊 EXTRACTION SUMMARY:")
            content_types = {}
            for entry in all_extracted_data:
                content_type = entry.get('content_type', 'unknown')
                content_types[content_type] = content_types.get(content_type, 0) + 1
            
            for content_type, count in content_types.items():
                print(f"   {content_type.title()}: {count} entries")
            
        except Exception as e:
            print(f"❌ Error saving clean output to JSON: {e}")
    
    def _determine_content_type_from_url(self, url: str, data: Dict[str, Any]) -> str:
        """Determine content type from URL and data"""
        if '/reel/' in url:
            return "video"
        elif '/p/' in url:
            # Check if it's actually a video post
            if (data.get('meta_data', {}).get('content_type') == 'video' or
                data.get('script_data', {}).get('is_video') or
                data.get('script_data', {}).get('video_url')):
                return "video"
            else:
                return "article"
        else:
            return "profile"


async def test_advanced_graphql_extractor():
    """Test the advanced GraphQL extractor with anti-detection features"""
    print("=" * 80)
    print("TESTING ADVANCED GRAPHQL EXTRACTOR WITH ANTI-DETECTION")
    print("=" * 80)
    
    extractor = AdvancedGraphQLExtractor(headless=False, enable_anti_detection=True)  # Enable anti-detection
    
    try:
        await extractor.start()
        print("✓ Advanced GraphQL extractor started successfully")
        
        # Test anti-detection features
        print("\n" + "=" * 60)
        print("ANTI-DETECTION FEATURES TEST")
        print("=" * 60)
        
        stealth_report = await extractor.get_stealth_report()
        print("✓ Stealth report generated:")
        print(f"  - Fingerprint Evasion: {stealth_report.get('fingerprint_evasion', {}).get('enabled', False)}")
        print(f"  - Behavioral Mimicking: {stealth_report.get('behavioral_mimicking', {}).get('enabled', False)}")
        print(f"  - Network Obfuscation: {stealth_report.get('network_obfuscation', {}).get('enabled', False)}")
        
        # Test 1: Extract user profile data
        print("\n" + "=" * 60)
        print("TEST 1: USER PROFILE DATA EXTRACTION")
        print("=" * 60)
        
        test_username = "amitabhbachchan"  # Instagram's official account
        profile_data = await extractor.extract_user_profile_data(test_username)
        
        print(f"✓ Profile data extraction completed for @{test_username}")
        print(f"  - HTML Length: {profile_data.get('html_length', 0):,} characters")
        print(f"  - Text Length: {profile_data.get('text_length', 0):,} characters")
        print(f"  - Popup Closed: {profile_data.get('popup_closed', False)}")
        print(f"  - Network Requests: {profile_data.get('network_requests', 0)}")
        print(f"  - GraphQL Responses: {profile_data.get('graphql_responses', 0)}")
        
        # Show GraphQL data
        if profile_data.get('graphql_data'):
            print(f"  - GraphQL Data Keys: {list(profile_data['graphql_data'].keys())}")
            for url, data in profile_data['graphql_data'].items():
                print(f"    - {url}: {type(data)}")
                if isinstance(data, dict):
                    print(f"      Keys: {list(data.keys())}")
        
        # Show API data
        if profile_data.get('api_data'):
            print(f"  - API Data Keys: {list(profile_data['api_data'].keys())}")
            for url, data in profile_data['api_data'].items():
                print(f"    - {url}: {type(data)}")
                if isinstance(data, dict):
                    print(f"      Keys: {list(data.keys())}")
        
        # Show extracted user data
        if profile_data.get('user_data'):
            user_data = profile_data['user_data']
            print(f"  - Extracted User Data:")
            print(f"    - Username: {user_data.get('username')}")
            print(f"    - Full Name: {user_data.get('full_name')}")
            print(f"    - Followers: {user_data.get('followers_count')}")
            print(f"    - Following: {user_data.get('following_count')}")
            print(f"    - Posts: {user_data.get('posts_count')}")
            print(f"    - Verified: {user_data.get('is_verified')}")
            print(f"    - Private: {user_data.get('is_private')}")
            print(f"    - Biography: {user_data.get('biography', '')[:100]}...")
        else:
            print(f"  - No user data extracted (API calls may have failed)")
        
        # Show network analysis
        if profile_data.get('network_analysis'):
            analysis = profile_data['network_analysis']
            print(f"  - Total Requests: {analysis.get('total_requests', 0)}")
            print(f"  - GraphQL Requests: {analysis.get('graphql_requests', 0)}")
            print(f"  - API Requests: {analysis.get('api_requests', 0)}")
            print(f"  - Successful Responses: {analysis.get('successful_responses', 0)}")
            print(f"  - Failed Responses: {analysis.get('failed_responses', 0)}")
        
        # Test 2: Extract post data
        print("\n" + "=" * 60)
        print("TEST 2: POST DATA EXTRACTION")
        print("=" * 60)
        
        test_post_id = "DMsercXMVeZ"  # From network analysis
        post_data = await extractor.extract_post_data(test_post_id)
        
        print(f"✓ Post data extraction completed for post {test_post_id}")
        print(f"  - HTML Length: {post_data.get('html_length', 0):,} characters")
        print(f"  - Text Length: {post_data.get('text_length', 0):,} characters")
        print(f"  - Popup Closed: {post_data.get('popup_closed', False)}")
        print(f"  - Network Requests: {post_data.get('network_requests', 0)}")
        print(f"  - GraphQL Responses: {post_data.get('graphql_responses', 0)}")
        
        # Show GraphQL data
        if post_data.get('graphql_data'):
            print(f"  - GraphQL Data Keys: {list(post_data['graphql_data'].keys())}")
            for url, data in post_data['graphql_data'].items():
                print(f"    - {url}: {type(data)}")
                if isinstance(data, dict):
                    print(f"      Keys: {list(data.keys())}")
        
        # Test 3: Extract reel data
        print("\n" + "=" * 60)
        print("TEST 3: REEL DATA EXTRACTION")
        print("=" * 60)
        
        test_reel_id = "CSb6-Rap2Ip" # Example reel ID
        reel_data = await extractor.extract_reel_data(test_reel_id)
        
        print(f"✓ Reel data extraction completed for reel {test_reel_id}")
        print(f"  - HTML Length: {reel_data.get('html_length', 0):,} characters")
        print(f"  - Text Length: {reel_data.get('text_length', 0):,} characters")
        print(f"  - Popup Closed: {reel_data.get('popup_closed', False)}")
        print(f"  - Network Requests: {reel_data.get('network_requests', 0)}")
        print(f"  - GraphQL Responses: {reel_data.get('graphql_responses', 0)}")
        
        # Save all scraped data to JSON file
        print("\n" + "=" * 60)
        print("SAVING SCRAPED DATA TO JSON")
        print("=" * 60)
        
        await extractor.save_scraped_data_to_json(profile_data, post_data, reel_data, "instagram_scraper/instagram_scraped_data.json")
        
        # Save clean final output
        print("\n" + "=" * 60)
        print("SAVING CLEAN FINAL OUTPUT")
        print("=" * 60)
        
        await extractor.save_clean_final_output(profile_data, post_data, reel_data, "instagram_scraper/instagram_final_output.json")
        
        # Print summary
        print("\n" + "=" * 80)
        print("EXTRACTION SUMMARY")
        print("=" * 80)
        
        all_data = {
            'profile': profile_data,
            'post': post_data,
            'reel': reel_data
        }
        
        for data_type, data in all_data.items():
            success = data.get('success', True)  # Assume success unless error key exists
            if 'error' in data:
                success = False
            
            print(f"✓ {data_type.title()}: {'SUCCESS' if success else 'FAILED'}")
            if success:
                print(f"  - Content Length: {data.get('html_length', 0):,} chars")
                print(f"  - Popup Closed: {data.get('popup_closed', False)}")
                print(f"  - Network Requests: {data.get('network_requests', 0)}")
                print(f"  - GraphQL Responses: {data.get('graphql_responses', 0)}")
                print(f"  - Content Type: {data.get('page_analysis', {}).get('content_type', 'unknown')}")
            else:
                print(f"  - Error: {data.get('error', 'Unknown error')}")
        
        print("\nTask 2: Basic Data Extraction - ADVANCED GRAPHQL EXTRACTOR - COMPLETED")
        
    except Exception as e:
        print(f"\n❌ Task 2: Basic Data Extraction - FAILED: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        await extractor.stop()
        print("\n✓ Advanced GraphQL extractor cleanup completed")


async def example_clean_extraction():
    """Example of how to use the clean extraction functionality"""
    print("=" * 80)
    print("EXAMPLE: CLEAN DATA EXTRACTION")
    print("=" * 80)
    
    # Example URLs to extract data from
    example_urls = [
        "https://www.instagram.com/amitabhbachchan/",  # Profile
        "https://www.instagram.com/p/DMsercXMVeZ/",    # Post
        "https://www.instagram.com/reel/CSb6-Rap2Ip/"  # Reel
    ]
    
    extractor = AdvancedGraphQLExtractor(headless=True)  # Set to True for faster execution
    
    try:
        await extractor.start()
        print("✓ Extractor started successfully")
        
        # Extract and save clean data from URLs
        await extractor.extract_and_save_clean_data_from_urls(example_urls, "instagram_scraper/example_clean_output.json")
        
        print("\n✅ Example extraction completed!")
        print("Check 'example_clean_output.json' for the clean data structure.")
        
    except Exception as e:
        print(f"❌ Example extraction failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await extractor.stop()
        print("✓ Extractor cleanup completed")


if __name__ == "__main__":
    # Run the full test
    asyncio.run(test_advanced_graphql_extractor())
    
    # Uncomment the line below to run just the clean extraction example
    # asyncio.run(example_clean_extraction()) 