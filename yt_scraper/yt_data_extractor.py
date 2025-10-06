"""
Advanced YouTube Extractor - Dynamic Automated YouTube Scraping System
Uses browser automation with network request capture to extract YouTube data

This module extends the browser manager to capture network requests
and extract YouTube data from various page types (videos, shorts, channels).
"""

import asyncio
import json
import re
import time
from typing import Dict, Any, Optional, List
from bs4 import BeautifulSoup
from yt_scraper.browser_manager import YouTubeBrowserManager
import zstandard as zstd

class AdvancedYouTubeExtractor:
    """Advanced YouTube extractor with network request capture"""
    
    def __init__(self, headless: bool = True, enable_anti_detection: bool = True, is_mobile: bool = False):
        self.browser_manager = YouTubeBrowserManager(headless=headless, enable_anti_detection=enable_anti_detection, is_mobile=is_mobile)
        self.network_requests = []
        self.api_responses = {}
        
    async def start(self) -> None:
        """Initialize browser manager with network monitoring"""
        print("Starting YouTube browser manager...")
        await self.browser_manager.start()
        print("✓ YouTube browser manager started")
        
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
        """Set up network request monitoring for YouTube"""
        if not self.browser_manager.page:
            raise RuntimeError("Browser page not available")
            
        print(f"✓ Setting up network monitoring for YouTube page: {self.browser_manager.page}")
        
        # Listen for network requests using proper event handling
        self.browser_manager.page.on("request", self._on_request)
        self.browser_manager.page.on("response", self._on_response)
        
        print("✓ YouTube network monitoring setup completed")
        
    async def _on_request(self, request) -> None:
        """Handle network requests for YouTube"""
        url = request.url
        
        # Filter for YouTube API and relevant requests
        if any(keyword in url for keyword in [
            '/youtubei/v1/', '/api/', 'youtube.com/api',
            'youtube.com/youtubei', '/player', '/next',
            'youtube.com/channel', 'youtube.com/c/',
            'youtube.com/watch', 'youtube.com/shorts'
        ]):
            post_data = None
            try:
                raw_data = await request.post_data_buffer()
                if raw_data:
                    try:
                        post_data = raw_data.decode("utf-8")
                    except UnicodeDecodeError:
                        post_data = raw_data.hex()  # store as hex if binary
            except Exception:
                pass
            
            req_data = {
                'type': 'request',
                'url': url,
                'method': request.method,
                'headers': dict(request.headers),
                'post_data': post_data,
                'timestamp': time.time()
            }
            self.network_requests.append(req_data)
            
    async def _on_response(self, response) -> None:
        """Handle network responses for YouTube"""
        url = response.url
        
        # Filter for YouTube API responses
        if any(keyword in url for keyword in [
            '/youtubei/v1/', '/api/', 'youtube.com/api',
            'youtube.com/youtubei', '/player', '/next'
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
                
                # Only process successful responses (status 200)
                if response.status == 200:
                    # Try to parse JSON responses
                    if 'application/json' in content_type or 'text/javascript' in content_type or 'text/plain' in content_type:
                        try:
                            if body:
                                # Handle potential zstd compression
                                if 'zstd' in content_type or 'zstd' in response.headers.get('content-encoding', ''):
                                    
                                    dctx = zstd.ZstdDecompressor()
                                    decompressed = dctx.decompress(body)
                                    text_body = decompressed.decode('utf-8')
                                else:
                                    text_body = body.decode('utf-8')
                                
                                # Remove potential ")]}'" prefix from YouTube API responses
                                if text_body.startswith(")]}'"):
                                    text_body = text_body[4:]
                                # Remove potential "for (;;);" prefix
                                if text_body.startswith('for (;;);'):
                                    text_body = text_body[9:]
                                    
                                try:
                                    json_data = json.loads(text_body)
                                    response_data['json_data'] = json_data
                                    
                                    # Check for errors in the response
                                    if 'error' in json_data:
                                        print(f"❌ API Error: {json_data['error']}")
                                    else:
                                        print(f"✅ Successful YouTube API Response: {url}")
                                    
                                    # Store API responses
                                    self.api_responses[url] = json_data
                                        
                                except json.JSONDecodeError:
                                    response_data['text_body'] = text_body[:1000]  # Store first 1000 chars
                                    
                        except Exception as e:
                            response_data['parse_error'] = str(e)
                else:
                    print(f"❌ Failed Response: {url} - Status: {response.status}")
                
                self.network_requests.append(response_data)
                
            except Exception as e:
                print(f"Error processing YouTube response: {e}")
    
    async def extract_youtube_data(self, url: str) -> Dict[str, Any]:
        """Extract YouTube data from a specific URL"""
        print(f"Extracting YouTube data from: {url}")
        
        # Validate URL first
        if not self._is_valid_youtube_url(url):
            print(f"❌ Invalid YouTube URL: {url}")
            return {
                'url': url,
                'error': 'Invalid YouTube URL',
                'success': False,
                'page_type': 'invalid'
            }
        
        # Determine page type and skip unsupported types
        page_type = self._determine_page_type(url)
        if page_type in ['search', 'invalid']:
            print(f"❌ Unsupported page type '{page_type}' for URL: {url}")
            return {
                'url': url,
                'error': f'Unsupported page type: {page_type}',
                'success': False,
                'page_type': page_type
            }
            
        # Clear previous requests
        self.network_requests = []
        self.api_responses = {}
        
        try:
            # Navigate to the page and close popup
            popup_closed = await self.browser_manager.navigate_to_with_popup_close(url)
            print(f"✓ Navigation completed, popup closed: {popup_closed}")
            
            # Wait for page to load based on content type
            page_type = self._determine_page_type(url)
            if page_type == 'video':
                await self.browser_manager.wait_for_video_load()
            elif page_type == 'channel':
                await self.browser_manager.wait_for_channel_load()
            elif page_type == 'shorts':
                await asyncio.sleep(7)  # Shorts need a bit more time to load
            else:
                await asyncio.sleep(3)
            
            # Wait additional time for network requests to complete
            additional_wait = 10 if page_type == 'channel' else 8
            await asyncio.sleep(additional_wait)
            
            # Get page content
            html_content = await self.browser_manager.get_page_content()
            rendered_text = await self.browser_manager.get_rendered_text()

            # ========== CHECK IF CONTENT IS TRAVEL RELATED ==========
            is_travel_related = await self._is_travel_related_content(rendered_text, html_content, url)
            if not is_travel_related:
                print(f"❌ Content is not travel related, skipping URL: {url}")
                return {
                    'url': url,
                    'error': 'Content is not travel related',
                    'success': False,
                    'page_type': page_type,
                    'skipped': True,
                    'reason': 'not_travel_related'
                }
            
            print(f"✓ Content is travel related, proceeding with extraction")

            # Extract data from different sources
            extracted_data = {
                'url': url,
                'page_type': page_type,
                'popup_closed': popup_closed,
                'html_length': len(html_content),
                'text_length': len(rendered_text),
                'network_requests': len(self.network_requests),
                'api_responses': len(self.api_responses),
                'extracted_data': {},
                'meta_data': {},
                'script_data': {},
                'page_analysis': {},
                'network_analysis': {}
            }
            
            # 1. Extract data from API responses
            api_data = await self._extract_data_from_api()
            extracted_data['extracted_data'].update(api_data)
            
            # 2. Extract meta data from HTML
            meta_data = await self._extract_meta_data(html_content)
            extracted_data['meta_data'] = meta_data
            
            # 3. Extract data from scripts
            script_data = await self._extract_script_data(html_content)
            extracted_data['script_data'] = script_data
            
            # 4. Extract data from page content
            page_data = await self._extract_page_content_data(rendered_text, html_content)
            extracted_data['extracted_data'].update(page_data)
            
            # 5. Analyze page content
            page_analysis = await self._analyze_page_content(rendered_text, html_content, page_type)
            extracted_data['page_analysis'] = page_analysis
            
            # 6. Analyze network requests
            network_analysis = await self._analyze_network_requests()
            extracted_data['network_analysis'] = network_analysis
            
            # 7. Combine and clean the extracted data based on page type
            final_data = await self._process_extracted_data(extracted_data, page_type)
            extracted_data['final_data'] = final_data
            
            return extracted_data
            
        except Exception as e:
            print(f"❌ Error extracting data from {url}: {e}")
            return {
                'url': url,
                'error': str(e),
                'success': False
            }
    
    def _is_valid_youtube_url(self, url: str) -> bool:
        """Validate if URL is a proper YouTube URL"""
        if not url or not isinstance(url, str):
            return False
        
        # Check if it's a YouTube domain
        youtube_domains = ['youtube.com', 'youtu.be', 'm.youtube.com', 'www.youtube.com']
        if not any(domain in url.lower() for domain in youtube_domains):
            return False
        
        # Check if URL starts with http/https
        if not url.lower().startswith(('http://', 'https://')):
            return False
        
        return True

    def _determine_page_type(self, url: str) -> str:
        """Determine YouTube page type from URL"""
        if not self._is_valid_youtube_url(url):
            return 'invalid'
        
        url_lower = url.lower()
        
        if '/shorts/' in url_lower:
            return 'shorts'
        elif '/watch?v=' in url_lower:
            return 'video'
        elif '/@' in url_lower or '/channel/' in url_lower or '/c/' in url_lower:
            return 'channel'
        else:
            return 'unknown'
    

    async def _is_travel_related_content(self, rendered_text: str, html_content: str, url: str) -> bool:
        """Check if the content is travel related"""
        try:
            # Convert to lowercase for case-insensitive matching
            text_lower = rendered_text.lower()
            html_lower = html_content.lower()
            url_lower = url.lower()
            
            # Primary travel keywords - strong indicators
            primary_travel_keywords = [
                'travel', 'trip', 'vacation', 'holiday', 'tourism', 'tourist',
                'destination', 'journey', 'adventure', 'explore', 'visiting',
                'backpack', 'backpacking', 'nomad', 'wanderlust', 'getaway',
                'itinerary', 'sightseeing', 'excursion', 'expedition'
            ]
            
            # Secondary travel keywords - supportive indicators
            secondary_travel_keywords = [
                'hotel', 'resort', 'hostel', 'accommodation', 'booking',
                'flight', 'airline', 'airport', 'passport', 'visa',
                'beach', 'mountain', 'city break', 'road trip', 'cruise',
                'restaurant', 'food tour', 'local cuisine', 'street food',
                'museum', 'landmark', 'attraction', 'guide', 'tour', 'travel vlog',
                'budget travel', 'luxury travel', 'solo travel', 'family travel',
                'culture', 'heritage', 'historic', 'scenery', 'landscape', 'backpack'
            ]
            
            # Travel-related places and regions
            travel_places = [
                'paris', 'tokyo', 'london', 'new york', 'bali', 'thailand',
                'italy', 'spain', 'greece', 'japan', 'india', 'australia',
                'europe', 'asia', 'africa', 'south america', 'north america',
                'caribbean', 'mediterranean', 'scandinavia', 'middle east',
                'national park', 'world heritage', 'unesco'
            ]
            
            # Travel activities and experiences
            travel_activities = [
                'hiking', 'trekking', 'camping', 'safari', 'diving', 'snorkeling',
                'skiing', 'surfing', 'climbing', 'photography', 'festival',
                'pilgrimage', 'volunteering', 'study abroad', 'gap year',
                'honeymoon', 'retreat', 'spa', 'wellness', 'eco tourism'
            ]
            
            # Transportation related
            transportation_keywords = [
                'train', 'bus', 'car rental', 'uber', 'taxi', 'metro',
                'transfer', 'transportation'
            ]
            
            # Combine all keywords
            all_travel_keywords = (primary_travel_keywords + secondary_travel_keywords + 
                                 travel_places + travel_activities + transportation_keywords)
            
            # Count matches in different content areas
            title_matches = 0
            description_matches = 0
            content_matches = 0
            url_matches = 0
            
            # Extract title from HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            title = ""
            title_tag = soup.find('title')
            if title_tag:
                title = title_tag.text.lower()
            
            # Also check meta description
            description = ""
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc:
                description = meta_desc.get('content', '').lower()
            
            # Check URL for travel keywords
            for keyword in all_travel_keywords:
                if keyword in url_lower:
                    url_matches += 1
            
            # Check title for travel keywords (higher weight)
            for keyword in primary_travel_keywords:
                if keyword in title:
                    title_matches += 2  # Higher weight for primary keywords in title
                    
            for keyword in secondary_travel_keywords + travel_places + travel_activities:
                if keyword in title:
                    title_matches += 1
            
            # Check description for travel keywords
            for keyword in primary_travel_keywords:
                if keyword in description:
                    description_matches += 2
                    
            for keyword in secondary_travel_keywords + travel_places + travel_activities:
                if keyword in description:
                    description_matches += 1
            
            # Check main content for travel keywords
            for keyword in all_travel_keywords:
                # Count occurrences but cap at 3 per keyword to avoid spam
                count = min(text_lower.count(keyword), 3)
                if keyword in primary_travel_keywords:
                    content_matches += count * 2  # Higher weight for primary keywords
                else:
                    content_matches += count
            
            # Calculate total score
            total_score = url_matches + title_matches + description_matches + content_matches
            
            # Scoring thresholds
            if total_score >= 5:  # Strong travel indication
                print(f"✓ Travel content detected - Score: {total_score} (URL: {url_matches}, Title: {title_matches}, Desc: {description_matches}, Content: {content_matches})")
                return True
            elif total_score >= 3:  # Moderate travel indication
                print(f"✓ Moderate travel content detected - Score: {total_score} (URL: {url_matches}, Title: {title_matches}, Desc: {description_matches}, Content: {content_matches})")
                return True
            else:
                print(f"❌ Not travel related - Score: {total_score} (URL: {url_matches}, Title: {title_matches}, Desc: {description_matches}, Content: {content_matches})")
                return False
                
        except Exception as e:
            print(f"⚠️ Error checking travel content: {e}")
            # If there's an error, assume it might be travel related to avoid false negatives
            return True

    async def _extract_data_from_api(self) -> Dict[str, Any]:
        """Extract data from YouTube API responses"""
        extracted = {}
        
        for url, response in self.api_responses.items():
            if 'player' in url.lower():
                # Video player data
                video_details = response.get('videoDetails', {})
                if video_details:
                    extracted.update({
                        'title': video_details.get('title'),
                        'video_id': video_details.get('videoId'),
                        'channel_name': video_details.get('author'),
                        'channel_id': video_details.get('channelId'),
                        'description': video_details.get('shortDescription'),
                        'views': video_details.get('viewCount'),
                        'duration': video_details.get('lengthSeconds'),
                        'upload_date': video_details.get('publishDate'),
                        'keywords': video_details.get('keywords', []),
                        'is_live': video_details.get('isLiveContent', False)
                    })
                    
            elif 'browse' in url.lower() or 'next' in url.lower():
                # Channel or video metadata
                contents = response.get('contents', {})
                header = response.get('header', {})
                metadata = response.get('metadata', {})
                
                if header:
                    # Channel header data
                    if 'channelHeaderRenderer' in header:
                        channel_header = header['channelHeaderRenderer']
                        extracted.update({
                            'channel_name': channel_header.get('title'),
                            'channel_avatar': channel_header.get('avatar', {}).get('thumbnails', [{}])[-1].get('url'),
                            'channel_banner': channel_header.get('banner', {}).get('thumbnails', [{}])[-1].get('url')
                        })
                        
                        # Subscriber count from header
                        if 'subscriberCountText' in channel_header:
                            sub_text = channel_header['subscriberCountText'].get('simpleText', '')
                            extracted['subscribers'] = sub_text
                
                if metadata:
                    # Channel metadata
                    if 'channelMetadataRenderer' in metadata:
                        channel_meta = metadata['channelMetadataRenderer']
                        extracted.update({
                            'channel_name': channel_meta.get('title'),
                            'description': channel_meta.get('description'),
                            'channel_url': channel_meta.get('channelUrl'),
                            'channel_keywords': channel_meta.get('keywords', '').split(',') if channel_meta.get('keywords') else []
                        })
        
        return extracted
    
    async def _extract_meta_data(self, html_content: str) -> Dict[str, Any]:
        """Extract meta data from HTML content"""
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
        
        return meta_data
    
    async def _extract_script_data(self, html_content: str) -> Dict[str, Any]:
        """Extract data from script tags"""
        soup = BeautifulSoup(html_content, 'html.parser')
        script_data = {}
        
        scripts = soup.find_all('script')
        
        for i, script in enumerate(scripts):
            if script.string:
                script_content = script.string
                
                # Look for YouTube-specific data patterns
                patterns = [
                    # Video data
                    (r'"videoId"\s*:\s*"([^"]+)"', 'video_id'),
                    (r'"title"\s*:\s*"([^"]+)"', 'title'),
                    (r'"author"\s*:\s*"([^"]+)"', 'channel_name'),
                    (r'"channelId"\s*:\s*"([^"]+)"', 'channel_id'),
                    (r'"viewCount"\s*:\s*"?(\d+)"?', 'views'),
                    (r'"lengthSeconds"\s*:\s*"?(\d+)"?', 'duration'),
                    (r'"shortDescription"\s*:\s*"([^"]*)"', 'description'),
                    (r'"publishDate"\s*:\s*"([^"]+)"', 'upload_date'),
                    (r'"uploadDate"\s*:\s*"([^"]+)"', 'upload_date'),
                    
                    # Channel data
                    (r'"subscriberCountText"\s*:\s*{\s*"simpleText"\s*:\s*"([^"]+)"', 'subscribers'),
                    (r'"channelUrl"\s*:\s*"([^"]+)"', 'channel_url'),
                    
                    # Generic patterns
                    (r'"likes"\s*:\s*"?(\d+)"?', 'likes'),
                    (r'"dislikes"\s*:\s*"?(\d+)"?', 'dislikes'),
                    (r'"keywords"\s*:\s*\[([^\]]*)\]', 'keywords'),
                ]
                
                for pattern, key in patterns:
                    matches = re.findall(pattern, script_content, re.IGNORECASE)
                    if matches:
                        if key in ['views', 'duration', 'likes', 'dislikes']:
                            try:
                                script_data[key] = int(matches[0])
                            except ValueError:
                                script_data[key] = matches[0]
                        elif key == 'keywords':
                            # Parse keywords array
                            keywords_str = matches[0]
                            keywords = re.findall(r'"([^"]+)"', keywords_str)
                            script_data[key] = keywords
                        else:
                            script_data[key] = matches[0]
        
        return script_data
    
    async def _extract_page_content_data(self, rendered_text: str, html_content: str) -> Dict[str, Any]:
        """Extract data from page content using text analysis"""
        soup = BeautifulSoup(html_content, 'html.parser')
        extracted = {}
        
        # Extract from specific YouTube elements
        # Title extraction
        title_selectors = [
            'h1.ytd-video-primary-info-renderer',
            'h1.style-scope.ytd-video-primary-info-renderer',
            'h1[class*="title"]',
            '#title h1'
        ]
        
        for selector in title_selectors:
            try:
                title_element = soup.select_one(selector)
                if title_element:
                    extracted['title'] = title_element.get_text().strip()
                    break
            except:
                continue
        
        # Channel name extraction
        channel_selectors = [
            'a.yt-simple-endpoint.style-scope.yt-formatted-string',
            'ytd-channel-name a',
            '.ytd-channel-name a',
            '#owner-name a'
        ]
        
        for selector in channel_selectors:
            try:
                channel_element = soup.select_one(selector)
                if channel_element:
                    extracted['channel_name'] = channel_element.get_text().strip()
                    break
            except:
                continue
        
        # Views extraction using text patterns
        views_patterns = [
            r'([\d,]+)\s+views?',
            r'(\d+(?:\.\d+)?[KMB]?)\s+views?',
            r'views?\s*:\s*([\d,]+)',
        ]
        
        for pattern in views_patterns:
            matches = re.findall(pattern, rendered_text, re.IGNORECASE)
            if matches:
                extracted['views'] = matches[0]
                break
        
        # Subscribers extraction
        subscribers_patterns = [
            r'([\d.]+[KMB]?)\s+subscribers?',
            r'subscribers?\s*:\s*([\d.]+[KMB]?)',
            r'([\d,]+)\s+subscribers?'
        ]
        
        for pattern in subscribers_patterns:
            matches = re.findall(pattern, rendered_text, re.IGNORECASE)
            if matches:
                extracted['subscribers'] = matches[0]
                break
        
        # Upload date extraction
        date_patterns = [
            r'Published on (\w+ \d+, \d{4})',
            r'(\w+ \d+, \d{4})',
            r'(\d+ \w+ ago)',
            r'Streamed (\w+ \d+, \d{4})'
        ]
        
        for pattern in date_patterns:
            matches = re.findall(pattern, rendered_text)
            if matches:
                extracted['upload_date'] = matches[0]
                break
        
        return extracted
    
    async def _analyze_page_content(self, rendered_text: str, html_content: str, page_type: str) -> Dict[str, Any]:
        """Analyze page content for YouTube-specific data"""
        analysis = {
            'youtube_keywords': [],
            'page_type': page_type,
            'has_video_player': False,
            'has_channel_info': False,
            'has_comments': False,
            'has_recommendations': False,
            'text_summary': ''
        }
        
        # Check for YouTube keywords
        youtube_keywords = [
            'views', 'subscribers', 'likes', 'comments', 'share',
            'subscribe', 'channel', 'video', 'shorts', 'playlist',
            'youtube', 'watch', 'uploaded', 'streamed'
        ]
        
        found_keywords = []
        for keyword in youtube_keywords:
            if keyword.lower() in rendered_text.lower():
                found_keywords.append(keyword)
        
        analysis['youtube_keywords'] = found_keywords
        
        # Check for specific elements
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Video player
        if soup.find(id='movie_player') or soup.find('video'):
            analysis['has_video_player'] = True
        
        # Channel info
        if soup.find('ytd-channel-header-renderer') or soup.find(id='channel-header'):
            analysis['has_channel_info'] = True
        
        # Comments
        if 'comments' in rendered_text.lower() or soup.find('ytd-comments'):
            analysis['has_comments'] = True
        
        # Recommendations
        if soup.find('ytd-compact-video-renderer') or 'related' in rendered_text.lower():
            analysis['has_recommendations'] = True
        
        # Create text summary
        lines = rendered_text.split('\n')
        non_empty_lines = [line.strip() for line in lines if line.strip() and len(line.strip()) > 3]
        analysis['text_summary'] = ' | '.join(non_empty_lines[:10])  # First 10 lines
        
        return analysis
    
    async def _analyze_network_requests(self) -> Dict[str, Any]:
        """Analyze captured network requests"""
        analysis = {
            'total_requests': len(self.network_requests),
            'api_requests': 0,
            'successful_responses': 0,
            'failed_responses': 0,
            'request_types': {},
            'response_statuses': {}
        }
        
        for request in self.network_requests:
            url = request.get('url', '')
            
            if '/youtubei/v1/' in url or '/api/' in url:
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
    
    async def _process_extracted_data(self, extracted_data: Dict[str, Any], page_type: str) -> Dict[str, Any]:
        """Process and clean extracted data based on page type"""
        # Combine data from all sources
        api_data = extracted_data.get('extracted_data', {})
        script_data = extracted_data.get('script_data', {})
        meta_data = extracted_data.get('meta_data', {})
        
        # Create final data structure
        final_data = {
            'url': extracted_data.get('url'),
            'page_type': page_type
        }
        
        if page_type == 'shorts':
            final_data.update({
                'title': self._get_best_value(api_data.get('title'), script_data.get('title'), meta_data.get('title')),
                'channel_name': self._get_best_value(script_data.get('channel_name')),
                'upload_date': self._get_best_value(api_data.get('upload_date'), script_data.get('upload_date')),
                'views': self._format_number(self._get_best_value(api_data.get('views'), script_data.get('views'))),
                'channel_url': self._get_best_value(api_data.get('channel_url'), script_data.get('channel_url'))
            })
            
        elif page_type == 'video':
            # Get full description
            description = self._get_best_value(api_data.get('description'), script_data.get('description'))

            # Extract social media handles from description
            social_media_handles = self._extract_social_media_handles(description) if description else {}

            final_data.update({
                'title': self._get_best_value(api_data.get('title'), script_data.get('title'), meta_data.get('title')),
                'channel_name': self._get_best_value(api_data.get('channel_name'), script_data.get('channel_name')),
                'upload_date': self._get_best_value(api_data.get('upload_date'), script_data.get('upload_date')),
                'description': description,
                'social_media_handles': social_media_handles,
                'email': [handle['username'] for handle in social_media_handles.get('email', [])],
                'subscribers': self._get_best_value(api_data.get('subscribers'), script_data.get('subscribers')),
                'views': self._format_number(self._get_best_value(api_data.get('views'), script_data.get('views'))),
                'channel_url': self._get_best_value(api_data.get('channel_url'), script_data.get('channel_url'))
            })
            
        elif page_type == 'channel':
            # Get full description
            description = self._get_best_value(meta_data.get('og:description'))

            # Extract social media handles from description
            social_media_handles = self._extract_social_media_handles(description) if description else {}

            final_data.update({
                'channel_name': self._get_best_value(meta_data.get('og:title'), meta_data.get('twitter:title')),
                'description': description,
                'social_media_handles': social_media_handles,
                'email': [handle['username'] for handle in social_media_handles.get('email', [])],    
                'subscribers': self._get_best_value(api_data.get('subscribers'), script_data.get('subscribers')),
                'videos': self._get_video_count(extracted_data)
            })
        
        # Remove None values
        final_data = {k: v for k, v in final_data.items() if v is not None}
        
        return final_data
    
    def _extract_social_media_handles(self, text: str) -> Dict[str, List[str]]:
        """Extract social media handles from text"""
        if not text:
            return {}
        
        social_media_handles = {}
        
        # Updated social media patterns to capture both URL and username
        patterns = {
            'instagram': [
                r'instagram\.com/([a-zA-Z0-9_.]+)/?',
                r'ig\.com/([a-zA-Z0-9_.]+)/?',
                r'instagr\.am/([a-zA-Z0-9_.]+)/?'
            ],
            'twitter': [
                r'twitter\.com/([a-zA-Z0-9_]+)/?',
                r'x\.com/([a-zA-Z0-9_]+)/?'
            ],
            'tiktok': [
                r'tiktok\.com/@([a-zA-Z0-9_.]+)/?',
                r'tiktok\.com/([a-zA-Z0-9_.]+)/?'
            ],
            'facebook': [
                r'facebook\.com/([a-zA-Z0-9_.]+)/?',
                r'fb\.com/([a-zA-Z0-9_.]+)/?',
                r'fb\.me/([a-zA-Z0-9_.]+)/?'
            ],
            'linkedin': [
                r'linkedin\.com/in/([a-zA-Z0-9_.-]+)/?',
                r'linkedin\.com/company/([a-zA-Z0-9_.-]+)/?'
            ],
            'snapchat': [
                r'snapchat\.com/add/([a-zA-Z0-9_.]+)/?',
                r'snapchat\.com/([a-zA-Z0-9_.]+)/?'
            ],
            'discord': [
                r'discord\.gg/([a-zA-Z0-9_.]+)/?',
                r'discord\.com/invite/([a-zA-Z0-9_.]+)/?'
            ],
            'twitch': [
                r'twitch\.tv/([a-zA-Z0-9_.]+)/?'
            ],
            'email': [
                r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
            ]
        }
        
        # Remove the invalid domains filter - it was blocking legitimate handles
        
        for platform, platform_patterns in patterns.items():
            found_handles = []  # Use list to maintain order and store objects
            
            for pattern in platform_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for match in matches:
                    # Clean up the handle
                    clean_handle = match.strip()
                    
                    # For email, keep the full email
                    if platform == 'email':
                        if len(clean_handle) > 5 and '@' in clean_handle:
                            found_handles.append({
                                'username': clean_handle,
                                'url': f"mailto:{clean_handle}"
                            })
                    else:
                        # For social media, store both username and full URL
                        if len(clean_handle) > 1 and not clean_handle.isdigit():
                            # Reconstruct the full URL
                            if platform == 'instagram':
                                full_url = f"https://www.instagram.com/{clean_handle}/"
                            elif platform == 'twitter':
                                full_url = f"https://twitter.com/{clean_handle}"
                            elif platform == 'facebook':
                                full_url = f"https://www.facebook.com/{clean_handle}"
                            elif platform == 'tiktok':
                                clean_handle = clean_handle.lstrip('@')  # Remove @ if present
                                full_url = f"https://www.tiktok.com/@{clean_handle}"
                            elif platform == 'twitch':
                                full_url = f"https://www.twitch.tv/{clean_handle}"
                            elif platform == 'linkedin':
                                full_url = f"https://www.linkedin.com/in/{clean_handle}"
                            elif platform == 'snapchat':
                                full_url = f"https://www.snapchat.com/add/{clean_handle}"
                            elif platform == 'discord':
                                full_url = f"https://discord.gg/{clean_handle}"
                            else:
                                full_url = clean_handle
                            
                            # Check for duplicates
                            if not any(h['username'].lower() == clean_handle.lower() for h in found_handles):
                                found_handles.append({
                                    'username': clean_handle,
                                    'url': full_url
                                })
            
            if found_handles:
                social_media_handles[platform] = found_handles
        
        return social_media_handles
        
    def _get_best_value(self, *values):
        """Get the best non-None, non-empty value from multiple sources"""
        for value in values:
            if value and str(value).strip():
                return str(value).strip()
        return None
    
    def _format_number(self, value):
        """Format number values"""
        if not value:
            return None
        
        # If it's already formatted (contains K, M, B), return as-is
        if isinstance(value, str) and re.search(r'\d+[KMB]', value):
            return value
        
        # Try to convert to int and format
        try:
            num = int(str(value).replace(',', ''))
            if num >= 1000000000:
                return f"{num/1000000000:.1f}B"
            elif num >= 1000000:
                return f"{num/1000000:.1f}M"
            elif num >= 1000:
                return f"{num/1000:.1f}K"
            else:
                return str(num)
        except (ValueError, TypeError):
            return str(value)
    
    def _get_video_count(self, extracted_data: Dict[str, Any]) -> Optional[str]:
        """Extract video count for channel pages"""
        # This would need more sophisticated extraction
        # from the channel's video list or API responses
        return None
    
    async def extract_video_data(self, video_url: str) -> Dict[str, Any]:
        """Extract video data"""
        return await self.extract_youtube_data(video_url)
    
    async def extract_shorts_data(self, shorts_url: str) -> Dict[str, Any]:
        """Extract shorts data"""
        return await self.extract_youtube_data(shorts_url)
    
    async def extract_channel_data(self, channel_url: str) -> Dict[str, Any]:
        """Extract channel data"""
        return await self.extract_youtube_data(channel_url)
    
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

    async def save_scraped_data_to_json(self, all_extracted_data: List[Dict[str, Any]], filename: str = "youtube_scraped_data.json") -> None:
        """Save all scraped data to a structured JSON file"""
        
        # Create structured data object
        scraped_data = {
            "metadata": {
                "scraping_timestamp": time.time(),
                "scraping_date": time.strftime("%Y-%m-%d %H:%M:%S"),
                "extractor_version": "youtube_advanced_extractor_v1.0",
                "total_urls_processed": len(all_extracted_data)
            },
            "extraction_summary": {
                "successful_extractions": 0,
                "failed_extractions": 0,
                "videos": 0,
                "shorts": 0,
                "channels": 0
            },
            "extracted_data": all_extracted_data
        }
        
        # Calculate summary statistics
        for data in all_extracted_data:
            if data.get('error'):
                scraped_data["extraction_summary"]["failed_extractions"] += 1
            else:
                scraped_data["extraction_summary"]["successful_extractions"] += 1
                page_type = data.get('page_type', 'unknown')
                if page_type == 'video':
                    scraped_data["extraction_summary"]["videos"] += 1
                elif page_type == 'shorts':
                    scraped_data["extraction_summary"]["shorts"] += 1
                elif page_type == 'channel':
                    scraped_data["extraction_summary"]["channels"] += 1
        
        # Save to JSON file
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(scraped_data, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"\n✅ Scraped data saved to: {filename}")
            print(f"   - File size: {len(json.dumps(scraped_data, indent=2, ensure_ascii=False, default=str)):,} characters")
            
            # Print summary
            print(f"\n📊 EXTRACTION SUMMARY:")
            print(f"   Total URLs: {scraped_data['metadata']['total_urls_processed']}")
            print(f"   Successful: {scraped_data['extraction_summary']['successful_extractions']}")
            print(f"   Failed: {scraped_data['extraction_summary']['failed_extractions']}")
            print(f"   Videos: {scraped_data['extraction_summary']['videos']}")
            print(f"   Shorts: {scraped_data['extraction_summary']['shorts']}")
            print(f"   Channels: {scraped_data['extraction_summary']['channels']}")
        except Exception as e:
            print(f"❌ Error saving scraped data to JSON: {e}")
            # Try to save a simplified version
            try:
                simplified_data = {
                    "error": f"Failed to save full data: {e}",
                    "extracted_data": all_extracted_data
                }
                with open(f"error_{filename}", 'w', encoding='utf-8') as f:
                    json.dump(simplified_data, f, indent=2, ensure_ascii=False, default=str)
                print(f"✅ Simplified data saved to: error_{filename}")
            except Exception as e2:
                print(f"❌ Failed to save even simplified data: {e2}")

    async def save_clean_final_output(self, all_extracted_data: List[Dict[str, Any]], filename: str = "youtube_final_output.json") -> List[Dict[str, Any]]:
        """Save clean, structured data to a final output JSON file"""
        
        final_output = []
        
        for data in all_extracted_data:
            if data.get('error'):
                continue
                
            page_type = data.get('page_type', 'unknown')
            final_data = data.get('final_data', {})
            
            if page_type == 'video':
                video_entry = {
                    "url": final_data.get('url'),
                    "content_type": "video",
                    "title": final_data.get('title'),
                    "channel_name": final_data.get('channel_name'),
                    "upload_date": final_data.get('upload_date'),
                    "views": final_data.get('views'),
                    "subscribers": final_data.get('subscribers'),
                    "social_media_handles": final_data.get('social_media_handles', {}),
                    "email": [handle['username'] for handle in final_data.get('social_media_handles', {}).get('email', [])], 
                    "description": final_data.get('description', '') if final_data.get('description') else None  # Limit description
                }
                # Remove None values
                video_entry = {k: v for k, v in video_entry.items() if v is not None}
                final_output.append(video_entry)
                
            elif page_type == 'shorts':
                shorts_entry = {
                    "url": final_data.get('url'),
                    "content_type": "shorts",
                    "title": final_data.get('title'),
                    "channel_name": final_data.get('channel_name'),
                    "upload_date": final_data.get('upload_date'),
                    "views": final_data.get('views')
                }
                # Remove None values
                shorts_entry = {k: v for k, v in shorts_entry.items() if v is not None}
                final_output.append(shorts_entry)
                
            elif page_type == 'channel':
                channel_entry = {
                    "url": final_data.get('url'),
                    "content_type": "channel",
                    "channel_name": final_data.get('channel_name'),
                    "subscribers": final_data.get('subscribers'),
                    "description": final_data.get('description', '') if final_data.get('description') else None,
                    "social_media_handles": final_data.get('social_media_handles', {}),
                    "email": [handle['username'] for handle in final_data.get('social_media_handles', {}).get('email', [])], 
                    "videos": final_data.get('videos')
                }
                # Remove None values
                channel_entry = {k: v for k, v in channel_entry.items() if v is not None}
                final_output.append(channel_entry)
        
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
                if content_type == 'channel':
                    channel_name = entry.get('channel_name', 'unknown')
                    subscribers = entry.get('subscribers', 'unknown')
                    print(f"   Channel: {channel_name} ({subscribers} subscribers)")
                elif content_type in ['video', 'shorts']:
                    title = entry.get('title', 'unknown')
                    channel_name = entry.get('channel_name', 'unknown')
                    views = entry.get('views', 'unknown')
                    print(f"   {content_type.title()}: {title[:50]}... by {channel_name} ({views} views)")
            
        except Exception as e:
            print(f"❌ Error saving clean output to JSON: {e}")

        return final_output

    async def extract_and_save_clean_data_from_urls(self, urls: List[str], filename: str = "youtube_final_output.json") -> None:
        """Extract data from a list of URLs and save in clean format"""
        print(f"Extracting YouTube data from {len(urls)} URLs...")
        
        # Filter valid URLs first
        valid_urls = []
        for url in urls:
            if self._is_valid_youtube_url(url):
                page_type = self._determine_page_type(url)
                if page_type not in ['search', 'invalid', 'unknown']:
                    valid_urls.append(url)
                else:
                    print(f"⚠️  Skipping unsupported URL ({page_type}): {url}")
            else:
                print(f"⚠️  Skipping invalid URL: {url}")
        
        print(f"Processing {len(valid_urls)} valid URLs out of {len(urls)} total URLs")
        
        all_extracted_data = []
        
        for i, url in enumerate(valid_urls, 1):
            print(f"\n[{i}/{len(valid_urls)}] Processing: {url}")
            
            try:
                # Extract data from the URL
                extracted_data = await self.extract_youtube_data(url)
                
                if extracted_data.get('error'):
                    print(f"❌ Failed to extract data from {url}: {extracted_data['error']}")
                    continue
                
                all_extracted_data.append(extracted_data)
                
                page_type = extracted_data.get('page_type', 'unknown')
                print(f"✅ Successfully extracted {page_type} data")
                
                # Small delay between requests to be respectful
                await asyncio.sleep(2)
                
            except Exception as e:
                print(f"❌ Error processing {url}: {e}")
                continue
        
        # Save clean final output
        await self.save_clean_final_output(all_extracted_data, filename)


async def test_advanced_youtube_extractor():
    """Test the advanced YouTube extractor with anti-detection features"""
    print("=" * 80)
    print("TESTING ADVANCED YOUTUBE EXTRACTOR WITH ANTI-DETECTION")
    print("=" * 80)
    
    extractor = AdvancedYouTubeExtractor(headless=False, enable_anti_detection=True)  # Enable anti-detection
    
    try:
        await extractor.start()
        print("✓ Advanced YouTube extractor started successfully")
        
        # Test anti-detection features
        print("\n" + "=" * 60)
        print("ANTI-DETECTION FEATURES TEST")
        print("=" * 60)
        
        stealth_report = await extractor.get_stealth_report()
        print("✓ Stealth report generated:")
        print(f"  - Fingerprint Evasion: {stealth_report.get('fingerprint_evasion', {}).get('enabled', False)}")
        print(f"  - Behavioral Mimicking: {stealth_report.get('behavioral_mimicking', {}).get('enabled', False)}")
        print(f"  - Network Obfuscation: {stealth_report.get('network_obfuscation', {}).get('enabled', False)}")
        
        # Test 1: Extract video data
        print("\n" + "=" * 60)
        print("TEST 1: VIDEO DATA EXTRACTION")
        print("=" * 60)
        
        test_video_url = "https://www.youtube.com/watch?v=Jbv1mIjWRpc"  # Rick Roll video
        video_data = await extractor.extract_video_data(test_video_url)
        
        print(f"✓ Video data extraction completed")
        print(f"  - HTML Length: {video_data.get('html_length', 0):,} characters")
        print(f"  - Text Length: {video_data.get('text_length', 0):,} characters")
        print(f"  - Page Type: {video_data.get('page_type', 'unknown')}")
        print(f"  - Network Requests: {video_data.get('network_requests', 0)}")
        print(f"  - API Responses: {video_data.get('api_responses', 0)}")
        
        # Show extracted video data
        if video_data.get('final_data'):
            final_data = video_data['final_data']
            print(f"  - Extracted Video Data:")
            print(f"    - Title: {final_data.get('title', 'N/A')}")
            print(f"    - Channel: {final_data.get('channel_name', 'N/A')}")
            print(f"    - Views: {final_data.get('views', 'N/A')}")
            print(f"    - Upload Date: {final_data.get('upload_date', 'N/A')}")
            print(f"    - Subscribers: {final_data.get('subscribers', 'N/A')}")
        
        # Test 2: Extract shorts data
        print("\n" + "=" * 60)
        print("TEST 2: SHORTS DATA EXTRACTION")
        print("=" * 60)
        
        test_shorts_url = "https://www.youtube.com/shorts/YIe4jPsvv5g"  # Example shorts URL
        shorts_data = await extractor.extract_shorts_data(test_shorts_url)
        
        print(f"✓ Shorts data extraction completed")
        print(f"  - HTML Length: {shorts_data.get('html_length', 0):,} characters")
        print(f"  - Page Type: {shorts_data.get('page_type', 'unknown')}")
        print(f"  - Network Requests: {shorts_data.get('network_requests', 0)}")
        
        # Test 3: Extract channel data
        print("\n" + "=" * 60)
        print("TEST 3: CHANNEL DATA EXTRACTION")
        print("=" * 60)
        
        test_channel_url = "https://www.youtube.com/@stillwatchingnetflix"  # YouTube's official channel
        channel_data = await extractor.extract_channel_data(test_channel_url)
        
        print(f"✓ Channel data extraction completed")
        print(f"  - HTML Length: {channel_data.get('html_length', 0):,} characters")
        print(f"  - Page Type: {channel_data.get('page_type', 'unknown')}")
        print(f"  - Network Requests: {channel_data.get('network_requests', 0)}")
        
        # Show extracted channel data
        if channel_data.get('final_data'):
            final_data = channel_data['final_data']
            print(f"  - Extracted Channel Data:")
            print(f"    - Channel Name: {final_data.get('channel_name', 'N/A')}")
            print(f"    - Subscribers: {final_data.get('subscribers', 'N/A')}")
            print(f"    - Description: {final_data.get('description', 'N/A')[:100]}...")
        
        # Save all scraped data to JSON file
        print("\n" + "=" * 60)
        print("SAVING SCRAPED DATA TO JSON")
        print("=" * 60)
        
        all_data = [video_data, shorts_data, channel_data]
        await extractor.save_scraped_data_to_json(all_data, "youtube_scraped_data.json")
        
        # Save clean final output
        print("\n" + "=" * 60)
        print("SAVING CLEAN FINAL OUTPUT")
        print("=" * 60)
        
        await extractor.save_clean_final_output(all_data, "youtube_final_output.json")
        
        # Print summary
        print("\n" + "=" * 80)
        print("EXTRACTION SUMMARY")
        print("=" * 80)
        
        for i, (data_type, data) in enumerate([('video', video_data), ('shorts', shorts_data), ('channel', channel_data)], 1):
            success = not data.get('error')
            
            print(f"✓ Test {i} - {data_type.title()}: {'SUCCESS' if success else 'FAILED'}")
            if success:
                print(f"  - Content Length: {data.get('html_length', 0):,} chars")
                print(f"  - Network Requests: {data.get('network_requests', 0)}")
                print(f"  - API Responses: {data.get('api_responses', 0)}")
                print(f"  - Page Type: {data.get('page_type', 'unknown')}")
            else:
                print(f"  - Error: {data.get('error', 'Unknown error')}")
        
        print("\nYouTube Advanced Extractor Testing - COMPLETED")
        
    except Exception as e:
        print(f"\n❌ YouTube Advanced Extractor Testing - FAILED: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        await extractor.stop()
        print("\n✓ YouTube Advanced extractor cleanup completed")


async def example_clean_youtube_extraction():
    """Example of how to use the clean extraction functionality for YouTube"""
    print("=" * 80)
    print("EXAMPLE: CLEAN YOUTUBE DATA EXTRACTION")
    print("=" * 80)
    
    # Example YouTube URLs to extract data from
    example_urls = [
        "https://www.youtube.com/watch?v=Jbv1mIjWRpc",  # Famous video
        "https://www.youtube.com/shorts/YIe4jPsvv5g",        # Example shorts
        "https://www.youtube.com/@stillwatchingnetflix"              # YouTube channel
    ]
    
    extractor = AdvancedYouTubeExtractor(headless=True)  # Set to True for faster execution
    
    try:
        await extractor.start()
        print("✓ YouTube extractor started successfully")
        
        # Extract and save clean data from URLs
        await extractor.extract_and_save_clean_data_from_urls(example_urls, "example_youtube_clean_output.json")
        
        print("\n✅ Example YouTube extraction completed!")
        print("Check 'example_youtube_clean_output.json' for the clean data structure.")
        
    except Exception as e:
        print(f"❌ Example YouTube extraction failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await extractor.stop()
        print("✓ YouTube extractor cleanup completed")


if __name__ == "__main__":
    # Run the full test
    asyncio.run(test_advanced_youtube_extractor())
    
    # Uncomment the line below to run just the clean extraction example
    # asyncio.run(example_clean_youtube_extraction())