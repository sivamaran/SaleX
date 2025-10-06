"""
Comprehensive JSON-LD filtration module for extracting client-relevant information
Works with any JSON-LD structure from any website
"""

import json
from typing import Any, Dict, List, Optional, Set, Union
import re
import logging


class JSONLDFilter:
    """
    A comprehensive filter for JSON-LD data that removes irrelevant information
    and preserves client-relevant data for AI processing.
    """
    
    def __init__(self):
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Define relevant fields that are likely to contain client information
        self.relevant_fields = {
            # Content fields
            'headline', 'name', 'title', 'description', 'articleBody', 'text', 'content',
            'mainEntityOfPage', 'url', 'sameAs', 'keywords', 'articleSection', 'about',
            
            # Organization/Business fields
            'publisher', 'author', 'organization', 'brand', 'manufacturer', 'provider',
            'legalName', 'foundingDate', 'location', 'address', 'contactPoint',
            'telephone', 'email', 'areaServed', 'serviceType', 'priceRange',
            
            # Person fields
            'jobTitle', 'worksFor', 'affiliation', 'alumniOf', 'memberOf',
            'knowsAbout', 'hasOccupation', 'nationality', 'birthPlace',
            
            # Product/Service fields
            'category', 'brand', 'model', 'sku', 'gtin', 'price', 'priceCurrency',
            'availability', 'condition', 'review', 'rating', 'offers',
            
            # Event fields
            'startDate', 'endDate', 'location', 'organizer', 'performer', 'audience',
            
            # Article/CreativeWork fields
            'genre','inLanguage', 'copyrightHolder', 'license', 'isAccessibleForFree',
            
            # List/Collection fields
            'itemListElement', 'numberOfItems', 'itemListOrder',
            
            # General relationship fields
            'parentOrganization', 'subOrganization', 'member', 'hasPart', 'isPartOf',
            'relatedTo', 'mentions', 'citation', 'isBasedOn'
        }

        # Fields to always remove (typically technical/display related)
        self.irrelevant_fields = {
            'logo', 'image', 'thumbnail', 'thumbnailUrl', 'photo', 'picture',
            'identifier', 'id', '@id', 'uuid', 'guid',
            'width', 'height', 'coordinates', 'encodingFormat', 'contentSize',
            'uploadDate', 'embedUrl', 'playerType', 'requiresSubscription',
            'sha256', 'contentUrl', 'embedCode', 'duration', 'bitrate',
            'videoFrameSize', 'videoQuality', 'aspectRatio', 'caption',
            
            # Schema.org technical fields
            'potentialAction', 'mainEntity', 'hasPart', 'workExample',
            'exampleOfWork', 'workTranslation', 'translationOfWork',
            'datePublished', 'dateModified', 'dateCreated', 'wordCount', 
            # SEO/Technical fields that don't contain client info
            'breadcrumb', 'mainNavigationElement', 'primaryImageOfPage',
            'significantLink', 'relatedLink', 'lastReviewed', 'reviewedBy',
            
            # Policy/Legal pages (usually not client-relevant)
            'masthead', 'correctionsPolicy', 'ethicsPolicy', 'publishingPrinciples',
            'missionCoveragePrioritiesPolicy', 'ownershipFundingInfo',
            'verificationFactCheckingPolicy', 'privacyPolicy', 'termsOfService'
        }

        # Context fields that add noise but aren't inherently bad
        self.context_fields = {'@context', '@type', '@graph'}

    def parse_input(self, data: Union[str, Dict, List, Any]) -> Union[Dict, List, Any]:
        """
        Parse input data to ensure it's valid JSON format
        
        Args:
            data: Input data (string, dict, list, or other)
            
        Returns:
            Parsed JSON data
            
        Raises:
            ValueError: If data cannot be parsed as JSON
        """
        # If already a dict or list, return as-is
        if isinstance(data, (dict, list)):
            return data
        
        # If not a string, try to convert to JSON string first
        if not isinstance(data, str):
            try:
                json_string = json.dumps(data)
                return json.loads(json_string)
            except (TypeError, ValueError) as e:
                raise ValueError(f"Cannot convert input to JSON: {e}")
        
        # Clean and parse string data
        cleaned_data = self._clean_json_string(data)
        
        try:
            # Try to parse as single JSON object
            return json.loads(cleaned_data)
        except json.JSONDecodeError:
            # Try to handle multiple JSON objects
            return self._parse_multiple_json_objects(cleaned_data)

    def _clean_json_string(self, json_string: str) -> str:
        """Clean JSON string to handle common formatting issues"""
        # Remove leading/trailing whitespace
        cleaned = json_string.strip()
        
        # First handle HTML entities that might break JSON parsing
        html_entities = {
            '&amp;': '&',
            '&lt;': '<',
            '&gt;': '>',
            '&quot;': '"',
            '&#39;': "'",
            '&nbsp;': ' ',
            '&apos;': "'"
        }
        
        for entity, replacement in html_entities.items():
            cleaned = cleaned.replace(entity, replacement)
        
        # Fix common JSON issues
        # Fix unescaped quotes inside strings (common issue)
        cleaned = self._fix_unescaped_quotes(cleaned)
        
        # Fix trailing commas
        cleaned = re.sub(r',(\s*[}\]])', r'\1', cleaned)
        
        # Fix single quotes to double quotes (if not already escaped)
        cleaned = re.sub(r"(?<!\\)'([^']*)'(?=\s*[,:\]}])", r'"\1"', cleaned)
        
        return cleaned

    def _fix_unescaped_quotes(self, json_str: str) -> str:
        """Fix unescaped quotes within JSON string values"""
        try:
            # Try to identify string values and escape quotes within them
            import re
            
            # Pattern to match string values in JSON
            # This handles: "key": "value with "quotes" inside"
            def escape_quotes_in_match(match):
                full_match = match.group(0)
                key_part = match.group(1)  # "key":
                quote_start = match.group(2)  # opening quote
                value_part = match.group(3)  # value content
                quote_end = match.group(4)  # closing quote or end
                
                # Escape any unescaped quotes in the value part
                if value_part:
                    # Replace unescaped quotes with escaped quotes
                    escaped_value = value_part.replace('\\"', '__TEMP_ESCAPED__')  # preserve already escaped
                    escaped_value = escaped_value.replace('"', '\\"')  # escape unescaped quotes
                    escaped_value = escaped_value.replace('__TEMP_ESCAPED__', '\\"')  # restore escaped quotes
                    return f'{key_part}{quote_start}{escaped_value}{quote_end}'
                
                return full_match
            
            # Pattern to match JSON key-value pairs with potential unescaped quotes
            pattern = r'("[\w\s]+"\s*:\s*)(")([^"]*?)("(?=\s*[,}])|$)'
            cleaned = re.sub(pattern, escape_quotes_in_match, json_str)
            
            return cleaned
            
        except Exception:
            # If regex fails, return original string
            return json_str

    def _parse_multiple_json_objects(self, data: str) -> Union[Dict, List]:
        """
        Parse string containing multiple JSON objects
        
        Args:
            data: String potentially containing multiple JSON objects
            
        Returns:
            Single JSON object containing all parsed objects
        """
        # Try to find multiple JSON objects using regex
        json_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
        potential_jsons = re.findall(json_pattern, data, re.DOTALL)
        
        if not potential_jsons:
            # Try to find JSON arrays
            array_pattern = r'\[[^\[\]]*(?:\[[^\[\]]*\][^\[\]]*)*\]'
            potential_jsons = re.findall(array_pattern, data, re.DOTALL)
        
        parsed_objects = []
        
        for json_str in potential_jsons:
            try:
                cleaned_json = self._clean_json_string(json_str.strip())
                parsed_obj = json.loads(cleaned_json)
                parsed_objects.append(parsed_obj)
            except json.JSONDecodeError as e:
                self.logger.warning(f"Failed to parse potential JSON: {json_str[:100]}... Error: {e}")
                continue
        
        if not parsed_objects:
            # Last resort: try to fix the entire string and parse
            try:
                # Try to fix common issues and parse
                fixed_data = self._attempt_json_repair(data)
                return json.loads(fixed_data)
            except json.JSONDecodeError:
                raise ValueError(f"Unable to parse as JSON after all attempts: {data[:200]}...")
        
        # If we found multiple objects, combine them
        if len(parsed_objects) == 1:
            return parsed_objects[0]
        else:
            # Combine multiple objects into a single structure
            return self._combine_json_objects(parsed_objects)

    def _attempt_json_repair(self, data: str) -> str:
        """Attempt to repair malformed JSON string"""
        cleaned = data.strip()
        
        # If it doesn't start and end with braces or brackets, try to wrap it
        if not ((cleaned.startswith('{') and cleaned.endswith('}')) or 
                (cleaned.startswith('[') and cleaned.endswith(']'))):
            # Try wrapping in braces first
            cleaned = f'{{{cleaned}}}'
        
        # Additional cleaning attempts
        cleaned = self._clean_json_string(cleaned)
        
        # Try to validate and return
        try:
            json.loads(cleaned)  # Test parse
            return cleaned
        except json.JSONDecodeError:
            # If still fails, try array wrapping
            if not cleaned.startswith('['):
                array_wrapped = f'[{data.strip()}]'
                array_cleaned = self._clean_json_string(array_wrapped)
                try:
                    json.loads(array_cleaned)  # Test parse
                    return array_cleaned
                except json.JSONDecodeError:
                    pass
            
            # Return the best attempt we have
            return cleaned

    def _combine_json_objects(self, objects: List[Union[Dict, List, Any]]) -> Dict:
        """
        Combine multiple JSON objects into a single structured object
        
        Args:
            objects: List of JSON objects to combine
            
        Returns:
            Combined JSON object
        """
        if not objects:
            return {}
        
        # If all objects are dictionaries, try to merge them intelligently
        if all(isinstance(obj, dict) for obj in objects):
            combined = {}
            for i, obj in enumerate(objects):
                if '@type' in obj and '@context' in obj:
                    # This looks like a main JSON-LD object
                    combined.update(obj)
                else:
                    # Treat as additional data
                    combined[f'data_{i}'] = obj
            return combined
        else:
            # Mixed types, return as array wrapper
            return {'combined_data': objects}

    def filter(self, data: Union[str, Dict, List, Any], **options) -> Union[Dict, List, Any, None]:
        """
        Main filtration method with automatic JSON parsing
        
        Args:
            data: JSON-LD data to filter (string, dict, list, or other format)
            **options: Filtration options
                - remove_context (bool): Remove @context fields (default: True)
                - preserve_types (bool): Keep @type fields (default: False)
                - max_depth (int): Maximum recursion depth (default: 10)
                - min_string_length (int): Minimum string length to keep (default: 2)
                - remove_empty_objects (bool): Remove empty objects (default: True)
                - custom_relevant_fields (list): Additional fields to keep
                - custom_irrelevant_fields (list): Additional fields to remove
        
        Returns:
            Filtered data structure
            
        Raises:
            ValueError: If input cannot be parsed as JSON
        """
        # First, ensure data is in JSON format
        try:
            parsed_data = self.parse_input(data)
        except ValueError as e:
            self.logger.error(f"Failed to parse input data: {e}")
            raise e
        
        config = {
            'remove_context': True,
            'preserve_types': False,
            'max_depth': 10,
            'min_string_length': 2,
            'remove_empty_objects': True,
            'custom_relevant_fields': [],
            'custom_irrelevant_fields': [],
            **options
        }

        # Add custom fields to sets
        if config['custom_relevant_fields']:
            self.relevant_fields.update(config['custom_relevant_fields'])
        if config['custom_irrelevant_fields']:
            self.irrelevant_fields.update(config['custom_irrelevant_fields'])

        return self._filter_recursive(parsed_data, config, 0)

    def _filter_recursive(self, obj: Any, config: Dict, depth: int) -> Any:
        """Recursive filtering function"""
        # Prevent infinite recursion
        if depth > config['max_depth']:
            return None

        if isinstance(obj, list):
            filtered = []
            for item in obj:
                filtered_item = self._filter_recursive(item, config, depth + 1)
                if filtered_item is not None:
                    filtered.append(filtered_item)
            return filtered if filtered else None

        if obj is None or not isinstance(obj, dict):
            # Filter out very short strings that are likely IDs or technical data
            if isinstance(obj, str) and len(obj) < config['min_string_length']:
                return None
            return obj

        filtered = {}
        has_content = False

        for key, value in obj.items():
            # Skip if field is explicitly irrelevant
            if key in self.irrelevant_fields:
                continue

            # Handle context fields based on config
            if key in self.context_fields:
                if not config['remove_context'] or (key == '@type' and config['preserve_types']):
                    filtered[key] = value
                    has_content = True
                continue

            # Check if field is relevant or if we should include unknown fields
            is_relevant = key in self.relevant_fields or self._is_likely_relevant(key, value)
            
            if is_relevant:
                filtered_value = self._filter_recursive(value, config, depth + 1)
                
                if filtered_value is not None:
                    filtered[key] = filtered_value
                    has_content = True

        # Return None if object is empty and we're removing empty objects
        if config['remove_empty_objects'] and not has_content:
            return None

        return filtered if has_content else None

    def _is_likely_relevant(self, key: str, value: Any) -> bool:
        """Heuristic to determine if an unknown field might be relevant"""
        lower_key = key.lower()

        # Skip technical-looking fields
        if 'id' in lower_key or ('url' in lower_key and 'thumb' in lower_key):
            return False

        # Include fields that seem to contain meaningful content
        if isinstance(value, str) and len(value) > 10:
            return True

        # Include objects that might contain nested relevant data
        if isinstance(value, dict):
            return True

        # Include arrays
        if isinstance(value, list):
            return True

        # Include numeric values that might be meaningful (not dimensions)
        if isinstance(value, (int, float)) and 'width' not in lower_key and 'height' not in lower_key:
            return True

        # Include boolean values for feature flags
        if isinstance(value, bool):
            return True

        return False

    def extract_client_info(self, data: Union[str, Dict, List, Any], **options) -> Dict:
        """
        Extract specific client information with enhanced structure and automatic JSON parsing
        
        Args:
            data: JSON-LD data to process (string, dict, list, or other format)
            **options: Same as filter() method
            
        Returns:
            Structured dictionary with categorized client information
            
        Raises:
            ValueError: If input cannot be parsed as JSON
        """
        filtered = self.filter(data, **options)
        return self._extract_structured_info(filtered)

    def _extract_structured_info(self, data: Any) -> Dict:
        """Extract and structure client-relevant information"""
        result = {
            'content': {},
            'organization': {},
            'person': {},
            'contact': {},
            'business': {},
            'metadata': {}
        }

        self._extract_from_object(data, result)
        
        # Remove empty categories
        result = {key: value for key, value in result.items() if value}
        
        return result

    def _extract_from_object(self, obj: Any, result: Dict) -> None:
        """Categorize extracted information"""
        if not obj or not isinstance(obj, dict):
            if isinstance(obj, list):
                for item in obj:
                    self._extract_from_object(item, result)
            return

        for key, value in obj.items():
            category = self._categorize_field(key)
            
            if category and isinstance(value, str) and value.strip():
                result[category][key] = value
            elif category and isinstance(value, (int, float, bool)):
                result[category][key] = value
            elif isinstance(value, (dict, list)):
                self._extract_from_object(value, result)

    def _categorize_field(self, key: str) -> Optional[str]:
        """Categorize fields into client-relevant categories"""
        content_fields = {'headline', 'name', 'title', 'description', 'articleBody', 'keywords', 'articleSection'}
        org_fields = {'publisher', 'legalName', 'foundingDate', 'organization', 'brand'}
        person_fields = {'author', 'jobTitle', 'worksFor', 'affiliation'}
        contact_fields = {'telephone', 'email', 'address', 'location', 'url', 'sameAs'}
        business_fields = {'serviceType', 'priceRange', 'areaServed', 'category'}
        meta_fields = {'datePublished', 'dateModified', 'wordCount', '@type'}

        if key in content_fields:
            return 'content'
        elif key in org_fields:
            return 'organization'
        elif key in person_fields:
            return 'person'
        elif key in contact_fields:
            return 'contact'
        elif key in business_fields:
            return 'business'
        elif key in meta_fields:
            return 'metadata'
        else:
            return 'content'  # Default category for unknown but potentially relevant fields

def split_json_objects(text: str):
    """
    Splits a string containing multiple JSON objects into a list of dicts.
    Works by balancing braces.
    """
    objects = []
    brace_count = 0
    start_idx = None

    for i, char in enumerate(text):
        if char == '{':
            if brace_count == 0:
                start_idx = i
            brace_count += 1
        elif char == '}':
            brace_count -= 1
            if brace_count == 0 and start_idx is not None:
                obj_str = text[start_idx:i+1].strip()
                try:
                    objects.append(json.loads(obj_str))
                except json.JSONDecodeError:
                    pass  # skip invalid JSON
                start_idx = None

    return objects

# Convenience functions for immediate use
def filter_jsonld(data: Union[str, Dict, List, Any], **options) -> Union[Dict, List, Any, None]:
    """
    Simple filter function for immediate use
    
    Args:
        data: JSON-LD data to filter (string, dict, list, or other format)
        **options: Filtration options (see JSONLDFilter.filter for details)
        
    Returns:
        Filtered JSON-LD data
        
    Raises:
        ValueError: If input cannot be parsed as JSON
    """
    result = split_json_objects(data)
    print(f"Found {len(result)} JSON objects")
 
    filter_instance = JSONLDFilter()
    filtered_outputs = []

    for obj in result:
        filtered = filter_instance.filter(obj, **options)
        filtered_outputs.append(filtered)

    return filtered_outputs


def extract_client_info(data: Union[str, Dict, List, Any], **options) -> Dict:
    """
    Extract structured client information
    
    Args:
        data: JSON-LD data to process (string, dict, list, or other format)
        **options: Filtration options (see JSONLDFilter.filter for details)
        
    Returns:
        Structured dictionary with categorized client information
        
    Raises:
        ValueError: If input cannot be parsed as JSON
    """
    filter_instance = JSONLDFilter()
    return filter_instance.extract_client_info(data, **options)


# Example usage and testing
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Example JSON-LD data (based on your provided example)
    sample_data = {
        "@context": "https://schema.org",
        "@type": "Article",
        "headline": "I recently travelled from Canada to the US — Here are the 6 differences I noticed immediately",
        "name": "I recently travelled from Canada to the US — Here are the 6 differences I noticed immediately",
        "identifier": "2672224284",
        "url": "https://www.narcity.com/i-recently-travelled-from-canada-to-us",
        "thumbnailUrl": "https://www.narcity.com/media-library/image.jpg",
        "description": "Recently, I made the journey across the Canada-U.S. border...",
        "articleBody": "Recently, I made the journey across the Canada-U.S. border, from Toronto to New York...",
        "keywords": "canada-us border,canada us travel",
        "wordCount": 703,
        "dateCreated": "2025-05-29T13:32:10Z",
        "datePublished": "2025-05-29T13:32:10Z",
        "publisher": {
            "@type": "Organization",
            "name": "Narcity",
            "legalName": "Narcity",
            "foundingDate": "2013",
            "url": "https://www.narcity.com",
            "logo": {
                "@type": "ImageObject",
                "url": "https://assets.rbl.ms/logo.png",
                "width": 1000,
                "height": 540
            }
        },
        "author": {
            "@type": "Person",
            "jobTitle": "Writer",
            "name": "Tomás Keating",
            "url": "https://www.narcity.com/author/tomaskeating/",
            "description": "Originally from Ireland, Tomás Keating is the News Writer...",
            "sameAs": ["https://www.instagram.com/tomask95/", "https://www.linkedin.com/in/tomaskeating/"]
        }
    }
    '''
    # Test with various input formats
    print("=== Testing Different Input Formats ===")
    
    # 1. Test with dictionary (already JSON)
    print("\n1. Testing with dictionary input:")
    filtered = filter_jsonld(sample_data)
    print("✓ Dictionary parsing successful")
    
    # 2. Test with JSON string
    print("\n2. Testing with JSON string input:")
    json_string = json.dumps(sample_data)
    filtered = filter_jsonld(json_string)
    print("✓ JSON string parsing successful")
    
    # 3. Test with multiple JSON objects
    print("\n3. Testing with multiple JSON objects:")
    multiple_json = '''
    {
        "@type": "Article",
        "name": "First Article",
        "description": "First description"
    }
    {
        "@type": "Organization", 
        "name": "Test Org",
        "url": "https://example.com"
    }
    '''
    try:
        filtered = filter_jsonld(multiple_json)
        print("✓ Multiple JSON objects parsing successful")
    except ValueError as e:
        print(f"✗ Multiple JSON parsing failed: {e}")
    
    # 4. Test with HTML entities
    print("\n4. Testing with HTML entities:")
    html_entity_json = '{"name": "Test &amp; Company", "description": "We\\"re the best"}'
    try:
        filtered = filter_jsonld(html_entity_json)
        print("✓ HTML entities cleaning successful")
    except ValueError as e:
        print(f"✗ HTML entities parsing failed: {e}")
    '''

    sample_data2 = '''
    {
  "@context": "https://schema.org",
    "@type": "Article",
    "headline": "I recently travelled from Canada to the US — Here are the 6 differences I noticed immediately",
	"name": "I recently travelled from Canada to the US — Here are the 6 differences I noticed immediately",
    "identifier": "2672224284",
    "mainEntityOfPage": {
            "@type": "WebPage",
            "@id": "https://www.narcity.com/i-recently-travelled-from-canada-to-us",
			"name": "I recently travelled from Canada to the US — Here are the 6 differences I noticed immediately"
    },
    "url": "https://www.narcity.com/i-recently-travelled-from-canada-to-us",
    "thumbnailUrl": "https://www.narcity.com/media-library/the-empire-state-building-right-narcity-s-tomas-keating-at-robert-h-treman-state-park-near-ithica-ny.jpg?id=60333744&amp;width=1245&amp;height=700&amp;coordinates=2%2C0%2C3%2C0",
	"description": "Recently, I made the journey across the Canada-U.S. border, from Toronto to New York — and I have some big takeaways after my journey. Given the current tensions between the countries, and less Canadians being willing to cross the border, I was even more in tune with the differences.",
    "articleBody": "Recently, I made the journey across the Canada-U.S. border, from Toronto to New York — and I have some big takeaways after my journey. Given the current tensions between the countries, and less Canadians being willing to cross the border, I was even more in tune with the differences. I actually travelled to the state of New York twice in April – first flying from Toronto Pearson Airport to La Guardia Airport in New York City for a Gaelic football match (I&#39;m originally from Ireland and couldn&#39;t not see my home country compete), and then driving across to visit my partner’s family. The back-to-back trips were like a crash-course in American culture, from the busy streets of N.Y.C. to the rural areas of the State. All in all, it was very clear that while the country is right beside my (now) home of Canada, it is wildly different. All in all, I I love travelling and experiencing cultures, especially between places that are so close, yet have so many differences. [shortcode-Opinion site_id=21132993 expand=&#39;1&#39;]",
    "keywords": "canada-us border,canada us travel",
	 "wordCount": 703,
    "dateCreated": "2025-05-29T13:32:10Z",
	"datePublished": "2025-05-29T13:32:10Z",
    "dateModified": "2025-05-29T15:47:54Z",
    "articleSection": "Travel ",
"image": [
   
  { 
      "@type": "ImageObject", 
      "url": "https://www.narcity.com/media-library/the-empire-state-building-right-narcity-s-tomas-keating-at-robert-h-treman-state-park-near-ithica-ny.jpg?id=60333744&amp;width=1200&amp;height=1200&amp;coordinates=264%2C0%2C264%2C0", 
      "width": 1200, 
      "height": 1200 
    }, 
     
   
    { 
      "@type": "ImageObject", 
      "url": "https://www.narcity.com/media-library/the-empire-state-building-right-narcity-s-tomas-keating-at-robert-h-treman-state-park-near-ithica-ny.jpg?id=60333744&amp;width=2000&amp;height=1500&amp;coordinates=152%2C0%2C152%2C0", 
      "width": 2000, 
      "height": 1500 
    },
     
  {
      "@type": "ImageObject",
      "url": "https://www.narcity.com/media-library/the-empire-state-building-right-narcity-s-tomas-keating-at-robert-h-treman-state-park-near-ithica-ny.jpg?id=60333744&amp;width=1245&amp;height=700&amp;coordinates=2%2C0%2C3%2C0",
      "width": 1245,
      "height": 700
  },
  {
      "@type": "ImageObject",
      "url": "https://www.narcity.com/media-library/the-empire-state-building-right-narcity-s-tomas-keating-at-robert-h-treman-state-park-near-ithica-ny.jpg?id=60333744&amp;width=1000&amp;height=750&amp;coordinates=152%2C0%2C152%2C0",
      "width": 1000,
      "height": 750
  },
  {
      "@type": "ImageObject",
      "url": "https://www.narcity.com/media-library/the-empire-state-building-right-narcity-s-tomas-keating-at-robert-h-treman-state-park-near-ithica-ny.jpg?id=60333744&amp;width=600&amp;height=600&amp;coordinates=264%2C0%2C264%2C0", 
      "width": 600,
      "height": 600
  }
],
    "publisher": {
        "@type": "Organization",
        "name": "Narcity",
		"legalName": "Narcity",
		"foundingDate": "2013",
		"url": "https://www.narcity.com",
        "logo": {
            "@type": "ImageObject",
            "url": "https://assets.rbl.ms/26421222/origin.png",
            "width": 1000,
        	"height": 540
        }
    },
	"author": {
        "@context": "https://schema.org",
        "@type": "Person",
		"jobTitle": "Writer",
        "name": "Tomás Keating",
        "url": "https://www.narcity.com/author/tomaskeating/",
		"identifier": "27150123",
        "description": "Originally from Ireland, Tomás Keating is the News Writer – Toronto for Narcity. After graduating with a Masters in Journalism from the University of Galway in 2019, Tomás utilized his passion for news, current affairs, pop culture and sports as a digital journalist before relocating to Toronto in 2024. In his spare time, Tomás loves exploring the city, going to the cinema and playing Gaelic football with his local GAA club in Toronto.",
        "sameAs": ["https://www.instagram.com/tomask95/", "https://www.linkedin.com/in/tomaskeating/", "https://x.com/mossboxx?lang=en", "https://www.narcity.com/author/tomaskeating/"],
        "image": {
            "@type": "ImageObject",
            "url": "https://www.narcity.com/media-library/eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpbWFnZSI6Imh0dHBzOi8vYXNzZXRzLnJibC5tcy81OTc1OTQ3MS9vcmlnaW4ucG5nIiwiZXhwaXJlc19hdCI6MTgwMzA2NjQ3MH0.2NJIonTGkVDp9yfUGNKGCWY9ed5aerUJZ4I6R4M3VCw/image.png?width=210",
            "width": 210,
            "height": 210
        },
        "worksFor": {
            "@type": "NewsMediaOrganization",
            "name": "Narcity Media",
            "logo": {
                "@type": "ImageObject",
                "url": "https://assets.rbl.ms/30059856/origin.png",
                "width": 500,
                "height": 289
            },
            "masthead": "https://www.narcity.com/masthead",
            "correctionsPolicy": "https://www.narcity.com/editorial-standards",
            "ethicsPolicy": "https://www.narcity.com/editorial-standards",
            "publishingPrinciples": "https://www.narcity.com/editorial-standards",
            "missionCoveragePrioritiesPolicy": "https://www.narcity.com/our-mission",
            "ownershipFundingInfo": "https://www.narcitymedia.com/about",
            "verificationFactCheckingPolicy": "https://www.narcity.com/editorial-standards",
            "parentOrganization": {
                "@type": "Organization",
                "name": "Narcity Media",
                "logo": {
                    "@type": "ImageObject",
                    "url": "https://assets.rbl.ms/30059856/origin.png",
                    "width": 500,
                    "height": 289
                }
            }
        }
    }
}



  
  {
    "@context": "https://schema.org",
    "@type": "ItemList",
    "url": "https://www.narcity.com/i-recently-travelled-from-canada-to-us",
    "name": "I recently travelled from Canada to the US — Here are the 6 differences I noticed immediately",
    "itemListElement": [
          {
            "@type": "ListItem",
            "position": 1,
            "name": "U.S. cities are built different ",
            "item": {
              "@type": "Thing",
              "name": "U.S. cities are built different ",
              "description": "Even Canada&#39;s biggest cities kind of seem low-key compared to American ones. I mean, I live in Toronto which is supposedly Canada&#39;s N.Y.C., and still found a visit to the Big Apple wild.It is absolutely gigantic and moves at lightning speed. Everyone there is in a rush to get where they need to be, whereas I generally find Toronto to be more laid back.There are many similarities between the two cities — NYC has Times Square, and Toronto has Sankofa Square (formerly Yonge-Dundas Square) and both have Flatiron Buildings — but the Big Apple&#39;s versions are far bigger.It also just has way more going on, even bars staying open much longer than in Toronto. In the 6ix, the last call at a bar on the weekend is typically around 1:45 a.m., as they have to stop serving alcohol at 2 a.m.In NYC, I left a bar at 2:30 a.m., and the place was jammed with people."
            }
          }
          
        
      ,
          {
            "@type": "ListItem",
            "position": 1,
            "name": "Tipping is way easier in Canada",
            "item": {
              "@type": "Thing",
              "name": "Tipping is way easier in Canada",
              "description": "I know that I&#39;ve complained in the past about Canadian tipping culture, but at least the process here is pretty straightforward.When you settle your bill here, the server presents you with the total bill and comes with a card machine. On the screen, there are percentages, such as 18%, 20%, or 22%. You select one, and you pay for it.When I paid for drinks in New York, the bartender handed me a receipt, on which I had to write down the tip I wanted to give. This meant I had to do the math! As someone who&#39;s not the best at math, I found it tough to know what to tip.With all its flaws, I prefer the Canadian way of doing things."
            }
          }
          
        
      ,
          {
            "@type": "ListItem",
            "position": 1,
            "name": "The metric system is superior",
            "item": {
              "@type": "Thing",
              "name": "The metric system is superior",
              "description": "When we crossed the border in Buffalo, our Google Maps automatically converted from the metric system to imperial. That was handy, but still our car is in km/h, not mp/h – making it super difficult to adjust our speed accordingly. For two countries so close, it&#39;s somewhat shocking that they follow a different system."
            }
          }
          
        
      ,
          {
            "@type": "ListItem",
            "position": 1,
            "name": "There was a severe lack of Timmies",
            "item": {
              "@type": "Thing",
              "name": "There was a severe lack of Timmies",
              "description": "While there are many Tim Hortons in the U.S., there just aren&#39;t as many as there are in Canada.Instead, there are so many Dunkin&#39; Donuts and Starbucks in the U.S. While there are lovely stores in their own right, you just can&#39;t beat Tim&#39;s."
            }
          }
          
        
      ,
          {
            "@type": "ListItem",
            "position": 1,
            "name": "U.S. portion sizes are massive",
            "item": {
              "@type": "Thing",
              "name": "U.S. portion sizes are massive",
              "description": "While eating out, I found the food portion sizes in the U.S. were bigger than in Canada. You get more bang for your buck, but I was surprised when the server put my plate down sometimes. Also I found that overall Canadian store groceries had generally better quality food. I noticed it especially with bacon from the store — Canadian bacon is superior."
            }
          }
          
        
      ,
          {
            "@type": "ListItem",
            "position": 1,
            "name": "There are U.S. flags EVERYWHERE",
            "item": {
              "@type": "Thing",
              "name": "There are U.S. flags EVERYWHERE",
              "description": "Americans love The Stars and Stripes.This was something I especially noticed driving for hours in upstate NY.The number of American flags in people&#39;s front yards or on their porches is not something you see too often in Canada with the Maple Leaf flag.Here, you might see a massive Canadian flag outside a government building or a shopping centre, but in the U.S., they are everywhere."
            }
          }
          
        
      
    ]
  }

'''


    # Test basic filtering
    print("\n=== Basic Filtering ===")
    filtered = filter_jsonld(sample_data2)
    print(json.dumps(filtered, indent=2))
    '''
    print("\n=== Client Info Extraction ===")
    client_info = extract_client_info(sample_data)
    print(json.dumps(client_info, indent=2))

    
    print("\n=== Advanced Filtering (Preserve Types) ===")
    advanced_filtered = filter_jsonld(sample_data, preserve_types=True, remove_context=False)
    print(json.dumps(advanced_filtered, indent=2))
    '''