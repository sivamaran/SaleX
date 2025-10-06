"""
Centralized scraper registry to simplify adding new scrapers.

Register scraper metadata once here to avoid touching multiple files.
"""

from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class ScraperMeta:
    """Metadata for a scraper used across the app.

    - name: unique scraper key used in APIs and selection
    - url_type: classification type stored by web_url_scraper (instagram|linkedin|youtube|general)
    - site_filter: snippet appended to queries for platform-targeting
    - prompt_block: instruction block injected into platform-specific prompt
    - description: short human-readable description
    """

    name: str
    url_type: str
    site_filter: str
    prompt_block: str
    description: str


# Base prompt helpers (kept close to registry to be single-source of truth)
PROMPT_BLOCKS: Dict[str, str] = {
    'instagram': (
        'STRICT: Every query MUST target Instagram profiles or posts only.\n'
        '- Always include: site:instagram.com\n'
        '- Prefer: (site:instagram.com/p OR site:instagram.com/reel OR site:instagram.com/*)\n'
        '- Use operators when helpful: intitle:, quotes for roles/industries, OR for personas.\n'
    ),
    'linkedin': (
        'STRICT: Every query MUST target LinkedIn people/company pages only.\n'
        '- Always include: site:linkedin.com\n'
        '- Prefer: (site:linkedin.com/in OR site:linkedin.com/company OR site:linkedin.com/posts OR site:linkedin.com/newsletters)\n'
        '- Use operators: intitle:, quotes for exact roles, OR for multiple personas.\n'
    ),
    'youtube': (
        'STRICT: Every query MUST target YouTube channels/profiles only.\n'
        '- Always include: site:youtube.com\n'
        '- Prefer: (site:youtube.com/@ OR site:youtube.com/channel)\n'
        '- Use operators: intitle:, quotes, OR as needed.\n'
    ),
    'facebook': (
        'STRICT: Every query MUST target Facebook profiles, pages, or posts only.\n'
        '- Always include: site:facebook.com\n'
        '- Prefer: (site:facebook.com/profile.php OR site:facebook.com/pages OR site:facebook.com/groups OR site:facebook.com/events)\n'
        '- Use operators: intitle:, quotes for exact roles, OR for multiple personas.\n'
    ),
    'company_directory': (
        'STRICT: Every query MUST target business directory websites only.\n'
        '- Always include business directory sites: site:thomasnet.com OR site:indiamart.com OR site:kompass.com OR site:yellowpages.com OR site:yelp.com OR site:crunchbase.com OR site:opencorporates.com\n'
        '- Prefer company/supplier/manufacturer listings and directories\n'
        '- Use operators: intitle:"directory" OR intitle:"companies" OR intitle:"suppliers", quotes for exact business types.\n'
    ),
    'web_scraper': (
        'STRICT: General web discovery (non-social).\n'
        '- EXCLUDE major social sites in queries (avoid instagram.com, linkedin.com, youtube.com, facebook.com).\n'
        '- Use intitle:, inurl:, and AND/OR operators to target likely company/contact pages.\n'
    ),
}


# Site filter snippets appended to base queries to get platform-specific variants
SITE_FILTERS: Dict[str, str] = {
    'instagram': 'site:instagram.com (site:instagram.com/p OR site:instagram.com/reel OR site:instagram.com/*) ',
    'linkedin': 'site:linkedin.com (site:linkedin.com/in OR site:linkedin.com/company OR site:linkedin.com/posts OR site:linkedin.com/newsletters) ',
    'youtube': 'site:youtube.com (site:youtube.com/@ OR site:youtube.com/channel) ',
    'facebook': 'site:facebook.com (site:facebook.com/profile.php OR site:facebook.com/pages OR site:facebook.com/groups OR site:facebook.com/events) ',
    'company_directory': 'site:thomasnet.com OR site:indiamart.com OR site:kompass.com OR site:yellowpages.com OR site:yelp.com OR site:crunchbase.com OR site:opencorporates.com OR site:manta.com OR site:dexknows.com OR site:superpages.com ',
    # web_scraper intentionally omitted from platform site filters
}


SCRAPERS: Dict[str, ScraperMeta] = {
    'web_scraper': ScraperMeta(
        name='web_scraper',
        url_type='general',
        site_filter='',
        prompt_block=PROMPT_BLOCKS['web_scraper'],
        description='General web scraping for websites'
    ),
    'company_directory': ScraperMeta(
        name='company_directory',
        url_type='company_directory',
        site_filter=SITE_FILTERS['company_directory'],
        prompt_block=PROMPT_BLOCKS['company_directory'],
        description='Business directory websites (ThomasNet, IndiaMart, YellowPages, etc.)'
    ),
    'instagram': ScraperMeta(
        name='instagram',
        url_type='instagram',
        site_filter=SITE_FILTERS['instagram'],
        prompt_block=PROMPT_BLOCKS['instagram'],
        description='Instagram profiles and posts'
    ),
    'linkedin': ScraperMeta(
        name='linkedin',
        url_type='linkedin',
        site_filter=SITE_FILTERS['linkedin'],
        prompt_block=PROMPT_BLOCKS['linkedin'],
        description='LinkedIn profiles and companies'
    ),
    'youtube': ScraperMeta(
        name='youtube',
        url_type='youtube',
        site_filter=SITE_FILTERS['youtube'],
        prompt_block=PROMPT_BLOCKS['youtube'],
        description='YouTube channels and videos'
    ),
    'facebook': ScraperMeta(
        name='facebook',
        url_type='facebook',
        site_filter=SITE_FILTERS['facebook'],
        prompt_block=PROMPT_BLOCKS['facebook'],
        description='Facebook profiles, pages, and posts'
    ),
}


def get_scraper_names() -> List[str]:
    return list(SCRAPERS.keys())


def get_available_scrapers() -> Dict[str, bool]:
    # All registered scrapers are considered available; adapt if dynamic checks are added later
    return {name: True for name in SCRAPERS}


def get_scrapers_info() -> Dict[str, str]:
    return {name: meta.description for name, meta in SCRAPERS.items()}


def get_site_filter(scraper_name: str) -> str:
    meta = SCRAPERS.get(scraper_name)
    return meta.site_filter if meta else ''


def get_prompt_block(scraper_name: str) -> str:
    meta = SCRAPERS.get(scraper_name)
    return meta.prompt_block if meta else ''


def is_valid_scraper(scraper_name: str) -> bool:
    return scraper_name in SCRAPERS


def get_url_type_map() -> Dict[str, str]:
    return {name: meta.url_type for name, meta in SCRAPERS.items()}


