"""
Spanish YouTube Channel Email Finder
Searches for Spanish YouTube channels and extracts contact information
Uses multiple methods: YouTube API v3, web scraping, and other sources
"""

import csv
import re
import time
import json
import requests
from typing import List, Dict, Optional
from urllib.parse import quote, urlparse
import logging

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set requests logging to WARNING level to reduce noise
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)

class SpanishYouTubeChannelFinder:
    def __init__(self, youtube_api_key: str):
        self.api_key = youtube_api_key
        self.channels_data = []
        self.emails_found = 0
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
    def search_youtube_api(self, query: str = "spanish", max_results: int = 50) -> List[Dict]:
        """Search YouTube using API v3"""
        logger.info(f"Searching YouTube API for: {query}")
        channels = []
        
        try:
            # Search for channels
            search_url = "https://www.googleapis.com/youtube/v3/search"
            params = {
                'part': 'snippet',
                'q': query,
                'type': 'channel',
                'relevanceLanguage': 'es',
                'maxResults': min(max_results, 50),
                'key': self.api_key
            }
            
            logger.debug(f"API Request URL: {search_url}")
            logger.debug(f"API Request Params: {params}")
            
            # Increase timeout and add retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = self.session.get(search_url, params=params, timeout=30)  # Increased timeout to 30 seconds
                    logger.debug(f"API Response Status: {response.status_code}")
                    
                    # Check for rate limiting or quota issues
                    if response.status_code == 403:
                        error_data = response.json()
                        if 'error' in error_data and 'errors' in error_data['error']:
                            for error in error_data['error']['errors']:
                                if error.get('reason') in ['quotaExceeded', 'dailyLimitExceeded', 'rateLimitExceeded']:
                                    logger.error("YouTube API quota exceeded or rate limited. Please check your Google Cloud Console.")
                                    return channels
                    
                    response.raise_for_status()
                    data = response.json()
                    logger.debug(f"API Response: {json.dumps(data, indent=2)[:500]}...")  # Log first 500 chars of response
                    break  # Exit retry loop if successful
                    
                except requests.exceptions.RequestException as e:
                    if attempt == max_retries - 1:  # Last attempt
                        logger.error(f"YouTube API request failed after {max_retries} attempts: {str(e)}")
                        if hasattr(e, 'response') and e.response is not None:
                            logger.error(f"Response content: {e.response.text}")
                        return channels
                    wait_time = (attempt + 1) * 5  # Exponential backoff
                    logger.warning(f"Attempt {attempt + 1} failed. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
            
            for item in data.get('items', []):
                channel_id = item['snippet']['channelId']
                channels.append(self.get_channel_details(channel_id))
                
                if self.emails_found >= 100:
                    logger.info("Reached 100 emails, stopping search")
                    break
                    
        except requests.exceptions.RequestException as e:
            logger.error(f"YouTube API error: {e}")
            
        return channels
    
    def get_channel_details(self, channel_id: str) -> Optional[Dict]:
        """Get detailed channel information from YouTube API"""
        try:
            url = "https://www.googleapis.com/youtube/v3/channels"
            params = {
                'part': 'snippet,statistics,brandingSettings',
                'id': channel_id,
                'key': self.api_key
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if not data.get('items'):
                return None
                
            item = data['items'][0]
            snippet = item['snippet']
            stats = item.get('statistics', {})
            branding = item.get('brandingSettings', {}).get('channel', {})
            
            channel_data = {
                'channel_name': snippet.get('title', ''),
                'channel_url': f"https://www.youtube.com/channel/{channel_id}",
                'handle': snippet.get('customUrl', ''),
                'subscriber_count': stats.get('subscriberCount', '0'),
                'description': snippet.get('description', ''),
                'emails': set(),
                'instagram': '',
                'twitter': '',
                'facebook': '',
                'tiktok': '',
                'twitch': '',
                'discord': '',
                'linkedin': '',
                'website': ''
            }
            
            # Extract emails and links from description
            self.extract_contacts_from_text(channel_data, snippet.get('description', ''))
            
            # Try to get more info from channel page
            self.scrape_channel_page(channel_data)
            
            if channel_data['emails']:
                self.emails_found += len(channel_data['emails'])
                
            return channel_data
            
        except Exception as e:
            logger.error(f"Error getting channel details: {e}")
            return None
    
    def scrape_channel_page(self, channel_data: Dict):
        """Scrape the channel page for additional contact information"""
        try:
            # Try both channel URL formats
            urls = [
                channel_data['channel_url'],
                f"https://www.youtube.com/{channel_data['handle']}" if channel_data['handle'] else None,
                f"{channel_data['channel_url']}/about"
            ]
            
            for url in urls:
                if not url:
                    continue
                    
                try:
                    response = self.session.get(url, timeout=10)
                    if response.status_code == 200:
                        html = response.text
                        self.extract_contacts_from_text(channel_data, html)
                        time.sleep(1)  # Rate limiting
                except:
                    continue
                    
        except Exception as e:
            logger.error(f"Error scraping channel page: {e}")
    
    def extract_contacts_from_text(self, channel_data: Dict, text: str):
        """Extract emails and social media links from text"""
        if not text:
            return
            
        # Extract emails
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(email_pattern, text)
        for email in emails:
            # Filter out common false positives
            if not any(x in email.lower() for x in ['example.com', 'yourdomain', 'test']):
                channel_data['emails'].add(email.lower())
        
        # Extract social media links
        url_pattern = r'https?://(?:www\.)?([^\s<>"]+)'
        urls = re.findall(url_pattern, text)
        
        for url in urls:
            url_lower = url.lower()
            
            if 'instagram.com' in url_lower and not channel_data['instagram']:
                channel_data['instagram'] = f"https://{url}" if not url.startswith('http') else url
            elif 'twitter.com' in url_lower or 'x.com' in url_lower and not channel_data['twitter']:
                channel_data['twitter'] = f"https://{url}" if not url.startswith('http') else url
            elif 'facebook.com' in url_lower and not channel_data['facebook']:
                channel_data['facebook'] = f"https://{url}" if not url.startswith('http') else url
            elif 'tiktok.com' in url_lower and not channel_data['tiktok']:
                channel_data['tiktok'] = f"https://{url}" if not url.startswith('http') else url
            elif 'twitch.tv' in url_lower and not channel_data['twitch']:
                channel_data['twitch'] = f"https://{url}" if not url.startswith('http') else url
            elif 'discord' in url_lower and not channel_data['discord']:
                channel_data['discord'] = f"https://{url}" if not url.startswith('http') else url
            elif 'linkedin.com' in url_lower and not channel_data['linkedin']:
                channel_data['linkedin'] = f"https://{url}" if not url.startswith('http') else url
            elif not any(platform in url_lower for platform in ['youtube.com', 'youtu.be', 'google.com']):
                # Potential website or other link
                full_url = f"https://{url}" if not url.startswith('http') else url
                if not channel_data['website'] and self.looks_like_website(url):
                    channel_data['website'] = full_url
    
    def looks_like_website(self, url: str) -> bool:
        """Check if URL looks like a personal/business website"""
        common_domains = ['instagram', 'twitter', 'facebook', 'tiktok', 'twitch', 
                         'discord', 'linkedin', 'youtube', 'youtu.be']
        return not any(domain in url.lower() for domain in common_domains)
    
    def search_with_web_scraping(self, queries: List[str]):
        """Fallback: Search using web scraping methods"""
        logger.info("Starting web scraping fallback methods")
        
        for query in queries:
            if self.emails_found >= 100:
                break
                
            # Method 1: Search engines
            self.search_via_duckduckgo(query)
            
            # Method 2: Social Blade (YouTube stats site)
            self.search_socialblade(query)
            
            time.sleep(2)  # Rate limiting
    
    def search_via_duckduckgo(self, query: str):
        """Search for Spanish YouTube channels via DuckDuckGo"""
        try:
            search_query = f"site:youtube.com {query} spanish español contact email"
            url = f"https://html.duckduckgo.com/html/?q={quote(search_query)}"
            
            response = self.session.get(url, timeout=10)
            if response.status_code == 200:
                # Extract YouTube channel URLs from results
                channel_pattern = r'youtube\.com/(?:channel/|@|c/)([^/\s"]+)'
                matches = re.findall(channel_pattern, response.text)
                
                for match in matches[:10]:  # Limit results
                    if self.emails_found >= 100:
                        break
                    # Process channel
                    logger.info(f"Found channel via web search: {match}")
                    
        except Exception as e:
            logger.error(f"DuckDuckGo search error: {e}")
    
    def search_socialblade(self, query: str):
        """Search Social Blade for channel information"""
        try:
            url = f"https://socialblade.com/youtube/search/search?query={quote(query)}"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                # Parse results (simplified - actual implementation would need proper HTML parsing)
                logger.info("Searched Social Blade for additional channels")
                
        except Exception as e:
            logger.error(f"Social Blade search error: {e}")
    
    def save_to_csv(self, filename: str = 'spanish_youtube_channels.csv'):
        """Save collected data to CSV file"""
        logger.info(f"Saving {len(self.channels_data)} channels to {filename}")
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['channel_name', 'channel_url', 'handle', 'subscriber_count', 
                         'emails', 'instagram', 'twitter', 'facebook', 'tiktok', 
                         'twitch', 'discord', 'linkedin', 'website', 'description']
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for channel in self.channels_data:
                # Convert sets and lists to strings
                channel_copy = channel.copy()
                channel_copy['emails'] = ', '.join(sorted(channel['emails'])) if channel['emails'] else ''
                channel_copy['description'] = channel['description'][:500]  # Limit description length
                
                writer.writerow(channel_copy)
        
        logger.info(f"Successfully saved to {filename}")
        logger.info(f"Total emails found: {self.emails_found}")
    
    def run(self, search_queries: List[str] = None):
        """Main execution method"""
        if search_queries is None:
            search_queries = [
    # Language Learning
    "clases de español",
    "español conversación",
    "gramática española",
    "vocabulario español",
    "español avanzado",
    
    # Entertainment & Culture
    "series en español",
    "películas en español",
    "humor en español",
    "historias en español",
    "leyendas hispanas",
    
    # Education & Learning
    "ciencia en español",
    "historia de España",
    "historia de Latinoamérica",
    "matemáticas en español",
    "educación financiera español",
    
    # Lifestyle & Hobbies
    "ejercicio en español",
    "yoga en español",
    "meditación en español",
    "manualidades en español",
    "jardinería en español",
    
    # Food & Cooking
    "recetas españolas",
    "cocina española",
    "cocina latina",
    "postres en español",
    "cocina saludable español",
    
    # Technology & Gaming
    "videojuegos en español",
    "programación en español",
    "reseñas tecnología español",
    "tutoriales photoshop español",
    "inteligencia artificial español",
    
    # News & Current Affairs
    "análisis noticias español",
    "política en español",
    "economía en español",
    "medio ambiente español",
    "salud en español",
    
    # Regional Focus
    "cultura mexicana",
    "cultura argentina",
    "cultura española",
    "cultura colombiana",
    "cultura peruana",
    
    # Business & Career
    "emprendimiento en español",
    "marketing digital español",
    "carrera profesional español",
    "inversiones en español",
    "trabajo remoto español",
    
    # Kids & Family
    "cuentos en español",
    "canciones infantiles español",
    "aprendizaje infantil español",
    "dibujos animados español",
    "actividades niños español",
    
    # Travel & Exploration
    "viajar por España",
    "América Latina viajes",
    "ciudades españolas",
    "playas de Latinoamérica",
    "aventuras en español"
]
            
        
        logger.info("Starting Spanish YouTube channel search")
        
        # Method 1: YouTube API v3
        try:
            for query in search_queries:
                if self.emails_found >= 100:
                    break
                    
                channels = self.search_youtube_api(query)
                self.channels_data.extend([c for c in channels if c])
                time.sleep(1)  # Rate limiting
                
        except Exception as e:
            logger.error(f"YouTube API failed: {e}")
        
        # Method 2: Web scraping fallback if needed
        if self.emails_found < 100:
            logger.info(f"Only {self.emails_found} emails found, trying web scraping methods")
            self.search_with_web_scraping(search_queries)
        
        # Save results
        self.save_to_csv()
        
        return self.channels_data


def main():
    """Main function to run the script"""
    
    # IMPORTANT: Replace with your actual YouTube API key
    # Get one from: https://console.cloud.google.com/apis/credentials
    YOUTUBE_API_KEY = ""
    
    if YOUTUBE_API_KEY == "YOUR_YOUTUBE_API_KEY_HERE":
        print("⚠️  ERROR: Please set your YouTube API key!")
        print("Get one from: https://console.cloud.google.com/apis/credentials")
        print("Then replace YOUR_YOUTUBE_API_KEY_HERE in the script")
        return
    
    # Initialize finder
    finder = SpanishYouTubeChannelFinder(YOUTUBE_API_KEY)
    
    # Define search queries
    search_queries = [
        "español vlog",
        "canal español comedia",
        "español gaming",
        "cocina española",
        "español música",
        "español tutorial",
        "español tecnología",
        "español viajes",
        "español fitness",
        "español belleza"
    ]
    
    # Run the search
    finder.run(search_queries)
    
    print(f"\n✅ Completed! Found {len(finder.channels_data)} channels")
    print(f"📧 Total emails found: {finder.emails_found}")
    print(f"💾 Results saved to: spanish_youtube_channels.csv")


if __name__ == "__main__":
    main()
    
    """search_queries = [
    "español viajes",          # Spanish travel
    "aprender español",        # Learn Spanish
    "cultura hispana",         # Hispanic culture
    "noticias en español",     # News in Spanish
    "español para principiantes",  # Spanish for beginners
    "música en español",       # Music in Spanish
    "deportes en español",     # Sports in Spanish
    "tecnología en español",   # Technology in Spanish
    "cocina mexicana",         # Mexican cuisine
    "español negocios"         # Business Spanish
]"""