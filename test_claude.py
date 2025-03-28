import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

class IMDbScraper:
    def __init__(self, max_workers=10, timeout=10):
        self.max_workers = max_workers
        self.timeout = timeout
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept-Language': 'en-US,en;q=0.9'
        }
        
    def get_imdb_search_results(self, country, start_year=2000, end_year=2025, start_index=1):
        url = f"https://www.imdb.com/search/title/?title_type=feature&release_date={start_year},{end_year}&countries={country}&sort=year,asc&start={start_index}&ref_=adv_nxt"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except requests.RequestException as e:
            logging.error(f"Failed to retrieve search results: {e}")
            return None

    def extract_movie_links(self, soup):
        """
        Extract unique movie links from IMDb search results page with enhanced robustness
        """
        movie_links = set()  # Use a set to ensure uniqueness
        
        link_selectors = [
            'div.lister-item-content h3.lister-item-header a',
            'div.sc-afe43def-1 a.sc-afe43def-2',
            'h3.lister-item-header a',
            'div.lister-item a[href^="/title/"]',
            'a.lister-item-header-link',
            'a[href^="/title/tt"]'
        ]
        
        for selector in link_selectors:
            movie_title_links = soup.select(selector)
            
            if movie_title_links:
                for link in movie_title_links:
                    href = link.get('href', '')
                    if href and href.startswith('/title/'):
                        # Remove any additional query parameters
                        clean_href = href.split('?')[0]
                        full_url = f"https://www.imdb.com{clean_href}"
                        movie_links.add(full_url)
                
                if movie_links:
                    logging.info(f"Found {len(movie_links)} unique movie links using selector: {selector}")
                    break
        
        if not movie_links:
            logging.warning("No movie links found. Debugging page content:")
            logging.warning(soup.prettify()[:1000])
        
        return list(movie_links)

    def extract_year(self, soup):
        """
        Enhanced year extraction with multiple methods
        """
        year_selectors = [
            # More comprehensive year extraction
            'h1 + div div[data-testid="title-metadata"] ul li',
            'ul.sc-afe43def-1 > li:first-child',
            'div[data-testid="hero-title-block__metadata"] > ul > li',
            'span.sc-8c396aa2-2',
            'li.ipc-inline-list__item:first-child'
        ]
        
        for selector in year_selectors:
            year_elem = soup.select_one(selector)
            if year_elem:
                # Try different parsing strategies
                year_text = year_elem.get_text(strip=True)
                year_match = re.search(r'\b(19\d{2}|20\d{2})\b', year_text)
                if year_match:
                    return year_match.group()
        
        # Fallback method: try to find year in page title
        title_elem = soup.find('title')
        if title_elem:
            title_year_match = re.search(r'\b(19\d{2}|20\d{2})\b', title_elem.get_text())
            if title_year_match:
                return title_year_match.group()
        
        return 'N/A'

    def extract_rating(self, soup):
        """
        More robust rating extraction with multiple methods
        """
        rating_selectors = [
            'div[data-testid="hero-rating-bar__aggregate-rating__score"] span',
            'span.sc-bde20123-1',
            'div.sc-7ab21ed2-1',
            'span.sc-7ab21ed2-1'
        ]
        
        for selector in rating_selectors:
            rating_elem = soup.select_one(selector)
            if rating_elem:
                rating_text = rating_elem.get_text().strip()
                # More flexible rating parsing
                rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                if rating_match:
                    return rating_match.group(1)
        
        return 'N/A'

    def extract_genres(self, soup):
        """
        More comprehensive genre extraction
        """
        genre_selectors = [
            'div[data-testid="genres"] a',
            'span.ipc-chip__text',
            'div.sc-16ede01-0 a',
            'li.ipc-inline-list__item a'
        ]
        
        genres = []
        for selector in genre_selectors:
            genre_elems = soup.select(selector)
            if genre_elems:
                genres = list(set(genre.get_text().strip() for genre in genre_elems))
                if genres:
                    return genres
        
        return []

    def get_movie_details(self, movie_url):
        """
        Comprehensive and resilient movie details extraction
        """
        try:
            # Add a unique identifier to the movie URL to prevent duplicates
            unique_url = movie_url.split('?')[0]
            
            response = requests.get(unique_url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Movie details extraction with fallback mechanisms
            movie_data = {
                'title': 'Unknown',
                'year': 'N/A',
                'genres': [],
                'imdb_rating': 'N/A',
                'local_gross': 'N/A',
                'worldwide_gross': 'N/A',
                'budget': 'N/A',
                'opening_weekend': 'N/A',
                'url': unique_url
            }
            
            # Title extraction
            title_elem = soup.find('h1')
            movie_data['title'] = title_elem.text.strip() if title_elem else 'Unknown'
            
            # Year extraction
            movie_data['year'] = self.extract_year(soup)
            
            # Skip movies with invalid years
            if movie_data['year'] == 'N/A':
                logging.info(f"Skipping {unique_url} - No valid year found")
                return None
            
            # Genres extraction
            movie_data['genres'] = self.extract_genres(soup)
            
            # Rating extraction
            movie_data['imdb_rating'] = self.extract_rating(soup)
            
            # Skip movies without ratings
            if movie_data['imdb_rating'] == 'N/A':
                logging.info(f"Skipping {unique_url} - No rating found")
                return None
            
            # Box Office and Budget extraction with more robust parsing
            try:
                box_office_section = soup.find('section', {'data-testid': 'BoxOffice'})
                if box_office_section:
                    list_items = box_office_section.find_all('li', class_='ipc-metadata-list__item')
                    
                    for item in list_items:
                        label_elem = item.find('span', class_='ipc-metadata-list-item__label')
                        if label_elem:
                            label = label_elem.text.strip()
                            value_elem = item.find('span', class_='ipc-metadata-list-item__list-content-item')
                            if value_elem:
                                value = value_elem.text.strip()
                                
                                if 'Budget' in label:
                                    movie_data['budget'] = value
                                elif 'Gross worldwide' in label:
                                    movie_data['worldwide_gross'] = value
                                elif 'Opening weekend' in label:
                                    movie_data['opening_weekend'] = value
                                elif 'Gross US & Canada' in label:
                                    movie_data['local_gross'] = value
            except Exception as e:
                logging.error(f"Box office extraction error: {e}")
            
            return movie_data
        
        except requests.RequestException as e:
            logging.error(f"Error scraping {unique_url}: {e}")
            return None

    def scrape_country_films(self, country, start_year=2000, end_year=2025, max_pages=5):
        """
        Enhanced parallel scraping with improved duplicate prevention
        """
        all_movies = []
        processed_urls = set()  # Track processed URLs to prevent duplicates
        
        country_codes = {
            'Kazakhstan': 'kz',
            'South Korea': 'kr'
        }
        
        country_code = country_codes.get(country, country.lower())
        
        for page in range(1, max_pages + 1):
            start_index = 1 + (page - 1) * 50
            logging.info(f"Scraping page {page} for {country} films...")
            
            soup = self.get_imdb_search_results(country_code, start_year, end_year, start_index)
            if not soup:
                logging.warning(f"No results found for page {page}")
                break
            
            movie_links = self.extract_movie_links(soup)
            if not movie_links:
                logging.warning("No movie links found on this page.")
                break

            
            # More robust duplicate prevention
            new_movie_links = [
                link for link in movie_links 
                if link.split('?')[0] not in processed_urls
            ]
            
            # Update processed URLs
            processed_urls.update(link.split('?')[0] for link in new_movie_links)
            
            # Parallel processing of movie details
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_url = {
                    executor.submit(self.get_movie_details, link): link 
                    for link in new_movie_links
                }
                
                for future in as_completed(future_to_url):
                    movie_url = future_to_url[future]
                    try:
                        movie_data = future.result()
                        if movie_data:
                            movie_data['country'] = country
                            all_movies.append(movie_data)
                    except Exception as e:
                        logging.error(f"Error processing {movie_url}: {e}")
            
            # Prevent potential IP blocking
            time.sleep(2)
        
        # Create DataFrame
        df = pd.DataFrame(all_movies)
        
        # Save to CSV
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{country.replace(' ', '_')}_films_{start_year}_{end_year}_{timestamp}.csv"
        df.to_csv(filename, index=False, encoding='utf-8')
        
        logging.info(f"Saved {len(df)} movies to {filename}")
        return df

def main():
    scraper = IMDbScraper(max_workers=20)
    
    # Increase max_pages to get more comprehensive results
    kazakhstan_films = scraper.scrape_country_films('Kazakhstan', 2000, 2025, max_pages=10)
    south_korea_films = scraper.scrape_country_films('South Korea', 2000, 2025, max_pages=10)
    
    combined_df = pd.concat([kazakhstan_films, south_korea_films], ignore_index=True)
    combined_df.to_csv('combined_film_data.csv', index=False, encoding='utf-8')
    
    logging.info("Scraping complete. Combined data saved.")

if __name__ == "__main__":
    main()