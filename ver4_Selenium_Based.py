import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Selenium Imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

class IMDbScraper:
    def __init__(self, max_workers=10, timeout=10):
        self.max_workers = max_workers
        self.timeout = timeout
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept-Language': 'en-US,en;q=0.9'
        }
        
    def get_imdb_search_results(self, url, max_clicks=3, wait_time=0.5):
        """
        Retrieves IMDb search results using Selenium for pages with 'Load More' button, 
        otherwise falls back to BeautifulSoup with requests.
        """
        logging.info("Using Selenium to scrape IMDb search results with dynamic content loading.")

        # Set up Selenium WebDriver
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Run in headless mode
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920x1080")
        
        service = Service(r'C:\chromedriver.exe')  # Update this path if needed
        driver = webdriver.Chrome(service=service)

        # Open IMDb search page
        driver.get(url)
        time.sleep(2)  # Let page load

        for i in range(max_clicks):
            try:
                # Scroll to the bottom of the page
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)  # Allow time for new movies to load
                
                load_more_button = WebDriverWait(driver, wait_time).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "button.ipc-btn.ipc-btn--single-padding.ipc-btn--center-align-content.ipc-btn--default-height.ipc-btn--core-base.ipc-btn--theme-base.ipc-btn--button-radius.ipc-btn--on-accent2.ipc-text-button.ipc-see-more__button"))
                )
                driver.execute_script("arguments[0].click();", load_more_button)
                time.sleep(1)  # Allow time for new movies to load
                logging.info(f"Clicked 'Load More' button {i+1} times")
            except Exception:
                logging.info("No more 'Load More' button found or all results loaded.")
                break

        # Get page source and parse with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        driver.quit()  # Close browser
        return soup
        
    def extract_movie_links(self, soup):
        """
        Extract unique movie links from IMDb search results.
        """
        movie_links = set()
        
        link_selectors = [
            'a[href^="/title/tt"]',
            'div.lister-item-content h3.lister-item-header a',
            'h3.lister-item-header a',
            'div.lister-item a[href^="/title/"]',
            'a.lister-item-header-link'
        ]
        
        for selector in link_selectors:
            movie_title_links = soup.select(selector)
            
            if movie_title_links:
                for link in movie_title_links:
                    href = link.get('href', '')
                    if href and href.startswith('/title/'):
                        clean_href = href.split('?')[0]
                        full_url = f"https://www.imdb.com{clean_href}"
                        movie_links.add(full_url)
                
                if movie_links:
                    logging.info(f"Found {len(movie_links)} unique movie links using selector: {selector}")
                    break
        
        if not movie_links:
            logging.warning("No movie links found.")
        
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
                # logging.info(f"Skipping {unique_url} - No rating found")
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

    def scrape_country_films(self, country, start_year=2000, end_year=2025, max_clicks=3):
        """
        Scrapes movies for a given country using Selenium or requests.
        """
        all_movies = []
        country_codes = {'Kazakhstan': 'kz', 'South Korea': 'kr'}
        country_code = country_codes.get(country, country.lower())

        imdb_url = f"https://www.imdb.com/search/title/?title_type=feature&release_date={start_year},{end_year}&countries={country_code}&sort=year,asc"
        logging.info(f"Starting IMDb scraping for {country} films")

        soup = self.get_imdb_search_results(imdb_url , max_clicks)
        if not soup:
            logging.error("Failed to retrieve search results.")
            return None

        movie_links = self.extract_movie_links(soup)
        if not movie_links:
            logging.warning("No movie links found.")
            return None

        logging.info(f"Found {len(movie_links)} movies. Extracting details...")

        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {executor.submit(self.get_movie_details, link): link for link in movie_links}

            for future in as_completed(future_to_url):
                try:
                    movie_data = future.result()
                    if movie_data:
                        all_movies.append(movie_data)
                except Exception as e:
                    logging.error(f"Error processing movie: {e}")

        # Save to CSV
        df = pd.DataFrame(all_movies)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"tables/{country.replace(' ', '_')}_films_{start_year}_{end_year}_{timestamp}.csv"
        
        if not df.empty:
            df.to_csv(filename, index=False, encoding='utf-8')
            logging.info(f"Saved {len(df)} movies to {filename}")
        else:
            logging.warning("No movies were found to save to CSV")
        
        return df



def main():
    scraper = IMDbScraper(max_workers=20)
    
    # Increase max_pages to get more comprehensive results
    kazakhstan_films = scraper.scrape_country_films('Kazakhstan', 2000, 2025, max_clicks=9)
    south_korea_films = scraper.scrape_country_films('South Korea', 2000, 2025, max_clicks=9)
    
    # Convert to Excel
    if kazakhstan_films is not None:
        kazakhstan_films.to_excel("tables/kazakhstan_films.xlsx", index=False, header=True)
        
    if south_korea_films is not None:
        south_korea_films.to_excel("tables/south_korea_films.xlsx", index=False, header=True)

if __name__ == "__main__":
    main()