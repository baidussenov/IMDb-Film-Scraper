import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

class IMDbScraper:
    def __init__(self, max_workers=10, timeout=10):
        """
        Initialize IMDb scraper with configurable concurrency
        
        Args:
            max_workers (int): Number of concurrent threads
            timeout (int): Request timeout in seconds
        """
        self.max_workers = max_workers
        self.timeout = timeout
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9'
        }
        
    def get_imdb_search_results(self, country, start_year=2000, end_year=2025, start_index=1):
        """
        Scrape IMDb search results for movies from a specific country within year range.
        
        Args:
            country (str): Country code (e.g., 'kz' for Kazakhstan)
            start_year (int): Starting year for the search
            end_year (int): Ending year for the search
            start_index (int): Starting index for pagination
            
        Returns:
            soup (BeautifulSoup): Parsed HTML of the search results
        """
        url = f"https://www.imdb.com/search/title/?title_type=feature&release_date={start_year},{end_year}&countries={country}&sort=year,asc&start={start_index}&ref_=adv_nxt"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            return soup
        except requests.RequestException as e:
            logging.error(f"Failed to retrieve search results: {e}")
            return None

    def extract_movie_links(self, soup):
        """
        Extract movie links from IMDb search results page.
        
        Args:
            soup (BeautifulSoup): Parsed HTML of search results
            
        Returns:
            list: List of movie URLs
        """
        movie_links = []
        
        # Try multiple potential selectors for movie links
        link_selectors = [
            # Very broad selector
            'a[href^="/title/tt"]'
            # Newer IMDb class selectors
            'div.lister-item-content h3.lister-item-header a',
            'div.sc-afe43def-1 a.sc-afe43def-2',
            
            # More generic selectors
            'h3.lister-item-header a',
            'div.lister-item a[href^="/title/"]',
            'a.lister-item-header-link',
            
        ]
        
        # Try each selector until we find links
        for selector in link_selectors:
            movie_title_links = soup.select(selector)
            
            if movie_title_links:
                for link in movie_title_links:
                    href = link.get('href', '')
                    if href and href.startswith('/title/'):
                        # Construct full URL
                        full_url = f"https://www.imdb.com{href}"
                        if full_url not in movie_links:
                            movie_links.append(full_url)
                
                # If we found links, break the loop
                if movie_links:
                    logging.info(f"Found {len(movie_links)} movie links using selector: {selector}")
                    break
        
        # If no links found, print debug information
        if not movie_links:
            logging.warning("No movie links found. Printing page content for debugging:")
            logging.warning(soup.prettify()[:1000])  # Print first 1000 characters for debugging
        
        return movie_links

    def get_movie_details(self, movie_url):
        """
        Scrape detailed information for a specific movie.
        
        Args:
            movie_url (str): URL of the movie page on IMDb
            
        Returns:
            dict: Dictionary containing movie details
        """
        try:
            response = requests.get(movie_url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            movie_data = {}
            
            # Extract movie title
            try:
                title_wrapper = soup.find('h1')
                movie_data['title'] = title_wrapper.text.strip() if title_wrapper else 'Unknown Title'
            except Exception as e:
                logging.error(f"Error extracting title: {e}")
                movie_data['title'] = 'Unknown Title'
            
            # Extract year
            try:
                year_elem = soup.find('span', class_='sc-8c396aa2-2 jwaBvf')
                if year_elem:
                    year_match = re.search(r'\d{4}', year_elem.text)
                    movie_data['year'] = year_match.group() if year_match else 'Unknown'
            except Exception as e:
                logging.error(f"Error extracting year: {e}")
                movie_data['year'] = 'Unknown'
            
            # Extract genres
            try:
                genres = soup.find_all('a', class_='sc-16ede01-3 bYNgQ ipc-chip ipc-chip--on-baseAlt')
                movie_data['genres'] = [genre.text.strip() for genre in genres] if genres else []
            except Exception as e:
                logging.error(f"Error extracting genres: {e}")
                movie_data['genres'] = []
            
            # Extract budget and box office info
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
                                    movie_data['domestic_gross'] = value
            except Exception as e:
                logging.error(f"Error extracting box office: {e}")
            
            # Extract rating
            try:
                rating_elem = soup.find('span', class_='sc-7ab21ed2-1 jGRxWM')
                movie_data['imdb_rating'] = rating_elem.text.strip() if rating_elem else 'N/A'
            except Exception as e:
                logging.error(f"Error extracting rating: {e}")
                movie_data['imdb_rating'] = 'N/A'
            
            # Extract vote count
            try:
                votes_elem = soup.find('div', class_='sc-7ab21ed2-3 dPVcnq')
                movie_data['vote_count'] = votes_elem.text.strip() if votes_elem else 'N/A'
            except Exception as e:
                logging.error(f"Error extracting vote count: {e}")
                movie_data['vote_count'] = 'N/A'
            
            # Extract runtime
            try:
                runtime_elem = soup.find('div', class_='sc-80d4314-2 iJtmbR')
                movie_data['runtime'] = runtime_elem.text.strip() if runtime_elem else 'N/A'
            except Exception as e:
                logging.error(f"Error extracting runtime: {e}")
                movie_data['runtime'] = 'N/A'
            
            # Extract countries
            try:
                countries = []
                details_section = soup.find_all('div', class_='sc-f65f65be-0 ktSkVi')
                for section in details_section:
                    label = section.find('span', class_='ipc-metadata-list-item__label')
                    if label and 'Countries of origin' in label.text:
                        country_links = section.find_all('a', class_='ipc-metadata-list-item__list-content-item')
                        countries = [country.text.strip() for country in country_links]
                movie_data['countries'] = countries
            except Exception as e:
                logging.error(f"Error extracting countries: {e}")
                movie_data['countries'] = []
            
            # Extract director
            try:
                director_section = soup.find('div', {'data-testid': 'title-pc-wide-screen'})
                if director_section:
                    director_links = director_section.find_all('a', class_='ipc-metadata-list-item__list-content-item')
                    movie_data['directors'] = [director.text.strip() for director in director_links] if director_links else []
            except Exception as e:
                logging.error(f"Error extracting directors: {e}")
                movie_data['directors'] = []
            
            # Add the URL
            movie_data['url'] = movie_url
            
            return movie_data
        
        except requests.RequestException as e:
            logging.error(f"Error scraping {movie_url}: {e}")
            return None

    def scrape_country_films(self, country, start_year=2000, end_year=2025, max_pages=5):
        """
        Parallel scraping of films with improved performance
        
        Args:
            country (str): Country to scrape
            start_year (int): Start year
            end_year (int): End year
            max_pages (int): Maximum pages to scrape
            
        Returns:
            pd.DataFrame: Scraped movie data
        """
        all_movies = []
        
        country_codes = {
            'Kazakhstan': 'kz',
            'South Korea': 'kr'
        }
        
        country_code = country_codes.get(country)
        if not country_code:
            logging.error(f"No country code for {country}")
            return pd.DataFrame()
        
        for page in range(1, max_pages + 1):
            start_index = 1 + (page - 1) * 50
            logging.info(f"Scraping page {page} for {country} films...")
            
            soup = self.get_imdb_search_results(country_code, start_year, end_year, start_index)
            if not soup:
                break
            
            movie_links = self.extract_movie_links(soup)
            if not movie_links:
                logging.warning("No movie links found on this page.")
                break
            
            # Parallel processing of movie details
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all movie link scraping tasks
                future_to_url = {
                    executor.submit(self.get_movie_details, link): link 
                    for link in movie_links
                }
                
                # Process results as they complete
                for future in as_completed(future_to_url):
                    movie_url = future_to_url[future]
                    try:
                        movie_data = future.result()
                        if movie_data:
                            movie_data['country_of_search'] = country
                            all_movies.append(movie_data)
                    except Exception as e:
                        logging.error(f"Error processing {movie_url}: {e}")
            
            # Small pause between pages to be considerate
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
    # Configure scraper with more workers for faster processing
    scraper = IMDbScraper(max_workers=20)  # Adjust based on your system capabilities
    
    # Scrape films for Kazakhstan and South Korea
    kazakhstan_films = scraper.scrape_country_films('Kazakhstan', 2000, 2025, max_pages=5)
    south_korea_films = scraper.scrape_country_films('South Korea', 2000, 2025, max_pages=5)
    
    # Combine datasets
    combined_df = pd.concat([kazakhstan_films, south_korea_films], ignore_index=True)
    combined_df.to_csv('combined_film_data.csv', index=False, encoding='utf-8')
    
    logging.info("Scraping complete. Combined data saved.")

if __name__ == "__main__":
    main()