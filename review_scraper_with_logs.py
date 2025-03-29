import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Configure logging with DEBUG level for detailed output
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s: %(message)s')

class IMDbReviewScraper:
    def __init__(self, max_workers=10, timeout=10):
        self.max_workers = max_workers
        self.timeout = timeout
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept-Language': 'en-US,en;q=0.9'
        }

    def setup_driver(self):
        """Set up Selenium WebDriver with enhanced options."""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920x1080")
        chrome_options.add_argument(f"user-agent={self.headers['User-Agent']}")
        chrome_options.add_argument("accept-language=en-US,en;q=0.9")
        chrome_options.add_argument("accept=text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
        service = Service(r'/usr/local/bin/chromedriver')
        return webdriver.Chrome(service=service, options=chrome_options)

    def get_review_page(self, url):
        """Fetch review page with improved loading."""
        driver = self.setup_driver()
        review_url = f"{url}reviews/?sort=num_votes%2Cdesc"
        
        try:
            logging.debug(f"Loading review page: {review_url}")
            driver.get(review_url)
            time.sleep(5)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, 'body'))
            )
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            
            page_source = driver.page_source
            logging.debug(f"Page source length: {len(page_source)} characters")
            soup = BeautifulSoup(page_source, 'html.parser')
            return soup
        except Exception as e:
            logging.error(f"Error loading review page {review_url}: {e}")
            return None
        finally:
            driver.quit()

    def extract_total_reviews(self, soup):
        """Extract the total number of reviews from the page."""
        total_reviews_elem = soup.find('div', {'data-testid': 'tturv-total-reviews'})
        logging.debug(f"Total reviews element: {total_reviews_elem}")
        if total_reviews_elem:
            total_text = total_reviews_elem.text.strip()
            num_reviews = ''.join(filter(str.isdigit, total_text))
            logging.debug(f"Extracted total reviews text: '{total_text}', parsed number: {num_reviews}")
            return int(num_reviews) if num_reviews else 0
        return 0

    def extract_reviews(self, soup, movie_title, movie_url):
        """Extract review details with detailed logging of all HTML elements."""
        reviews = []
        
        # Log entire soup for reference (limited to first 1000 chars for brevity)
        logging.debug(f"HTML soup for {movie_title} (first 1000 chars): {str(soup)[:1000]}")
        
        # Extract total reviews
        total_reviews = self.extract_total_reviews(soup)
        logging.debug(f"Total reviews for {movie_title}: {total_reviews}")
        
        # Find all review articles
        review_articles = soup.find_all('article', class_='sc-8c92b587-1')
        logging.debug(f"Number of review articles found for {movie_title}: {len(review_articles)}")
        
        if not review_articles:
            logging.info(f"No reviews found for {movie_title}")
            # Log all article tags to see if class has changed
            all_articles = soup.find_all('article')
            logging.debug(f"Total <article> tags found: {len(all_articles)}")
            for i, article in enumerate(all_articles[:5]):  # Limit to 5 for brevity
                logging.debug(f"Article {i}: {str(article)[:500]}")  # First 500 chars
            return reviews, total_reviews

        for i, review in enumerate(review_articles[:25]):  # Limit to 25 most voted reviews
            logging.debug(f"Processing review {i+1} for {movie_title}")
            try:
                # Review score (stars out of 10)
                rating_elem = review.find('span', class_='ipc-rating-star--otherUserAlt')
                logging.debug(f"Rating element: {rating_elem}")
                review_score = rating_elem.find('span', class_='ipc-rating-star--rating').text.strip() if rating_elem else 'N/A'
                logging.debug(f"Review score: {review_score}")

                # Review title
                title_elem = review.find('a', class_='ipc-title-link-wrapper')
                logging.debug(f"Title element: {title_elem}")
                review_title = title_elem.find('h3').text.strip() if title_elem else 'N/A'
                permalink = f"https://www.imdb.com{title_elem['href']}" if title_elem and title_elem.get('href') else movie_url
                logging.debug(f"Review title: {review_title}, Permalink: {permalink}")

                # Review content
                content_elem = review.find('div', class_='ipc-html-content-inner-div')
                logging.debug(f"Content element: {content_elem}")
                review_content = content_elem.text.strip() if content_elem else 'N/A'
                logging.debug(f"Review content (first 200 chars): {review_content[:200]}")

                # Votes
                vote_elem = review.find('span', class_='ipc-voting__label__count--up')
                logging.debug(f"Upvote element: {vote_elem}")
                upvotes = vote_elem.text.strip() if vote_elem else 'N/A'
                downvote_elem = review.find('span', class_='ipc-voting__label__count--down')
                logging.debug(f"Downvote element: {downvote_elem}")
                downvotes = downvote_elem.text.strip() if downvote_elem else 'N/A'
                logging.debug(f"Upvotes: {upvotes}, Downvotes: {downvotes}")

                # Date of publication
                date_elem = review.find('li', class_='review-date')
                logging.debug(f"Date element: {date_elem}")
                review_date = date_elem.text.strip() if date_elem else 'N/A'
                logging.debug(f"Review date: {review_date}")

                review_data = {
                    'movie_title': movie_title,
                    'review_score': review_score,
                    'review_title': review_title,
                    'review_content': review_content,
                    'upvotes': upvotes,
                    'downvotes': downvotes,
                    'date': review_date,
                    'permalink': permalink
                }
                reviews.append(review_data)
            except Exception as e:
                logging.error(f"Error parsing review {i+1} for {movie_title}: {e}")
                continue

        logging.info(f"Extracted {len(reviews)} reviews for {movie_title}")
        return reviews, total_reviews

    def scrape_movie_reviews(self, movie_url, movie_title):
        """Scrape reviews for a single movie."""
        soup = self.get_review_page(movie_url)
        if not soup:
            return [], 0
        return self.extract_reviews(soup, movie_title, movie_url)

    def process_movies(self, input_file, output_reviews_file, output_movies_file):
        """Process all movies from the input Excel file and update with total reviews."""
        # Read the Excel file
        df_movies = pd.read_excel(input_file)
        all_reviews = []
        total_reviews_dict = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_movie = {
                executor.submit(self.scrape_movie_reviews, row['url'], row['title']): row['title']
                for _, row in df_movies.iterrows()
            }

            for future in as_completed(future_to_movie):
                movie_title = future_to_movie[future]
                try:
                    reviews, total_reviews = future.result()
                    all_reviews.extend(reviews)
                    total_reviews_dict[movie_title] = total_reviews
                except Exception as e:
                    logging.error(f"Error processing reviews for {movie_title}: {e}")

        # Update original movies DataFrame with total reviews
        df_movies['total_reviews'] = df_movies['title'].map(total_reviews_dict).fillna(0).astype(int)
        df_movies.to_excel(output_movies_file, index=False)
        logging.info(f"Updated {input_file} with total reviews and saved to {output_movies_file}")

        # Save reviews to Excel
        if all_reviews:
            df_reviews = pd.DataFrame(all_reviews)
            df_reviews.to_excel(output_reviews_file, index=False)
            logging.info(f"Saved {len(df_reviews)} reviews to {output_reviews_file}")
        else:
            logging.warning("No reviews found to save.")

def main():
    scraper = IMDbReviewScraper(max_workers=15)  # Increased workers for larger dataset
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Process Kazakhstan films
    scraper.process_movies(
        input_file="tables/kazakhstan_films copy.xlsx",
        output_reviews_file=f"tables/kazakhstan_reviews.xlsx",
        output_movies_file=f"tables/kazakhstan_films copy_updated.xlsx"
    )

if __name__ == "__main__":
    main()