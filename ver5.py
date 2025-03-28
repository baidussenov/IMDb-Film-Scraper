import os
import re
import time
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from tqdm import tqdm

class IMDBScraper:
    def __init__(self, base_url, max_pages=5, max_threads=8):
        self.base_url = base_url
        self.max_pages = max_pages
        self.max_threads = max_threads
        self.movie_details = []
        self.driver_path = r'C:\chromedriver.exe'

    def setup_driver(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920x1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        service = Service(self.driver_path)
        driver = webdriver.Chrome(service=service, options=options)
        return driver

    def get_movie_links(self):
        driver = self.setup_driver()
        movie_links = set()
        try:
            for page in range(1, self.max_pages + 1):
                driver.get(f"{self.base_url}&page={page}")
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "h3.lister-item-header a"))
                )
                soup = BeautifulSoup(driver.page_source, "html.parser")
                for link in soup.select("h3.lister-item-header a"):
                    movie_links.add("https://www.imdb.com" + link["href"].split("?")[0])
        finally:
            driver.quit()
        return list(movie_links)

    def get_movie_details(self, url):
        driver = self.setup_driver()
        movie_data = {}
        try:
            driver.get(url)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            soup = BeautifulSoup(driver.page_source, "html.parser")

            title_tag = soup.find("h1")
            movie_data["Title"] = title_tag.text.strip() if title_tag else "N/A"

            year_tag = soup.find("a", href=re.compile("/year/"))
            movie_data["Year"] = year_tag.text.strip() if year_tag else "N/A"

            rating_tag = soup.find("span", class_="sc-bde20123-1 cMEQkK dUdcBf")
            movie_data["Rating"] = rating_tag.text.strip() if rating_tag else "N/A"

            genre_tags = soup.find_all("span", class_="ipc-chip__text")
            movie_data["Genres"] = ", ".join([g.text for g in genre_tags]) if genre_tags else "N/A"
        
            box_office_section = soup.find("section", {'data-testid': 'BoxOffice'})
            if box_office_section:
                for item in box_office_section.find_all('li'):
                    label = item.find('span', string=re.compile("Budget|Gross", re.I))
                    value = item.find('span', class_="ipc-metadata-list-item__list-content-item")
                    if label and value:
                        movie_data[label.get_text().strip()] = value.get_text().strip()
        
            self.movie_details.append(movie_data)
        finally:
            driver.quit()

    def scrape_movies(self):
        movie_links = self.get_movie_links()
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = {executor.submit(self.get_movie_details, link): link for link in movie_links}
            for future in tqdm(as_completed(futures), total=len(movie_links), desc="Scraping Movies"):
                future.result()
        self.save_to_csv()

    def save_to_csv(self, filename="imdb_movies.csv"):
        df = pd.DataFrame(self.movie_details)
        df.to_csv(filename, index=False)
        print(f"Data saved to {filename}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scraper = IMDBScraper("https://www.imdb.com/search/title/?genres=drama", max_pages=5, max_threads=8)
    scraper.scrape_movies()
