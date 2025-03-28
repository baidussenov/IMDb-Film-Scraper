from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from ver2 import IMDbScraper
import time

def scrape_imdb_with_load_more(url, max_clicks=3, wait_time=2):
    # Set up Selenium WebDriver
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in headless mode
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")
    
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"

    chrome_options.add_argument(f"user-agent={user_agent}")
    chrome_options.add_argument("Accept-Language': 'en-US,en;q=0.9")
 
    service = Service(r'C:\chromedriver.exe')
    driver = webdriver.Chrome(service=service)

    # Open IMDb search page
    driver.get(url)
    time.sleep(5)  # Let page load

    for i in range(max_clicks):
        try:
            #  scroll to the bottom of the page
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(5)  # Allow time for new movies to load
            
            load_more_button = WebDriverWait(driver, wait_time).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "button.ipc-btn.ipc-btn--single-padding.ipc-btn--center-align-content.ipc-btn--default-height.ipc-btn--core-base.ipc-btn--theme-base.ipc-btn--button-radius.ipc-btn--on-accent2.ipc-text-button.ipc-see-more__button"))
            )
            driver.execute_script("arguments[0].click();", load_more_button)
            time.sleep(2)  # Allow time for new movies to load
            print(f"Clicked Load More {i+1} times")
        except Exception as e:
            print(f"No more 'Load More' button found or all results loaded. Error: {e}")
            break

    # Get page source and parse with BeautifulSoup
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    driver.quit()  # Close browser

    return soup

# Example IMDb search URL
imdb_url = "https://www.imdb.com/search/title/?title_type=feature&release_date=2000-01-01,2024-12-31&countries=KZ&sort=year,asc"

soup = scrape_imdb_with_load_more(imdb_url)

# Extract movie links using your existing function
movie_links = IMDbScraper().extract_movie_links(soup)
print("Extracted movie links:", movie_links)
