from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

chrome_options = Options()
chrome_options.add_argument("--headless")  # Run in headless mode

service = Service(r'C:\chromedriver.exe')  # Remove if added to PATH
driver = webdriver.Chrome(service=service, options=chrome_options)

driver.get("https://www.google.com")
print("Title:", driver.title)
driver.quit()
