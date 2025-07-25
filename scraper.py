from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import requests
import time
import os
import re
import json
import pandas as pd
from datetime import datetime, date, timedelta

class TikiScraper:
    """
    A web scraper designed to collect product and brand data from Tiki.vn.
    It uses a hybrid approach: Selenium for navigating listing pages and
    direct requests/API calls for fetching detailed information to optimize performance.
    """
    def __init__(self, driver_path):
        """Initializes the scraper with the path to the WebDriver."""
        if not os.path.exists(driver_path):
            raise FileNotFoundError(f"ChromeDriver not found at path: {driver_path}")

        # Configure Selenium WebDriver
        self.service = Service(executable_path=driver_path)
        self.options = webdriver.ChromeOptions()
        self.options.add_argument("start-maximized")
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option('useAutomationExtension', False)

        self.driver = webdriver.Chrome(service=self.service, options=self.options)
        self.wait = WebDriverWait(self.driver, 20)
        self.today = date.today()
        print("TikiScraper initialized.")

    def _get_page_source(self, url):
        """Navigates to a URL using Selenium, scrolls to load all content, and returns the page source."""
        print(f"Navigating to: {url}")
        self.driver.get(url)
        
        # Critical Step: Wait for product items to be present in the DOM
        self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a.product-item")))
        
        # Scroll down to trigger lazy-loading of all elements
        print("Page loaded. Scrolling to load all content...")
        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
        time.sleep(1)
        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        
        return self.driver.page_source

    def _get_data_and_ids_from_next_data(self, product_url):
        """
        Fetches the __NEXT_DATA__ JSON from a product detail page to extract
        the full product data block and essential IDs (product_id, spid, seller_id).
        This method uses `requests` for high performance.
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
            }
            response = requests.get(product_url, headers=headers, timeout=10)
            response.raise_for_status()
    
            soup = BeautifulSoup(response.content, 'html.parser')
            script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
    
            if script_tag:
                json_data = json.loads(script_tag.string)
                
                # Recursively find the data blocks we need
                sold_data = self.find_parent_recursively(json_data, 'quantity_sold')
                product_data = self.find_parent_recursively(json_data, 'sellerId')

                if not product_data:
                    print(f"  -> Could not recursively find a product data block in __NEXT_DATA__ for {product_url}")
                    return None, None

                if isinstance(product_data, dict):
                    quantity_sold = (sold_data or {}).get('quantity_sold', {}).get('value', 0)
                    ids = {
                        'product_id': product_data.get('productId'),
                        'spid': product_data.get('spid'),
                        'seller_id': product_data.get('sellerId'),
                        'quantity_sold': quantity_sold
                    }
            
                    # Verify that all essential IDs were found
                    if not all([ids['product_id'], ids['spid'], ids['seller_id']]):
                        print(f"  -> Missing one or more essential IDs in __NEXT_DATA__ for {product_url}")
                        return None, None
            
                    # Return both the full JSON blob and the extracted IDs
                    return json_data, ids
            else:
                print(f"  -> __NEXT_DATA__ script tag not found on page {product_url}")
                return None, None
        except Exception as e:
            print(f"  -> An unexpected error occurred while processing {product_url}: {e}")
            return None, None

    def _get_brand_details_via_api(self, ids):
        """Fetches detailed brand (seller) data using Tiki's internal API."""
        if not all(ids.get(k) for k in ['seller_id', 'product_id', 'spid']):
            return None
        
        api_url = f"https://api.tiki.vn/product-detail/v2/widgets/seller?seller_id={ids['seller_id']}&mpid={ids['product_id']}&spid={ids['spid']}&trackity_id=0c339b08-95f0-a9ef-8705-0feb40c4d971&platform=desktop&version=3"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}
        
        try:
            response = requests.get(api_url, headers=headers, timeout=5)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"  -> Brand API call failed for seller {ids['seller_id']}. Error: {e}")
            return None

    def find_parent_recursively(self, data_blob, target_key):
        """Recursively searches for a target_key and returns the PARENT DICTIONARY that contains it."""
        if isinstance(data_blob, dict):
            if target_key in data_blob:
                return data_blob
            for value in data_blob.values():
                found = self.find_parent_recursively(value, target_key)
                if found is not None:
                    return found
        elif isinstance(data_blob, list):
            for item in data_blob:
                found = self.find_parent_recursively(item, target_key)
                if found is not None:
                    return found
        return None

    def scrape(self, base_url, num_pages=1):
        """Orchestrates the entire scraping process."""
        all_product_links = []
        scraped_data_from_list_page = []

        try:
            # --- PHASE 1: Scrape basic product info and all links from listing pages ---
            for page_num in range(1, num_pages + 1):
                url_with_page = f"{base_url}&page={page_num}"
                print(f"\n--- Scraping Page {page_num} for basic info and links ---")
                
                page_source = self._get_page_source(url_with_page)
                soup = BeautifulSoup(page_source, 'html.parser')
                product_items = soup.find_all('a', class_='product-item')
                
                for item in product_items:
                    try:
                        name = item.find('h3', class_='sc-68e86366-8 dDeapS').text.strip() if item.find('h3', class_='sc-68e86366-8 dDeapS') else "N/A"
                        price_str = item.find('div', class_='price-discount__price').text.strip() if item.find('div', class_='price-discount__price') else "0"
                        price = int(re.sub(r'\D', '', price_str))
                        link = 'https://tiki.vn' + item['href']
                        
                        rating_stars = 0.0
                        parent_rating_div = item.find('div', class_='sc-68e86366-6 lbZNwv') 
                        if parent_rating_div:
                            width_div = parent_rating_div.find('div', style=True)
                            if width_div:
                                match = re.search(r'width:\s*(\d+)%', width_div['style'])
                                if match:
                                    rating_stars = round((int(match.group(1)) / 100) * 5, 1)
                        
                        scraped_data_from_list_page.append({
                            'Name': name,
                            'Price': price,
                            'Link': link,
                            'Rating': rating_stars,
                            'ScrapedDate': datetime.now().strftime('%Y-%m-%d')
                        })
                        all_product_links.append(link)
                    except Exception:
                        continue
        finally:
            self.driver.quit()
            print("\nSelenium driver closed. Starting API/Requests phase.")
    
        # --- Data enrichment phase ---
        final_products_history = []
        final_brands_details = {} # Use dict with seller_id as key to avoid duplicates

        print(f"\n--- Fetching detailed data for {len(all_product_links)} products ---")
        for i, link in enumerate(all_product_links):
            print(f"Processing product {i+1}/{len(all_product_links)}: {link[:70]}...")
            
            product_data_json, ids = self._get_data_and_ids_from_next_data(link)
            if not (product_data_json and ids):
                print("  -> Could not get data/IDs. Skipping product.")
                continue

            seller_id = ids.get('seller_id')
            if seller_id:
                brand_json = self._get_brand_details_via_api(ids)
                
                # Enrich the initial scraped data with new info
                current_product_record = scraped_data_from_list_page[i]
                current_product_record['BrandName'] = brand_json['data']['seller'].get('name', 'N/A') if brand_json else 'N/A'
                current_product_record['SoldCount'] = ids['quantity_sold']
                final_products_history.append(current_product_record)
                
                # Process and store brand details if it's a new brand
                if brand_json and 'data' in brand_json and 'seller' in brand_json['data']:
                    if seller_id not in final_brands_details:
                        seller_data = brand_json['data']['seller']
                        join_days = timedelta(days=seller_data.get('days_since_joined', 0))
                        
                        brand_rating, num_rating = 0.0, 0
                        for info_item in seller_data.get('info', []):
                            if info_item.get('type') == 'review':
                                brand_rating = float(info_item.get('title', 0.0))
                                match = re.search(r'(\d+)', info_item.get('sub_title', ''))
                                if match: num_rating = int(match.group(1))
                                break
                    
                        final_brands_details[seller_id] = {
                            'BrandName': seller_data.get('name', 'N/A'),
                            'BrandLink': seller_data.get('url', 'N/A'),
                            'IsOfficial': 1 if seller_data.get('is_official') else 0,
                            'BrandRating': brand_rating,
                            'NumRating': num_rating,
                            'JoinedDate': (self.today - join_days).strftime('%Y-%m-%d'),
                            'LastScrapedDate': datetime.now().strftime('%Y-%m-%d')
                        }
        
        # --- PHASE 4: Prepare and return DataFrames ---
        brands_df = pd.DataFrame(list(final_brands_details.values()))
        history_df = pd.DataFrame(final_products_history)
    
        return brands_df, history_df