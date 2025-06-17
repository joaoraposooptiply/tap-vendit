import json
import requests
import time
import os
import csv
from datetime import datetime, timedelta
from typing import List, Dict, Any
from tap_vendit.tap import TapVendit
from tap_vendit.streams import ProductsFindStream, ProductsGetMultipleStream

class VenditAPI:
    def __init__(self, config: Dict[str, Any]):
        self.base_url = config['api_url']
        self.api_key = config['api_key']
        self.username = config['username']
        self.password = config['password']
        self.verify_ssl = False
        self.session = requests.Session()
        self.session.verify = self.verify_ssl
        self.token = None
        self.token_expire = 0
        self._get_token()

    def _get_token(self):
        """Get a new token."""
        token_url = "https://oauth.staging.vendit.online/Api/GetToken"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "ApiKey": self.api_key
        }
        data = {
            "apiKey": self.api_key,
            "username": self.username,
            "password": self.password
        }
        response = self.session.post(token_url, data=data, headers=headers)
        response.raise_for_status()
        token_data = response.json()
        self.token = token_data["token"]
        self.token_expire = token_data["expire"]
        # Update session headers for all future requests
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Token": self.token,
            "ApiKey": self.api_key
        })

    def _ensure_valid_token(self):
        """Ensure we have a valid token, refreshing if necessary."""
        if not self.token or time.time() >= self.token_expire:
            self._get_token()

    def get_all_product_ids(self, start_date: str, end_date: str) -> List[str]:
        """Get all product IDs using pagination."""
        self._ensure_valid_token()
        all_ids = []
        offset = 0
        page_size = 100  # API always returns 100 results per page
        total_pages = 0
        
        print(f"\nStarting pagination with page size {page_size}")
        while True:
            print(f"\nFetching page {total_pages + 1} (offset: {offset})...")
            request_body = {
                "fieldFilters": [
                    {
                        "field": 524,
                        "value": start_date,
                        "value2": end_date,
                        "filterComparison": 12
                    }
                ],
                "paginationOffset": offset,
                "operator": 0
            }
            print(f"Request body: {json.dumps(request_body, indent=2)}")
            
            response = self.session.post(
                f"{self.base_url}/VenditPublicApi/Products/Find",
                json=request_body
            )
            
            if response.status_code == 401:
                print("Token expired, refreshing...")
                self._get_token()
                continue
                
            if response.status_code != 200:
                print(f"Error fetching products: {response.status_code}")
                print(response.text)
                break
                
            data = response.json()
            print(f"Response data: {json.dumps(data, indent=2)}")
            
            if not data or not data.get("results"):  # No more results
                print("No more results found")
                break
                
            # Extract IDs from the response
            ids = data["results"]
            all_ids.extend(ids)
            total_pages += 1
            print(f"Retrieved {len(ids)} IDs in current page")
            print(f"Total IDs so far: {len(all_ids)}")
            
            # Check if we've received fewer results than the page size
            if len(ids) < page_size:  # Last page
                print("Received fewer results than page size, this is the last page")
                break
                
            # Increment offset for next page
            offset += page_size
            
        print(f"\nPagination complete:")
        print(f"Total pages processed: {total_pages}")
        print(f"Total IDs found: {len(all_ids)}")
        return all_ids

    def get_products_in_chunks(self, product_ids: List[str]) -> List[Dict[str, Any]]:
        """Get products in chunks of 100 IDs."""
        self._ensure_valid_token()
        all_products = []
        chunk_size = 100  # Maximum IDs per GetMultiple request
        
        for i in range(0, len(product_ids), chunk_size):
            chunk = product_ids[i:i + chunk_size]
            response = self.session.post(
                f"{self.base_url}/VenditPublicApi/Products/GetMultiple",
                json={"primaryKeys": chunk}
            )
            
            if response.status_code == 401:
                # Token might have expired, try refreshing
                self._get_token()
                continue
                
            if response.status_code != 200:
                print(f"Error fetching products chunk: {response.status_code}")
                print(response.text)
                continue
                
            products = response.json()
            # The API returns a dict with an 'items' key
            if isinstance(products, dict) and 'items' in products:
                all_products.extend(products['items'])
                print(f"Retrieved {len(products['items'])} products in current chunk, total so far: {len(all_products)}")
            else:
                print('Warning: Unexpected response format from GetMultiple:', products)
            
        print(f"Retrieved total of {len(all_products)} product details")
        return all_products

    def get_all_products(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Get all products for the given date range."""
        # First get all IDs
        print(f"Fetching all product IDs from {start_date} to {end_date}...")
        all_ids = self.get_all_product_ids(start_date, end_date)
        
        if not all_ids:
            print("No product IDs found")
            return []
            
        # Then get all products in chunks
        print("Fetching product details...")
        return self.get_products_in_chunks(all_ids)

# Load config and secrets
config_path = os.path.abspath('config.json')
print(f"Reading config from: {config_path}")
with open(config_path, 'r') as f:
    config = json.load(f)
print(f"Initial api_url: {config['api_url']}")
print(f"Loaded config: {config}")

# Initialize the API client
api = VenditAPI(config)

# Get all products
print("Fetching all products...")
products = api.get_all_products(config['start_date'], config['end_date'])
print(f"Retrieved {len(products)} products")

if products:
    # Collect all unique fieldnames from all products
    all_fieldnames = set()
    for prod in products:
        all_fieldnames.update(prod.keys())
    all_fieldnames = list(all_fieldnames)
    
    # Save to CSV (append if file exists, write header only if new)
    csv_filename = 'products_response.csv'
    file_exists = os.path.isfile(csv_filename)
    with open(csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=all_fieldnames)
        if not file_exists:
            writer.writeheader()
        for product in products:
            writer.writerow(product)
    
    print(f"\nAppended {len(products)} products to {csv_filename}")
    
    # Save to JSON (overwrite as before)
    json_filename = 'products_response.json'
    with open(json_filename, 'w', encoding='utf-8') as jsonfile:
        json.dump(products, jsonfile, indent=2)
    
    print(f"Saved {len(products)} products to {json_filename}")

def main():
    # Load config and secrets
    config_path = os.path.abspath('config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Initialize the API client
    api = VenditAPI(config)
    
    # Get all products
    products = api.get_all_products(config['start_date'], config['end_date'])
    
    if products:
        # Collect all unique fieldnames from all products
        all_fieldnames = set()
        for prod in products:
            all_fieldnames.update(prod.keys())
        all_fieldnames = list(all_fieldnames)
        
        # Save to CSV (append if file exists, write header only if new)
        csv_filename = 'products_response.csv'
        file_exists = os.path.isfile(csv_filename)
        with open(csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=all_fieldnames)
            if not file_exists:
                writer.writeheader()
            for product in products:
                writer.writerow(product)
        
        print(f"\nAppended {len(products)} products to {csv_filename}")
        
        # Save to JSON (overwrite as before)
        json_filename = 'products_response.json'
        with open(json_filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(products, jsonfile, indent=2)
        
        print(f"Saved {len(products)} products to {json_filename}")

if __name__ == "__main__":
    main()

# Initialize tap with config
tap = TapVendit(config=config)

# Step 1: Find product IDs
find_stream = ProductsFindStream(tap)
product_ids = []
print("Finding product IDs...")
for record in find_stream.get_records(context=None):
    if isinstance(record, dict) and "id" in record:
        product_ids.append(record["id"])
    elif isinstance(record, int):
        product_ids.append(record)
print(f"Found {len(product_ids)} product IDs.")

# Step 2: Get product details for those IDs
if product_ids:
    get_multiple_stream = ProductsGetMultipleStream(tap)
    print("Fetching product details for found IDs...")
    for record in get_multiple_stream.get_records(context={"product_ids": product_ids[:50]}):  # limit to 50 for demo
        print(json.dumps(record, indent=2))
else:
    print("No product IDs found.") 