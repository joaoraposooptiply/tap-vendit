import json
import requests
import time
import os
import csv
from datetime import datetime, timedelta
from typing import List, Dict, Any
from tap_vendit.tap import TapVendit
from tap_vendit.streams import ProductsFindStream, ProductsGetMultipleStream

# Load config and secrets
config_path = os.path.abspath('config.json')
print(f"Reading config from: {config_path}")
with open(config_path, 'r') as f:
    config = json.load(f)
print(f"Initial api_url: {config['api_url']}")  # Debug print
print(f"Loaded config: {config}")  # Debug print
with open('secrets.json', 'r') as f:
    secrets = json.load(f)
config.update(secrets)

# Token handling
token = secrets.get('token')
token_expire = secrets.get('token_expire', 0)

# If token is missing or expired, fetch a new one
if not token or time.time() >= token_expire:
    token_url = "https://oauth.staging.vendit.online/Api/GetToken"
    print(f"Requesting token from: {token_url}")
    print(f"Token request data: apiKey={config['api_key']}, username={config['username']}, password=***")
    token_resp = requests.post(token_url, data={
        "apiKey": config["api_key"],
        "username": config["username"],
        "password": config["password"]
    }, verify=False)
    print(f"Token response status: {token_resp.status_code}")
    print(f"Token response body: {token_resp.text}")
    token_resp.raise_for_status()
    token_data = token_resp.json()
    token = token_data["token"]
    token_expire = token_data["expire"]
    # Save token to secrets.json
    secrets['token'] = token
    secrets['token_expire'] = token_expire
    with open('secrets.json', 'w') as f:
        json.dump(secrets, f, indent=4)
    print(f"Token saved to secrets.json")

# Make the Products/Find request
find_url = f"{config['api_url'].rstrip('/')}/VenditPublicApi/Products/Find"
print(f"Using API URL: {config['api_url']}")  # Debug print
print(f"Full config before request: {config}")  # Debug print
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Token": token,
    "ApiKey": config["api_key"]
}
body = {
    "fieldFilters": [
        {
            "field": 524,
            "value": config["start_date"],
            "value2": config["end_date"],
            "filterComparison": 12
        }
    ],
    "paginationOffset": 0,
    "operator": 0
}
print(f"\nPOST {find_url}")
print(f"Headers: {headers}")
print(f"Body: {json.dumps(body)}")
resp = requests.post(find_url, headers=headers, json=body, verify=False)
print("Status:", resp.status_code)
print("Response:", resp.text)

# Call GetMultiple with the results array
if resp.status_code == 200:
    results = resp.json().get("results", [])
    if results:
        get_multiple_url = f"{config['api_url'].rstrip('/')}/VenditPublicApi/Products/GetMultiple"
        get_multiple_body = {"primaryKeys": results}
        print(f"\nPOST {get_multiple_url}")
        print(f"Headers: {headers}")
        print(f"Body: {json.dumps(get_multiple_body)}")
        get_multiple_resp = requests.post(get_multiple_url, headers=headers, json=get_multiple_body, verify=False)
        print("Status:", get_multiple_resp.status_code)
        print("Response:", get_multiple_resp.text)
        
        # Save response to CSV
        if get_multiple_resp.status_code == 200:
            response_data = get_multiple_resp.json()
            items = response_data.get('items', [])
            
            if items:
                # Define CSV fields
                fields = [
                    'productId', 'productNumber', 'productType', 'productDescription', 
                    'productSubdescription', 'productSize', 'productColor', 'salesUnitQuantity',
                    'lastModified', 'creationDatetime'
                ]
                
                # Create CSV file
                csv_filename = 'products_response.csv'
                with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fields)
                    writer.writeheader()
                    for item in items:
                        # Only include the fields we want
                        row = {field: item.get(field, '') for field in fields}
                        writer.writerow(row)
                
                print(f"\nResponse saved to {csv_filename}")
                print(f"Total products saved: {len(items)}")

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
        page_size = 500  # Maximum page size for Find endpoint
        
        while True:
            response = self.session.post(
                f"{self.base_url}/VenditPublicApi/Products/Find",
                json={
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
            )
            
            if response.status_code == 401:
                # Token might have expired, try refreshing
                self._get_token()
                continue
                
            if response.status_code != 200:
                print(f"Error fetching products: {response.status_code}")
                print(response.text)
                break
                
            data = response.json()
            if not data or not data.get("results"):  # No more results
                break
                
            # Extract IDs from the response
            ids = data["results"]
            all_ids.extend(ids)
            
            if len(ids) < page_size:  # Last page
                break
                
            offset += page_size
            
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
            else:
                print('Warning: Unexpected response format from GetMultiple:', products)
            
        return all_products

    def get_all_products(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Get all products for the given date range."""
        # First get all IDs
        print(f"Fetching all product IDs from {start_date} to {end_date}...")
        all_ids = self.get_all_product_ids(start_date, end_date)
        print(f"Found {len(all_ids)} products")
        
        # Then get all products in chunks
        print("Fetching product details...")
        all_products = self.get_products_in_chunks(all_ids)
        print(f"Retrieved {len(all_products)} product details")
        
        return all_products

def main():
    # Load config and secrets
    config_path = os.path.abspath('config.json')
    print(f"Reading config from: {config_path}")
    with open(config_path, 'r') as f:
        config = json.load(f)
    print(f"Initial api_url: {config['api_url']}")
    print(f"Loaded config: {config}")
    
    # Initialize API client
    api = VenditAPI(config)
    
    # Get all products
    products = api.get_all_products(config['start_date'], config['end_date'])
    
    # Save to CSV
    if products:
        csv_file = 'products_response.csv'
        # Collect all unique fieldnames from all products
        all_fieldnames = set()
        for prod in products:
            all_fieldnames.update(prod.keys())
        all_fieldnames = list(all_fieldnames)
        with open(csv_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=all_fieldnames)
            writer.writeheader()
            writer.writerows(products)
        print(f"Saved {len(products)} products to {csv_file}")
    
    # Save to JSON
    if products:
        json_file = 'products_response.json'
        with open(json_file, 'w') as f:
            json.dump(products, f, indent=2)
        print(f"Saved {len(products)} products to {json_file}")

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