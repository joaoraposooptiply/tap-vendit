"""Test script for the Vendit Suppliers API."""

import json
import requests
import time
import os
import csv
from datetime import datetime, timedelta
from typing import List, Dict, Any
from tap_vendit.tap import TapVendit
from tap_vendit.streams import SuppliersFindStream, SuppliersGetMultipleStream
from tap_vendit.auth import VenditAuthenticator, TokenRefreshError, EmptyResponseError

class TestVenditSuppliersAPI:
    def __init__(self, config_path: str = "config.json"):
        """Initialize the test API client.
        
        Args:
            config_path: Path to the config file containing credentials
        """
        # Get absolute path to config
        config_path = os.path.abspath(config_path)
        
        # Load config
        with open(config_path) as f:
            self.config = json.load(f)
        
        # Ensure config has the config_file path
        self.config["config_file"] = config_path
            
        # Create a mock stream to use with authenticator
        self.tap = TapVendit(config=self.config)
        self.stream = SuppliersFindStream(tap=self.tap)
        
        # Initialize authenticator
        self.auth = VenditAuthenticator(
            stream=self.stream,
            config_file=config_path
        )
        
        # Set up session
        self.session = requests.Session()
        self.session.verify = False
        
        print("\nInitialized TestVenditSuppliersAPI with config:")
        print(f"API URL: {self.config['api_url']}")
        print(f"Using config file: {config_path}")
        print(f"Date range: {self.config['start_date']} to {self.config['end_date']}")

    def _ensure_authenticated_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make an authenticated request, handling token refresh if needed.
        
        Args:
            method: HTTP method to use
            url: URL to request
            **kwargs: Additional arguments to pass to requests
            
        Returns:
            Response from the API
        """
        # Add auth headers
        headers = kwargs.pop('headers', {})
        headers.update(self.auth.auth_headers)
        kwargs['headers'] = headers
        
        # Make request
        response = self.session.request(method, url, **kwargs)
        
        # Handle 401 by refreshing token and retrying once
        if response.status_code == 401:
            print("Token expired, refreshing...")
            self.auth.update_access_token()
            headers.update(self.auth.auth_headers)
            response = self.session.request(method, url, **kwargs)
            
        return response

    def get_all_supplier_ids(self, start_date: str, end_date: str) -> List[str]:
        """Get all supplier IDs using pagination."""
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
            
            response = self._ensure_authenticated_request(
                'POST',
                f"{self.config['api_url']}/VenditPublicApi/Suppliers/Find",
                json=request_body
            )
            
            if response.status_code != 200:
                print(f"Error fetching suppliers: {response.status_code}")
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

    def get_suppliers_in_chunks(self, supplier_ids: List[str]) -> List[Dict[str, Any]]:
        """Get suppliers in chunks of 100 IDs."""
        all_suppliers = []
        chunk_size = 100  # Maximum IDs per GetMultiple request
        
        for i in range(0, len(supplier_ids), chunk_size):
            chunk = supplier_ids[i:i + chunk_size]
            response = self._ensure_authenticated_request(
                'POST',
                f"{self.config['api_url']}/VenditPublicApi/Suppliers/GetMultiple",
                json={"primaryKeys": chunk}
            )
            
            if response.status_code != 200:
                print(f"Error fetching suppliers chunk: {response.status_code}")
                print(response.text)
                continue
                
            suppliers = response.json()
            # The API returns a dict with an 'items' key
            if isinstance(suppliers, dict) and 'items' in suppliers:
                all_suppliers.extend(suppliers['items'])
                print(f"Retrieved {len(suppliers['items'])} suppliers in current chunk, total so far: {len(all_suppliers)}")
            else:
                print('Warning: Unexpected response format from GetMultiple:', suppliers)
            
        print(f"Retrieved total of {len(all_suppliers)} supplier details")
        return all_suppliers

def main():
    """Run the manual test."""
    # Initialize API client
    api = TestVenditSuppliersAPI()
    
    # Use date range from config
    start_date = api.config.get("start_date")
    end_date = api.config.get("end_date")
    
    if not start_date or not end_date:
        print("Error: start_date and end_date are required in config.json")
        return
    
    try:
        # Get all supplier IDs
        print("\nFetching supplier IDs...")
        supplier_ids = api.get_all_supplier_ids(start_date, end_date)
        
        if not supplier_ids:
            print("No suppliers found in date range")
            return
            
        # Get supplier details
        print("\nFetching supplier details...")
        suppliers = api.get_suppliers_in_chunks(supplier_ids)
        
        # Save results
        print("\nSaving results...")
        
        # Save as JSON
        with open('suppliers_response.json', 'w') as f:
            json.dump(suppliers, f, indent=2)
        print(f"Saved {len(suppliers)} suppliers to suppliers_response.json")
        
        # Save as CSV with dynamic fields
        if suppliers:
            # Get all unique fields from all suppliers
            fieldnames = set()
            for supplier in suppliers:
                fieldnames.update(supplier.keys())
            fieldnames = sorted(list(fieldnames))
            
            with open('suppliers_response.csv', 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(suppliers)
            print(f"Saved {len(suppliers)} suppliers to suppliers_response.csv")
            
    except Exception as e:
        print(f"Error during test: {str(e)}")

if __name__ == "__main__":
    main() 