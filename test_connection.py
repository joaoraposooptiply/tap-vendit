import json
from pathlib import Path
from tap_vendit.client import VenditStream, VenditAuthenticator
from tap_vendit.tap import TapVendit

def test_products_connection():
    # Load config and secrets
    with open("config.json") as f:
        config = json.load(f)
    
    with open("secrets.json") as f:
        secrets = json.load(f)
    
    # Combine configs
    config.update(secrets)
    
    # Initialize tap
    tap = TapVendit(config=config)
    
    # Get products stream
    products_stream = tap.streams["products"]
    
    # Make a request
    response = products_stream.requests.get(products_stream.url_base + products_stream.path)
    
    print(f"Status Code: {response.status_code}")
    print(f"Response Headers: {response.headers}")
    print(f"Response Body: {response.text[:500]}...")  # Print first 500 chars of response

if __name__ == "__main__":
    test_products_connection() 