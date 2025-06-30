"""Stream type classes for tap-vendit."""

from __future__ import annotations

import typing as t
from importlib import resources
from datetime import datetime
from typing import List, Dict, Any, Optional, Iterable, TYPE_CHECKING
import time
import os
import requests

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers._util import read_json_file

from tap_vendit.client import VenditStream

if TYPE_CHECKING:
    from tap_vendit.tap import TapVendit

# Schema directory constant
SCHEMAS_DIR = os.path.join(os.path.dirname(__file__), "schemas")

# Constants for common field IDs and values
FIELD_IDS = {
    "LAST_MODIFIED": 204,
    "CREATION_DATE": 205,
}

FILTER_COMPARISONS = {
    "GREATER_THAN_OR_EQUAL": 2,
    "LESS_THAN_OR_EQUAL": 3,
}

# Common pagination settings
DEFAULT_PAGE_SIZE = 100
DEFAULT_BATCH_SIZE = 100

class BaseStream(VenditStream):
    """Base stream with DRY incremental and request logic."""
    
    def get_starting_time(self, context: Optional[dict]) -> datetime:
        """Get starting time for incremental sync."""
        replication_key_value = self.get_starting_replication_key_value(context)
        if replication_key_value:
            return datetime.fromisoformat(replication_key_value)
        start_date = self.config.get("start_date")
        if start_date:
            return datetime.fromisoformat(start_date)
        return datetime(1970, 1, 1)

    def _request(self, method, url, **kwargs):
        """Make authenticated request with automatic token refresh."""
        if not self.authenticator.is_token_valid():
            self.logger.info("Token missing or expired, fetching new token...")
            self.authenticator.update_access_token()
        
        headers = kwargs.pop('headers', {})
        headers.update(self.authenticator.auth_headers)
        kwargs['headers'] = headers
        self.logger.debug(f"Request headers: {headers}")
        
        response = self.session.request(method, url, **kwargs)
        
        # If 401, refresh token and retry once
        if response.status_code == 401:
            self.logger.warning("401 Unauthorized received, refreshing token and retrying request...")
            self.authenticator.update_access_token()
            headers = self.authenticator.auth_headers
            kwargs['headers'] = headers
            response = self.session.request(method, url, **kwargs)
        
        return response

    def _parse_json_response(self, response: requests.Response, context: str = "") -> Dict[str, Any]:
        """Parse JSON response with consistent error handling."""
        try:
            return response.json()
        except Exception as e:
            self.logger.error(f"Failed to parse JSON response {context}: {response.status_code}")
            self.logger.error(f"Response text: {response.text}")
            raise

class BaseFindStream(BaseStream):
    """Base class for Find streams that only return IDs."""
    
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
    ).to_dict()
    
    def get_all_ids_with_filter(self, field_id: int, start_date: datetime, page_size: int = DEFAULT_PAGE_SIZE) -> List[str]:
        """Get all IDs using field filter with pagination."""
        all_ids = []
        offset = 0
        
        while True:
            payload = {
                "fieldFilters": [
                    {
                        "field": field_id,
                        "value": start_date.strftime("%Y-%m-%dT%H:%M:%S.000"),
                        "filterComparison": FILTER_COMPARISONS["GREATER_THAN_OR_EQUAL"]
                    }
                ],
                "paginationOffset": offset,
                "operator": 0
            }
            
            url = f"{self.config['api_url']}{self.path}"
            response = self._request("POST", url, json=payload)
            data = self._parse_json_response(response, f"finding {self.name} IDs")
            
            ids = data.get("results", [])
            if not ids:
                break
                
            all_ids.extend([str(i) for i in ids if i])
            if len(ids) < page_size:
                break
            offset += page_size
            
        return all_ids

class BaseOptiplyStream(BaseStream):
    """Base class for Optiply streams with unix timestamp incremental processing."""
    
    replication_key = "unix_timestamp"
    
    def get_starting_unix(self) -> int:
        """Get the starting unix timestamp (Jan 1st, 2022)."""
        return 1640995200000

    def get_current_unix(self) -> int:
        """Get current unix timestamp in milliseconds."""
        return int(time.time() * 1000)

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Get records using unix timestamp incremental sync."""
        state = context or {}
        last_synced_unix = state.get("replication_key_value")
        
        if last_synced_unix is None:
            last_synced_unix = self.get_starting_unix()
            self.logger.info(f"First run: using default start unix {last_synced_unix}")
        else:
            self.logger.info(f"Incremental run: using saved unix {last_synced_unix}")
        
        url = self.get_url(last_synced_unix)
        self.logger.info(f"Fetching data from {url}")
        
        response = self.session.get(url, headers=self.authenticator.auth_headers)
        if response.status_code != 200:
            self.logger.error(f"Error fetching data: {response.status_code}")
            self.logger.error(response.text)
            return
        
        data = self._parse_json_response(response, f"fetching {self.name}")
        items = data.get("items", [])
        self.logger.info(f"Retrieved {len(items)} records")
        
        for item in items:
            # Add unix timestamp to the record
            record = dict(item)
            record["unix_timestamp"] = last_synced_unix
            yield record
        
        # Save current unix timestamp for next run
        current_unix = self.get_current_unix()
        self.logger.info(f"Current unix timestamp for next run: {current_unix}")
        
        # Update state for next run
        if context is not None:
            context["replication_key_value"] = current_unix

    def get_url(self, unix_ms: int) -> str:
        """Get URL for the Optiply endpoint. Override in subclasses."""
        raise NotImplementedError("Subclasses must implement get_url")

class BaseFindGetMultipleStream(BaseFindStream):
    """Base class for streams that use Find → GetMultiple pattern."""
    
    def get_records(self, context: Optional[Dict]) -> Iterable[Dict[str, Any]]:
        """Get records using Find → GetMultiple pattern."""
        self.logger.info(f"Step 1: Finding {self.name} IDs...")
        start_date = self.get_starting_time(context)
        
        # Get IDs using the Find endpoint
        all_ids = self.get_all_ids_with_filter(
            field_id=FIELD_IDS["LAST_MODIFIED"], 
            start_date=start_date
        )
        
        if not all_ids:
            self.logger.warning(f"No {self.name} IDs found")
            return
            
        self.logger.info(f"Found {len(all_ids)} {self.name} IDs")
        self.logger.info("Step 2: Getting details...")
        
        # Get details in batches
        for i in range(0, len(all_ids), DEFAULT_BATCH_SIZE):
            batch = all_ids[i:i + DEFAULT_BATCH_SIZE]
            url = f"{self.config['api_url']}{self.path}"
            response = self._request("POST", url, json={"primaryKeys": batch})
            
            if response.status_code != 200:
                self.logger.error(f"Error fetching {self.name} batch: {response.status_code}")
                continue
                
            data = self._parse_json_response(response, f"fetching {self.name} batch")
            for item in data.get("items", []):
                yield item

class BaseFindGetWithDetailsStream(BaseFindStream):
    """Base class for streams that use Find → GetWithDetails pattern."""
    
    def get_records(self, context: Optional[Dict]) -> Iterable[Dict[str, Any]]:
        """Get records using Find → GetWithDetails pattern."""
        self.logger.info(f"Step 1: Finding {self.name} IDs...")
        start_date = self.get_starting_time(context)
        
        # Get IDs using the Find endpoint
        all_ids = self.get_all_ids_with_filter(
            field_id=FIELD_IDS["LAST_MODIFIED"], 
            start_date=start_date
        )
        
        if not all_ids:
            self.logger.warning(f"No {self.name} IDs found")
            return
            
        self.logger.info(f"Found {len(all_ids)} {self.name} IDs")
        self.logger.info("Step 2: Getting details...")
        
        # Get individual details
        for item_id in all_ids:
            url = f"{self.config['api_url']}{self.path}/{item_id}"
            response = self._request("GET", url)
            
            if response.status_code != 200:
                self.logger.error(f"Error fetching {self.name} {item_id}: {response.status_code}")
                continue
                
            data = self._parse_json_response(response, f"fetching {self.name} {item_id}")
            if data:
                yield data

# Schema loading helper
def load_schema(filename: str) -> Dict[str, Any]:
    """Load schema from JSON file."""
    return read_json_file(os.path.join(SCHEMAS_DIR, filename))

# Stream implementations
class ProductsStream(BaseFindGetMultipleStream):
    """Products stream using Find → GetMultiple pattern."""
    name = "products"
    primary_keys = ["productId"]
    replication_key = "LastModified"
    records_jsonpath = "$.items[*]"
    schema = load_schema("product.json")

    @property
    def path(self):
        return "/VenditPublicApi/Products/GetMultiple"

class SuppliersStream(BaseFindGetMultipleStream):
    """Suppliers stream using Find → GetMultiple pattern."""
    name = "suppliers"
    primary_keys = ["supplierId"]
    replication_key = None
    records_jsonpath = "$.items[*]"
    schema = load_schema("supplier.json")

    @property
    def path(self):
        return "/VenditPublicApi/Suppliers/GetMultiple"

    def get_records(self, context: Optional[Dict]) -> Iterable[Dict[str, Any]]:
        """Override to use GetAllIds instead of Find for suppliers."""
        self.logger.info("Step 1: Getting all supplier IDs...")
        
        # Use GetAllIds endpoint instead of Find
        get_all_ids_url = f"{self.config['api_url']}/VenditPublicApi/Suppliers/GetAllIds"
        response = self._request("GET", get_all_ids_url)
        data = self._parse_json_response(response, "getting supplier IDs")
        
        supplier_ids = [str(i) for i in data if i]
        if not supplier_ids:
            self.logger.warning("No supplier IDs found")
            return
            
        self.logger.info(f"Found {len(supplier_ids)} supplier IDs")
        self.logger.info("Step 2: Getting supplier details...")
        
        # Get details in batches
        for i in range(0, len(supplier_ids), DEFAULT_BATCH_SIZE):
            batch = supplier_ids[i:i + DEFAULT_BATCH_SIZE]
            url = f"{self.config['api_url']}{self.path}"
            response = self._request("POST", url, json={"primaryKeys": batch})
            
            if response.status_code != 200:
                self.logger.error(f"Error fetching suppliers batch: {response.status_code}")
                continue
                
            data = self._parse_json_response(response, "fetching suppliers batch")
            for item in data.get("items", []):
                yield item

class OrdersStream(BaseFindGetWithDetailsStream):
    """Orders stream using Find → GetWithDetails pattern."""
    name = "orders"
    primary_keys = ["customerOrderHeaderId"]
    replication_key = "LastModified"
    records_jsonpath = "$"
    schema = load_schema("order.json")

    @property
    def path(self):
        return "/VenditPublicApi/Orders/GetWithDetails"

class PurchaseOrdersStream(BaseFindGetWithDetailsStream):
    """Purchase Orders stream using Find → GetWithDetails pattern."""
    name = "purchase_orders"
    primary_keys = ["productPurchaseOrderId"]
    replication_key = None
    records_jsonpath = "$"
    schema = load_schema("purchase_order.json")

    @property
    def path(self):
        return "/VenditPublicApi/PurchaseOrders/GetWithDetails"

    def get_records(self, context: Optional[Dict]) -> Iterable[Dict[str, Any]]:
        """Override to handle date range filtering for purchase orders."""
        self.logger.info("Step 1: Finding purchase order IDs...")
        
        start_date = self.config.get("start_date")
        end_date = self.config.get("end_date")
        if not start_date or not end_date:
            self.logger.error("start_date and end_date are required in config")
            return
            
        # Use date range filtering
        find_url = f"{self.config['api_url']}/VenditPublicApi/PurchaseOrders/Find"
        all_ids = []
        offset = 0
        
        while True:
            payload = {
                "fieldFilters": [
                    {
                        "field": FIELD_IDS["LAST_MODIFIED"],
                        "value": start_date,
                        "filterComparison": FILTER_COMPARISONS["GREATER_THAN_OR_EQUAL"]
                    }
                ],
                "paginationOffset": offset,
                "paginationLimit": DEFAULT_PAGE_SIZE,
                "operator": 0
            }
            
            response = self._request("POST", find_url, json=payload)
            data = self._parse_json_response(response, "finding purchase order IDs")
            
            ids = data.get("results", [])
            if not ids:
                break
                
            all_ids.extend([str(i) for i in ids if i])
            if len(ids) < DEFAULT_PAGE_SIZE:
                break
            offset += DEFAULT_PAGE_SIZE
            
        if not all_ids:
            self.logger.warning("No purchase order IDs found")
            return
            
        self.logger.info(f"Found {len(all_ids)} purchase order IDs")
        self.logger.info("Step 2: Getting purchase order details...")
        
        # Get individual details
        for po_id in all_ids:
            url = f"{self.config['api_url']}{self.path}/{po_id}"
            response = self._request("GET", url)
            
            if response.status_code != 200:
                self.logger.error(f"Error fetching purchase order {po_id}: {response.status_code}")
                continue
                
            data = self._parse_json_response(response, f"fetching purchase order {po_id}")
            if data:
                yield data

class SupplierProductsStream(BaseOptiplyStream):
    """Stream for supplier-product relationships using Optiply endpoint."""
    name = "supplier_products"
    primary_keys = ["productSupplierId"]
    schema = load_schema("supplier_product.json")

    def get_url(self, unix_ms: int) -> str:
        return f"{self.config['api_url']}/Optiply/GetProductSuppliersFromDate/{unix_ms}"

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Override to handle the flattened productPurchasePrice."""
        state = context or {}
        last_synced_unix = state.get("replication_key_value")
        
        if last_synced_unix is None:
            last_synced_unix = self.get_starting_unix()
            self.logger.info(f"First run: using default start unix {last_synced_unix}")
        else:
            self.logger.info(f"Incremental run: using saved unix {last_synced_unix}")
        
        url = self.get_url(last_synced_unix)
        self.logger.info(f"Fetching supplier products from {url}")
        
        response = self.session.get(url, headers=self.authenticator.auth_headers)
        if response.status_code != 200:
            self.logger.error(f"Error fetching supplier products: {response.status_code}")
            self.logger.error(response.text)
            return
        
        data = self._parse_json_response(response, "fetching supplier products")
        items = data.get("items", [])
        self.logger.info(f"Retrieved {len(items)} supplier-product relationships")
        
        for item in items:
            # Flatten productPurchasePrice
            ppp = item.get("productPurchasePrice", {}) or {}
            record = dict(item)
            record["productPurchasePriceId"] = ppp.get("productPurchasePriceId")
            record["purchasePriceEx"] = ppp.get("purchasePriceEx")
            record["unix_timestamp"] = last_synced_unix
            yield record
        
        # Save current unix timestamp for next run
        current_unix = self.get_current_unix()
        self.logger.info(f"Current unix timestamp for next run: {current_unix}")
        
        # Update state for next run
        if context is not None:
            context["replication_key_value"] = current_unix

class PurchaseOrdersOptiplyStream(BaseOptiplyStream):
    """Stream for purchase orders using Optiply endpoint."""
    name = "purchase_orders_optiply"
    primary_keys = ["productPurchaseOrderId"]
    schema = load_schema("purchase_order_optiply.json")

    def __init__(self, tap: "TapVendit"):
        super().__init__(tap)
        self.path = "/Optiply/GetProductPurchaseOrdersFromDate"

    def get_url(self, unix_ms: int) -> str:
        return f"{self.config['api_url']}{self.path}/{unix_ms}"

class OrdersOptiplyStream(BaseOptiplyStream):
    """Stream for orders using Optiply endpoint."""
    name = "orders_optiply"
    primary_keys = ["customerOrderHeaderId"]
    schema = load_schema("order_optiply.json")

    def __init__(self, tap: "TapVendit"):
        super().__init__(tap)
        self.path = "/Optiply/GetOrdersFromDate"

    def get_url(self, unix_ms: int) -> str:
        return f"{self.config['api_url']}{self.path}/{unix_ms}/true"
