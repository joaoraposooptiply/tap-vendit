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
    "LAST_MODIFIED": 204,  # Products
    "LAST_MODIFIED_ORDERS": 524,  # Orders
    "LAST_MODIFIED_PURCHASE_ORDERS": 524,  # Purchase Orders (same as orders)
    "ORDER_DATE_TIME": 200,  # Purchase Orders orderDateTime
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
        page_count = 0
        
        self.logger.info(f"🔍 Using field {field_id} filter from {start_date.strftime('%Y-%m-%d %H:%M:%S')}")
        
        while True:
            page_count += 1
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
            
            self.logger.debug(f"📄 Fetching page {page_count} (offset: {offset})...")
            response = self._request("POST", url, json=payload)
            data = self._parse_json_response(response, f"finding {self.name} IDs")
            
            ids = data.get("results", [])
            if not ids:
                self.logger.debug(f"📄 Page {page_count}: No results")
                break
                
            all_ids.extend([str(i) for i in ids if i])
            self.logger.debug(f"📄 Page {page_count}: Found {len(ids)} IDs (total: {len(all_ids)})")
            
            if len(ids) < page_size:
                self.logger.debug(f"📄 Page {page_count}: Last page (less than {page_size} results)")
                break
            offset += page_size
        
        self.logger.info(f"📊 Found {len(all_ids)} total IDs across {page_count} pages")
        return all_ids

class BaseOptiplyStream(BaseStream):
    """Base class for Optiply streams with unix timestamp incremental processing."""
    
    replication_key = "unix_timestamp"
    
    def get_starting_unix(self) -> int:
        """Get the starting unix timestamp (Jan 1st, 1970 - Unix epoch)."""
        return 1735693261000 

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
        start_time = time.time()
        self.logger.info(f"🚀 Starting {self.name} sync using Find → GetMultiple pattern...")
        
        start_date = self.get_starting_time(context)
        self.logger.info(f"📅 Sync start date: {start_date.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Step 1: Find IDs
        self.logger.info(f"🔍 Step 1: Finding {self.name} IDs...")
        all_ids = self.get_all_ids_with_filter(
            field_id=FIELD_IDS["LAST_MODIFIED_ORDERS"], 
            start_date=start_date
        )
        
        if not all_ids:
            self.logger.warning(f"⚠️ No {self.name} IDs found")
            return
            
        find_elapsed = time.time() - start_time
        self.logger.info(f"✅ Step 1 completed: Found {len(all_ids)} {self.name} IDs ({find_elapsed:.2f}s)")
        
        # Step 2: Get details in batches
        self.logger.info(f"🔍 Step 2: Getting {self.name} details in batches...")
        total_batches = (len(all_ids) + DEFAULT_BATCH_SIZE - 1) // DEFAULT_BATCH_SIZE
        self.logger.info(f"📊 Processing {len(all_ids)} records in {total_batches} batches (batch size: {DEFAULT_BATCH_SIZE})")
        
        successful_batches = 0
        failed_batches = 0
        total_items = 0
        
        for batch_num, i in enumerate(range(0, len(all_ids), DEFAULT_BATCH_SIZE), 1):
            batch = all_ids[i:i + DEFAULT_BATCH_SIZE]
            batch_size = len(batch)
            
            self.logger.info(f"📦 Processing batch {batch_num}/{total_batches} ({batch_size} items)...")
            
            url = f"{self.config['api_url']}{self.path}"
            response = self._request("POST", url, json={"primaryKeys": batch})
            
            if response.status_code != 200:
                self.logger.error(f"❌ Failed to fetch {self.name} batch {batch_num}: HTTP {response.status_code}")
                failed_batches += 1
                continue
                
            data = self._parse_json_response(response, f"fetching {self.name} batch {batch_num}")
            items = data.get("items", [])
            total_items += len(items)
            
            self.logger.info(f"✅ Batch {batch_num}: Retrieved {len(items)} items")
            successful_batches += 1
            
            for item in items:
                yield item
        
        total_elapsed = time.time() - start_time
        self.logger.info(f"🎉 {self.name} sync completed!")
        self.logger.info(f"📊 Final Summary:")
        self.logger.info(f"   • Total IDs found: {len(all_ids)}")
        self.logger.info(f"   • Successful batches: {successful_batches}/{total_batches}")
        self.logger.info(f"   • Failed batches: {failed_batches}")
        self.logger.info(f"   • Total items retrieved: {total_items}")
        self.logger.info(f"   • Total time: {total_elapsed:.2f}s")
        self.logger.info(f"   • Average time per batch: {total_elapsed/total_batches:.3f}s")

class BaseFindGetWithDetailsStream(BaseFindStream):
    """Base class for streams that use Find → GetWithDetails pattern."""
    
    def get_records(self, context: Optional[Dict]) -> Iterable[Dict[str, Any]]:
        """Get records using Find → GetWithDetails pattern."""
        start_time = time.time()
        self.logger.info(f"🚀 Starting {self.name} sync using Find → GetWithDetails pattern...")
        
        start_date = self.get_starting_time(context)
        self.logger.info(f"📅 Sync start date: {start_date.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Step 1: Find IDs
        self.logger.info(f"🔍 Step 1: Finding {self.name} IDs...")
        all_ids = self.get_all_ids_with_filter(
            field_id=FIELD_IDS["LAST_MODIFIED"], 
            start_date=start_date
        )
        
        if not all_ids:
            self.logger.warning(f"⚠️ No {self.name} IDs found")
            return
            
        find_elapsed = time.time() - start_time
        self.logger.info(f"✅ Step 1 completed: Found {len(all_ids)} {self.name} IDs ({find_elapsed:.2f}s)")
        
        # Step 2: Get details
        self.logger.info(f"🔍 Step 2: Getting {self.name} details...")
        self.logger.info(f"📊 Processing {len(all_ids)} records...")
        
        successful = 0
        failed = 0
        
        for i, item_id in enumerate(all_ids):
            if (i + 1) % 50 == 0 or (i + 1) == len(all_ids):
                progress = ((i + 1) / len(all_ids)) * 100
                self.logger.info(f"🔄 Progress: {i + 1}/{len(all_ids)} ({progress:.1f}%) - Success: {successful}, Failed: {failed}")
            
            url = f"{self.config['api_url']}{self.path}/{item_id}"
            response = self._request("GET", url)
            
            if response.status_code != 200:
                self.logger.error(f"❌ Failed to fetch {self.name} {item_id}: HTTP {response.status_code}")
                failed += 1
                continue
                
            data = self._parse_json_response(response, f"fetching {self.name} {item_id}")
            if data:
                successful += 1
                yield data
            else:
                failed += 1
        
        total_elapsed = time.time() - start_time
        self.logger.info(f"🎉 {self.name} sync completed!")
        self.logger.info(f"📊 Final Summary:")
        self.logger.info(f"   • Total IDs found: {len(all_ids)}")
        self.logger.info(f"   • Successfully processed: {successful}")
        self.logger.info(f"   • Failed requests: {failed}")
        self.logger.info(f"   • Total time: {total_elapsed:.2f}s")
        self.logger.info(f"   • Average time per record: {total_elapsed/len(all_ids):.3f}s")

# Schema loading helper
def load_schema(filename: str) -> Dict[str, Any]:
    """Load schema from JSON file."""
    return read_json_file(os.path.join(SCHEMAS_DIR, filename))

# Stream implementations
class ProductsStream(BaseFindGetMultipleStream):
    """Products stream using Find → GetMultiple pattern."""
    name = "products"
    primary_keys = ["productId"]
    replication_key = "lastModified"
    records_jsonpath = "$.items[*]"
    schema = load_schema("product.json")

    @property
    def path(self):
        return "/VenditPublicApi/Products/GetMultiple"

    def get_records(self, context: Optional[Dict]) -> Iterable[Dict[str, Any]]:
        """Override to use correct field ID for products."""
        self.logger.info(f"Step 1: Finding {self.name} IDs...")
        start_date = self.get_starting_time(context)
        
        # Use the correct Find endpoint for products
        find_url = f"{self.config['api_url']}/VenditPublicApi/Products/Find"
        all_ids = []
        offset = 0
        
        while True:
            payload = {
                "fieldFilters": [
                    {
                        "field": FIELD_IDS["LAST_MODIFIED_ORDERS"],
                        "value": start_date.strftime("%Y-%m-%dT%H:%M:%S.000"),
                        "filterComparison": FILTER_COMPARISONS["GREATER_THAN_OR_EQUAL"]
                    }
                ],
                "paginationOffset": offset,
                "operator": 0
            }
            
            response = self._request("POST", find_url, json=payload)
            data = self._parse_json_response(response, "finding product IDs")
            
            ids = data.get("results", [])
            if not ids:
                break
                
            all_ids.extend([str(i) for i in ids if i])
            if len(ids) < DEFAULT_PAGE_SIZE:
                break
            offset += DEFAULT_PAGE_SIZE
            
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
    records_jsonpath = "$"
    schema = load_schema("order.json")

    @property
    def path(self):
        return "/VenditPublicApi/Orders/GetWithDetails"

    def get_records(self, context: Optional[Dict]) -> Iterable[Dict[str, Any]]:
        """Override to use correct field ID for orders."""
        self.logger.info(f"Step 1: Finding {self.name} IDs...")
        start_date = self.get_starting_time(context)
        
        # Use the correct Find endpoint for orders
        find_url = f"{self.config['api_url']}/VenditPublicApi/Orders/Find"
        all_ids = []
        offset = 0
        
        while True:
            payload = {
                "fieldFilters": [
                    {
                        "field": FIELD_IDS["LAST_MODIFIED_ORDERS"],
                        "value": start_date.strftime("%Y-%m-%dT%H:%M:%S.000"),
                        "filterComparison": FILTER_COMPARISONS["GREATER_THAN_OR_EQUAL"]
                    }
                ],
                "paginationOffset": offset,
                "paginationLimit": DEFAULT_PAGE_SIZE,
                "operator": 0
            }
            
            response = self._request("POST", find_url, json=payload)
            data = self._parse_json_response(response, "finding order IDs")
            
            ids = data.get("results", [])
            if not ids:
                break
                
            all_ids.extend([str(i) for i in ids if i])
            if len(ids) < DEFAULT_PAGE_SIZE:
                break
            offset += DEFAULT_PAGE_SIZE
            
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
        """Override to use correct field ID for purchase orders."""
        self.logger.info("Step 1: Finding purchase order IDs...")
        start_date = self.get_starting_time(context)
        
        # Use the correct Find endpoint for purchase orders with orderDateTime field
        find_url = f"{self.config['api_url']}/VenditPublicApi/PurchaseOrders/Find"
        all_ids = []
        offset = 0
        
        while True:
            payload = {
                "fieldFilters": [
                    {
                        "field": FIELD_IDS["ORDER_DATE_TIME"],
                        "value": start_date.strftime("%Y-%m-%dT%H:%M:%S.000"),
                        "filterComparison": FILTER_COMPARISONS["GREATER_THAN_OR_EQUAL"]
                    }
                ],
                "paginationOffset": offset,
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


class StockChangesStream(BaseOptiplyStream):
    """Stream for stock changes using Optiply endpoint."""
    name = "stock_changes"
    primary_keys = ["productStockId"]
    schema = load_schema("stock_changes.json")

    def get_url(self, unix_ms: int) -> str:
        return f"{self.config['api_url']}/VenditPublicApi/ProductStock/GetChangedStockFromDate/{unix_ms}"


class PrePurchaseOrdersStream(BaseStream):
    """Pre Purchase Orders stream using GetAll endpoint."""
    name = "pre_purchase_orders"
    primary_keys = ["productPurchaseOrderId"]
    replication_key = None  # No replication key for GetAll
    records_jsonpath = "$.items[*]"
    schema = load_schema("purchase_order.json")

    @property
    def path(self):
        return "/VenditPublicApi/PrePurchaseOrders/GetAll"

    def get_records(self, context: Optional[Dict]) -> Iterable[Dict[str, Any]]:
        """Get all pre purchase orders using GetAll endpoint."""
        start_time = time.time()
        self.logger.info("🚀 Starting PrePurchaseOrders sync...")
        self.logger.info(f"📡 Endpoint: {self.config['api_url']}{self.path}")
        
        url = f"{self.config['api_url']}{self.path}"
        self.logger.info("⏳ Making API request...")
        
        response = self._request("GET", url)
        
        if response.status_code != 200:
            self.logger.error(f"❌ Error fetching pre purchase orders: {response.status_code}")
            self.logger.error(f"Response: {response.text}")
            return
        
        self.logger.info("✅ API request successful")
        data = self._parse_json_response(response, "fetching pre purchase orders")
        items = data.get("items", [])
        
        elapsed = time.time() - start_time
        self.logger.info(f"📊 Retrieved {len(items)} pre purchase orders in {elapsed:.2f}s")
        self.logger.info(f"📈 Processing {len(items)} records...")
        
        processed = 0
        for item in items:
            processed += 1
            if processed % 100 == 0 or processed == len(items):
                self.logger.info(f"🔄 Processed {processed}/{len(items)} records ({(processed/len(items)*100):.1f}%)")
            yield item
        
        total_elapsed = time.time() - start_time
        self.logger.info(f"🎉 PrePurchaseOrders sync completed! Total time: {total_elapsed:.2f}s")
        self.logger.info(f"📊 Final count: {len(items)} records processed")


class HistoryPurchaseOrdersStream(BaseFindGetWithDetailsStream):
    """History Purchase Orders stream using Find → GetWithDetails pattern."""
    name = "history_purchase_orders"
    primary_keys = ["productPurchaseOrderId"]
    replication_key = "custom_sync_date"  # Custom replication key for our own state management
    records_jsonpath = "$"
    schema = load_schema("purchase_order.json")

    @property
    def path(self):
        return "/VenditPublicApi/HistoryPurchaseOrders/GetWithDetails"

    def get_records(self, context: Optional[Dict]) -> Iterable[Dict[str, Any]]:
        """Override to use orderDatetime field (200) for history purchase orders."""
        start_time = time.time()
        self.logger.info("🚀 Starting HistoryPurchaseOrders incremental sync...")
        self.logger.info(f"📡 Find endpoint: {self.config['api_url']}/VenditPublicApi/HistoryPurchaseOrders/Find")
        self.logger.info(f"📡 Details endpoint: {self.config['api_url']}{self.path}")
        
        start_date = self.get_starting_time(context)
        self.logger.info(f"📅 Sync start date: {start_date.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"ℹ️ Using field filtering for incremental sync with custom state management")
        
        # Step 1: Find IDs
        self.logger.info("🔍 Step 1: Finding history purchase order IDs...")
        find_url = f"{self.config['api_url']}/VenditPublicApi/HistoryPurchaseOrders/Find"
        all_ids = []
        offset = 0
        page_count = 0
        
        while True:
            page_count += 1
            payload = {
                "fieldFilters": [
                    {
                        "field": FIELD_IDS["ORDER_DATE_TIME"],  # Field 200 (orderDatetime)
                        "value": start_date.strftime("%Y-%m-%dT%H:%M:%S.000"),
                        "filterComparison": FILTER_COMPARISONS["GREATER_THAN_OR_EQUAL"]
                    }
                ],
                "paginationOffset": offset,
                "operator": 0
            }
            
            self.logger.info(f"📄 Fetching page {page_count} (offset: {offset})...")
            response = self._request("POST", find_url, json=payload)
            data = self._parse_json_response(response, "finding history purchase order IDs")
            
            ids = data.get("results", [])
            if not ids:
                self.logger.info(f"📄 Page {page_count}: No more results")
                break
                
            all_ids.extend([str(i) for i in ids if i])
            self.logger.info(f"📄 Page {page_count}: Found {len(ids)} IDs (total so far: {len(all_ids)})")
            
            if len(ids) < DEFAULT_PAGE_SIZE:
                self.logger.info(f"📄 Page {page_count}: Last page (less than {DEFAULT_PAGE_SIZE} results)")
                break
            offset += DEFAULT_PAGE_SIZE
            
        if not all_ids:
            self.logger.warning("⚠️ No history purchase order IDs found")
            return
            
        find_elapsed = time.time() - start_time
        self.logger.info(f"✅ Step 1 completed: Found {len(all_ids)} IDs in {page_count} pages ({find_elapsed:.2f}s)")
        
        # Step 2: Get details
        self.logger.info("🔍 Step 2: Fetching purchase order details...")
        self.logger.info(f"📊 Processing {len(all_ids)} purchase orders...")
        
        successful = 0
        failed = 0
        latest_date = start_date
        
        for i, po_id in enumerate(all_ids):
            if (i + 1) % 50 == 0 or (i + 1) == len(all_ids):
                progress = ((i + 1) / len(all_ids)) * 100
                self.logger.info(f"🔄 Progress: {i + 1}/{len(all_ids)} ({progress:.1f}%) - Success: {successful}, Failed: {failed}")
            
            url = f"{self.config['api_url']}{self.path}/{po_id}"
            response = self._request("GET", url)
            
            if response.status_code != 200:
                self.logger.error(f"❌ Failed to fetch PO {po_id}: HTTP {response.status_code}")
                failed += 1
                continue
                
            data = self._parse_json_response(response, f"fetching history purchase order {po_id}")
            if data:
                # Add our custom replication key for state management
                data["custom_sync_date"] = datetime.now().isoformat()
                
                # Track the latest date for next sync
                if hasattr(self, '_latest_sync_date'):
                    self._latest_sync_date = max(self._latest_sync_date, datetime.now())
                else:
                    self._latest_sync_date = datetime.now()
                
                successful += 1
                yield data
            else:
                failed += 1
        
        total_elapsed = time.time() - start_time
        self.logger.info(f"🎉 HistoryPurchaseOrders sync completed!")
        self.logger.info(f"📊 Final Summary:")
        self.logger.info(f"   • Total IDs found: {len(all_ids)}")
        self.logger.info(f"   • Successfully processed: {successful}")
        self.logger.info(f"   • Failed requests: {failed}")
        self.logger.info(f"   • Total time: {total_elapsed:.2f}s")
        self.logger.info(f"   • Average time per record: {total_elapsed/len(all_ids):.3f}s")
    
    def get_starting_time(self, context: Optional[dict]) -> datetime:
        """Override to handle our own state management."""
        if context and "replication_key_value" in context:
            # Use our saved state
            saved_date = context["replication_key_value"]
            if isinstance(saved_date, str):
                return datetime.fromisoformat(saved_date)
            elif isinstance(saved_date, datetime):
                return saved_date
        
        # Fallback to config start_date or default
        start_date = self.config.get("start_date")
        if start_date:
            return datetime.fromisoformat(start_date)
        return datetime(1970, 1, 1)
    
    def _increment_stream_state(self, record, context):
        """Override to handle our own state updates."""
        if context and hasattr(self, '_latest_sync_date'):
            context["replication_key_value"] = self._latest_sync_date.isoformat()
