"""Stream type classes for tap-vendit."""

from __future__ import annotations

import typing as t
from importlib import resources
from datetime import datetime
from typing import List, Dict, Any, Optional, Iterable, TYPE_CHECKING
import time
import os
import requests
import backoff

# from singer_sdk import typing as th  # No longer needed with dynamic schemas
# from singer_sdk.helpers._util import read_json_file  # No longer needed

from tap_vendit.client import VenditStream

if TYPE_CHECKING:
    from tap_vendit.tap import TapVendit

# Schema directory constant - no longer needed with dynamic schemas
# SCHEMAS_DIR = os.path.join(os.path.dirname(__file__), "schemas")

# Constants for common field IDs and values
FIELD_IDS = {
    "LAST_MODIFIED": 204,  # Products
    "LAST_MODIFIED_ORDERS": 524,  # Orders
    "LAST_MODIFIED_PURCHASE_ORDERS": 524,  # Purchase Orders (same as orders)
    "ORDER_DATE_TIME": 200,  # Purchase Orders orderDateTime
    "DELIVERY_DATE_TIME": 401,  # HistoryPurchaseOrders deliveryDateTime
    "CREATION_DATE": 205,
    "TRANSACTION_DATETIME": 802,  # Transactions transactionDatetime
    "TRANSACTION_FILTER_154": 154,  # Additional filter field for transactions
}

# Filter comparison values per Vendit API docs
FILTER_COMPARISONS = {
    "EQUALS": 0,
    "NOT_EQUALS": 1,
    "GREATER_THAN": 2,
    "LESS_THAN": 3,
    "GREATER_OR_EQUAL": 4,
    "LESS_OR_EQUAL": 5,
    "IS_NOT_NULL": 8,  # Used for transactions field 154 filter
}

# Common pagination settings
DEFAULT_PAGE_SIZE = 100
DEFAULT_BATCH_SIZE = 100
DEFAULT_CONNECTION_POOL_SIZE = 10
DEFAULT_MAX_RETRIES = 3

class BaseStream(VenditStream):
    """Base stream with DRY incremental and request logic."""
    
    @property
    def schema(self):
        """Return minimal schema for dynamic field discovery."""
        return {
            "type": "object",
            "properties": {},
            "additionalProperties": True
        }
    
    def get_starting_time(self, context: Optional[dict]) -> datetime:
        """Get starting time for incremental sync."""
        replication_key_value = self.get_starting_replication_key_value(context)
        if replication_key_value:
            # Handle ISO format with 'Z' timezone suffix
            if isinstance(replication_key_value, str):
                if replication_key_value.endswith('Z'):
                    replication_key_value = replication_key_value[:-1] + '+00:00'
                try:
                    return datetime.fromisoformat(replication_key_value)
                except ValueError:
                    # Fallback to parsing without timezone info
                    return datetime.fromisoformat(replication_key_value.replace('Z', ''))
            elif isinstance(replication_key_value, datetime):
                return replication_key_value
        start_date = self.config.get("start_date")
        if start_date:
            # Handle ISO format with 'Z' timezone suffix
            if start_date.endswith('Z'):
                start_date = start_date[:-1] + '+00:00'
            try:
                return datetime.fromisoformat(start_date)
            except ValueError:
                # Fallback to parsing without timezone info
                return datetime.fromisoformat(start_date.replace('Z', ''))
        return datetime(1970, 1, 1)

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException, requests.exceptions.HTTPError),
        max_tries=3,
        factor=2,
        jitter=backoff.random_jitter
    )
    def _request(self, method, url, **kwargs):
        """Make authenticated request with automatic token refresh and retry logic."""
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
            response.raise_for_status()  # Raise an exception for bad status codes
            return response.json()
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error {context}: {response.status_code} - {e}")
            self.logger.error(f"Response text: {response.text}")
            raise
        except requests.exceptions.JSONDecodeError as e:
            self.logger.error(f"JSON decode error {context}: {e}")
            self.logger.error(f"Response text: {response.text}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error parsing response {context}: {e}")
            self.logger.error(f"Response status: {response.status_code}")
            self.logger.error(f"Response text: {response.text}")
            raise

class BaseFindStream(BaseStream):
    """Base class for Find streams that only return IDs."""
    
    # No schema - dynamic field discovery
    
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
                        "filterComparison": FILTER_COMPARISONS["GREATER_OR_EQUAL"]
                    }
                ],
                "paginationOffset": offset,
                "paginationLimit": page_size,
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
        return 1 

    def get_current_unix(self) -> int:
        """Get current unix timestamp in milliseconds."""
        return int(time.time() * 1000)

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Get records using unix timestamp incremental sync."""
        # Use the Singer SDK's state management
        last_synced_unix = self.get_starting_replication_key_value(context)
        
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
        
        # Save current unix timestamp for next run
        current_unix = self.get_current_unix()
        self.logger.info(f"Current unix timestamp for next run: {current_unix}")
        
        for item in items:
            # Add unix timestamp to the record for state management
            record = dict(item)
            record["unix_timestamp"] = current_unix
            yield record

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

# Schema loading helper - no longer needed with dynamic schemas
# def load_schema(filename: str) -> Dict[str, Any]:
#     """Load schema from JSON file."""
#     return read_json_file(os.path.join(SCHEMAS_DIR, filename))

# Stream implementations
class ProductsStream(BaseFindGetMultipleStream):
    """Products stream using Find → GetMultiple pattern."""
    name = "products"
    primary_keys = ["productId"]
    replication_key = "lastModified"
    records_jsonpath = "$.items[*]"
    
    @property
    def schema(self):
        """Return schema specific to products data."""
        return {
            "type": "object",
            "properties": {
                "productId": {"type": ["integer", "null"]},
                "productNumber": {"type": ["string", "null"]},
                "productType": {"type": ["string", "null"]},
                "productDescription": {"type": ["string", "null"]},
                "productSubdescription": {"type": ["string", "null"]},
                "additionalInfo": {"type": ["string", "null"]},
                "useStock": {"type": ["boolean", "null"]},
                "salesVisibilityId": {"type": ["integer", "null"]},
                "availabilityStatusId": {"type": ["integer", "null"]},
                "creationDatetime": {"type": ["string", "null"], "format": "date-time"},
                "lastModified": {"type": ["string", "null"], "format": "date-time"},
                "productGuid": {"type": ["string", "null"]},
                "memoCommon": {"type": ["string", "null"]},
                "memoEcommerce": {"type": ["string", "null"]},
                "productSize": {"type": ["string", "null"]},
                "productColor": {"type": ["string", "null"]},
                "salesUnitQuantity": {"type": ["number", "null"]},
                "hintInfo": {"type": ["string", "null"]},
                "productSearchCode": {"type": ["string", "null"]},
                "groupId": {"type": ["integer", "null"]},
                "brandId": {"type": ["integer", "null"]},
                "brancheId": {"type": ["integer", "null"]},
                "productKindId": {"type": ["integer", "null"]},
                "productLabelLayoutId": {"type": ["integer", "null"]},
                "cabProfileCode": {"type": ["integer", "null"]},
                "allowInvoiceDiscount": {"type": ["boolean", "null"]},
                "useGroupInvoiceDiscount": {"type": ["boolean", "null"]},
                "isBaseProduct": {"type": ["boolean", "null"]},
                "deliveryFromWarehouse": {"type": ["integer", "null"]},
                "purchaseFromWarehouse": {"type": ["integer", "null"]},
                "viaCollectionWarehouse": {"type": ["integer", "null"]},
                "bebat": {"type": ["integer", "null"]},
                "extraCostQuantity": {"type": ["integer", "null"]},
                "mintatonPosDealId": {"type": ["string", "null"]},
                "assortmentCode": {"type": ["string", "null"]},
                "modifiedBy": {"type": ["string", "null"]},
                "originCountryCode": {"type": ["string", "null"]},
                "originCountry": {"type": ["string", "null"]},
                "suppliers": {"type": ["object", "null"]},
                "salesPrices": {"type": ["object", "null"]},
                "productVats": {"type": ["object", "null"]},
                "availableStock": {"type": ["object", "null"]}
            },
            "additionalProperties": True
        }

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
                        "filterComparison": FILTER_COMPARISONS["GREATER_OR_EQUAL"]
                    }
                ],
                "paginationOffset": offset,
                "paginationLimit": DEFAULT_PAGE_SIZE,
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
    
    @property
    def schema(self):
        """Return schema specific to suppliers data."""
        return {
            "type": "object",
            "properties": {
                "supplierId": {"type": ["integer", "null"]},
                "supplierName": {"type": ["string", "null"]},
                "supplierNumber": {"type": ["string", "null"]},
                "supplierEmail": {"type": ["string", "null"]},
                "supplierWebsite": {"type": ["string", "null"]},
                "supplierBankAccount": {"type": ["string", "null"]},
                "supplierInfo": {"type": ["string", "null"]},
                "supplierDebtNumber": {"type": ["string", "null"]},
                "orderCommunicationServiceId": {"type": ["integer", "null"]},
                "orderLayoutId": {"type": ["integer", "null"]},
                "orderEmailAttachmentFilename": {"type": ["string", "null"]},
                "orderEmailTo": {"type": ["string", "null"]},
                "orderEmailToCc": {"type": ["string", "null"]},
                "stockinfoCommunicationServiceId": {"type": ["integer", "null"]},
                "stockinfoUrl": {"type": ["string", "null"]},
                "deliveryCommunicationServiceId": {"type": ["integer", "null"]},
                "stockLookupTypeEnum": {"type": ["integer", "null"]},
                "supplierGroupBitvalue": {"type": ["integer", "null"]},
                "extraInfo": {"type": ["string", "null"]},
                "orderCommunicationServiceLayoutId": {"type": ["integer", "null"]},
                "orderFtpServer": {"type": ["string", "null"]},
                "orderFtpPort": {"type": ["integer", "null"]},
                "orderFtpPath": {"type": ["string", "null"]},
                "orderFtpFilename": {"type": ["string", "null"]},
                "orderFtpPassiveMode": {"type": ["boolean", "null"]},
                "orderFtpSftp": {"type": ["boolean", "null"]},
                "orderEmailSubject": {"type": ["string", "null"]},
                "orderEmailAsAttachment": {"type": ["boolean", "null"]},
                "orderLocalFilePath": {"type": ["string", "null"]},
                "orderLocalFileFilename": {"type": ["string", "null"]},
                "glnNumber": {"type": ["string", "null"]},
                "supplierImageUrl": {"type": ["string", "null"]},
                "minimumOrderAmount": {"type": ["integer", "null"]},
                "extraCosts": {"type": ["number", "null"]},
                "productPriceCommunicationServiceId": {"type": ["integer", "null"]},
                "deliveryAutoAddProductSupplier": {"type": ["boolean", "null"]},
                "vatNumber": {"type": ["string", "null"]},
                "deliveryDays": {"type": ["integer", "null"]},
                "supplierGuid": {"type": ["string", "null"]},
                "orderFtpTimeout": {"type": ["integer", "null"]},
                "bicAndSwift": {"type": ["string", "null"]},
                "banknummerAndIBAN": {"type": ["string", "null"]}
            },
            "additionalProperties": True
        }

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
    
    @property
    def schema(self):
        """Return schema specific to customer orders data."""
        return {
            "type": "object",
            "properties": {
                "customerOrderHeaderId": {"type": ["integer", "null"]},
                "orderDetails": {"type": ["array", "null"]},
                "customerId": {"type": ["integer", "null"]},
                "planningId": {"type": ["integer", "null"]},
                "customerOrderNumber": {"type": ["string", "null"]},
                "orderTypeId": {"type": ["integer", "null"]},
                "orderPriorityId": {"type": ["integer", "null"]},
                "deliveryDate": {"type": ["string", "null"], "format": "date-time"},
                "activationDate": {"type": ["string", "null"], "format": "date-time"},
                "orderStatusId": {"type": ["integer", "null"]},
                "completeDelivery": {"type": ["boolean", "null"]},
                "deliveryNotificationType": {"type": ["integer", "null"]},
                "invoiceAddressId": {"type": ["integer", "null"]},
                "invoiceContactId": {"type": ["integer", "null"]},
                "deliveryAddressId": {"type": ["integer", "null"]},
                "deliveryContactId": {"type": ["integer", "null"]},
                "journalId": {"type": ["integer", "null"]},
                "stockStatusEnum": {"type": ["integer", "null"]},
                "saleExVat": {"type": ["number", "null"]},
                "invoiceDiscountPercentage": {"type": ["number", "null"]},
                "invoiceDiscountAmount": {"type": ["number", "null"]},
                "discountIsPercentage": {"type": ["boolean", "null"]},
                "baselineReferenceNumber": {"type": ["string", "null"]},
                "orderLabelNumber": {"type": ["string", "null"]},
                "orderStatusDate": {"type": ["string", "null"], "format": "date-time"},
                "orderStatusDateFormat": {"type": ["string", "null"]},
                "orderSubTitle": {"type": ["string", "null"]},
                "telecomReferenceNumber": {"type": ["string", "null"]},
                "originalWorkstationId": {"type": ["integer", "null"]},
                "originalDrawerId": {"type": ["integer", "null"]},
                "internalMemo": {"type": ["string", "null"]},
                "statusEmployeeId": {"type": ["integer", "null"]},
                "url": {"type": ["string", "null"]},
                "dropshipmentType": {"type": ["integer", "null"]},
                "purchaseOrderNumber": {"type": ["string", "null"]},
                "officeId": {"type": ["integer", "null"]},
                "employeeId": {"type": ["integer", "null"]},
                "creationDatetime": {"type": ["string", "null"], "format": "date-time"},
                "ecommerceWebsiteGuid": {"type": ["string", "null"]},
                "isEcommerce": {"type": ["boolean", "null"]},
                "invoiceDebitCustomerId": {"type": ["integer", "null"]}
            },
            "additionalProperties": True
        }

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
                        "filterComparison": FILTER_COMPARISONS["GREATER_OR_EQUAL"]
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
    # No schema - dynamic field discovery

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
                        "filterComparison": FILTER_COMPARISONS["GREATER_OR_EQUAL"]
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
    
    @property
    def schema(self):
        """Return schema specific to supplier products data."""
        return {
            "type": "object",
            "properties": {
                "productSupplierId": {"type": ["integer", "null"]},
                "productId": {"type": ["integer", "null"]},
                "supplierId": {"type": ["integer", "null"]},
                "supplierProductNumber": {"type": ["string", "null"]},
                "productNumber": {"type": ["string", "null"]},
                "productType": {"type": ["string", "null"]},
                "productDescription": {"type": ["string", "null"]},
                "productSubdescription": {"type": ["string", "null"]},
                "productExtraInfo": {"type": ["string", "null"]},
                "amount": {"type": ["number", "null"]},
                "purchasePriceEx": {"type": ["number", "null"]},
                "minOrderQuantity": {"type": ["number", "null"]},
                "extraPriceInfo": {"type": ["string", "null"]},
                "creationDatetime": {"type": ["string", "null"], "format": "date-time"},
                "optiplyId": {"type": ["integer", "string", "null"]},
                "productPreorderId": {"type": ["integer", "null"]},
                "isManual": {"type": ["boolean", "null"]},
                "officeId": {"type": ["integer", "null"]},
                "employeeId": {"type": ["integer", "null"]},
                "targetSupplierId": {"type": ["integer", "null"]},
                "productPurchaseHeaderId": {"type": ["integer", "null"]},
                "deliveryDatetime": {"type": ["string", "null"], "format": "date-time"},
                "deliveryDocumentNumber": {"type": ["string", "null"]},
                "details": {"type": ["array", "null"]},
                "lastModified": {"type": ["string", "null"], "format": "date-time"},
                "unix_timestamp": {"type": ["integer", "null"]},
                "preferredDefaultSupplier": {"type": ["boolean", "null"]},
                "recommendedSalesPriceInc": {"type": ["number", "null"]},
                "availabilityStatusId": {"type": ["integer", "null"]},
                "supplierStock2": {"type": ["string", "null"]},
                "productPurchasePrice": {"type": ["object", "null"]},
                "productPurchasePriceId": {"type": ["integer", "null"]},
                "expectedArrivalDatetime": {"type": ["string", "null"], "format": "date-time"}
            },
            "additionalProperties": True
        }

    def get_url(self, unix_ms: int) -> str:
        return f"{self.config['api_url']}/Optiply/GetProductSuppliersFromDate/{unix_ms}"

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Override to handle the flattened productPurchasePrice."""
        # Get records from parent class (which handles incremental sync)
        for record in super().get_records(context):
            if isinstance(record, dict):
                # Flatten productPurchasePrice
                ppp = record.get("productPurchasePrice", {}) or {}
                record["productPurchasePriceId"] = ppp.get("productPurchasePriceId")
                record["purchasePriceEx"] = ppp.get("purchasePriceEx")
            yield record
    


class PurchaseOrdersOptiplyStream(BaseOptiplyStream):
    """Stream for purchase orders using Optiply endpoint."""
    name = "purchase_orders_optiply"
    primary_keys = ["productPurchaseOrderId"]
    
    @property
    def schema(self):
        """Return schema specific to purchase orders optiply data."""
        return {
            "type": "object",
            "properties": {
                "productPurchaseOrderId": {"type": ["integer", "null"]},
                "officeId": {"type": ["integer", "null"]},
                "purchaseOrderNumber": {"type": ["string", "null"]},
                "supplierId": {"type": ["integer", "null"]},
                "orderDatetime": {"type": ["string", "null"], "format": "date-time"},
                "orderReference": {"type": ["string", "null"]},
                "preorderEmployeeId": {"type": ["integer", "null"]},
                "employeeId": {"type": ["integer", "null"]},
                "onlineOrderReference": {"type": ["string", "null"]},
                "orderRemark": {"type": ["string", "null"]},
                "optiplyId": {"type": ["string", "null"]},
                "details": {"type": ["array", "null"]},
                "unix_timestamp": {"type": ["integer", "null"]},
            },
            "additionalProperties": True
        }

    def __init__(self, tap: "TapVendit"):
        super().__init__(tap)
        self.path = "/Optiply/GetProductPurchaseOrdersFromDate"

    def get_url(self, unix_ms: int) -> str:
        return f"{self.config['api_url']}{self.path}/{unix_ms}"
    
    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Override get_records to add data cleaning and details field extraction."""
        # Get records from parent class
        for record in super().get_records(context):
            if isinstance(record, dict):
                # Clean empty strings that should be nulls
                for key, value in record.items():
                    if value == "":
                        record[key] = None
                    # Handle literal "string" values that should be null
                    elif key == "optiplyId" and value == "string":
                        record[key] = None
                    # Extract items from details object if it exists
                    elif key == "details" and isinstance(value, dict) and "items" in value:
                        record[key] = value["items"]
                    # Extract items from orderDetails object if it exists
                    elif key == "orderDetails" and isinstance(value, dict) and "items" in value:
                        record[key] = value["items"]
            yield record

class OrdersOptiplyStream(BaseOptiplyStream):
    """Stream for orders using Optiply endpoint."""
    name = "orders_optiply"
    primary_keys = ["customerOrderHeaderId"]
    
    @property
    def schema(self):
        """Return schema specific to orders optiply data."""
        return {
            "type": "object",
            "properties": {
                "customerOrderHeaderId": {"type": ["integer", "null"]},
                "orderDetails": {"type": ["object", "array", "null"]},
                "customerId": {"type": ["integer", "null"]},
                "planningId": {"type": ["integer", "null"]},
                "customerOrderNumber": {"type": ["string", "null"]},
                "orderTypeId": {"type": ["integer", "null"]},
                "orderPriorityId": {"type": ["integer", "null"]},
                "deliveryDate": {"type": ["string", "null"], "format": "date-time"},
                "activationDate": {"type": ["string", "null"], "format": "date-time"},
                "orderStatusId": {"type": ["integer", "null"]},
                "completeDelivery": {"type": ["boolean", "null"]},
                "deliveryNotificationType": {"type": ["integer", "null"]},
                "invoiceAddressId": {"type": ["integer", "null"]},
                "invoiceContactId": {"type": ["integer", "null"]},
                "deliveryAddressId": {"type": ["integer", "null"]},
                "deliveryContactId": {"type": ["integer", "null"]},
                "journalId": {"type": ["integer", "null"]},
                "stockStatusEnum": {"type": ["integer", "null"]},
                "saleExVat": {"type": ["number", "boolean", "null"]},
                "invoiceDiscountPercentage": {"type": ["number", "null"]},
                "invoiceDiscountAmount": {"type": ["number", "null"]},
                "discountIsPercentage": {"type": ["boolean", "null"]},
                "baselineReferenceNumber": {"type": ["string", "null"]},
                "orderLabelNumber": {"type": ["string", "null"]},
                "orderStatusDate": {"type": ["string", "null"], "format": "date-time"},
                "orderStatusDateFormat": {"type": ["string", "null"]},
                "orderSubTitle": {"type": ["string", "null"]},
                "telecomReferenceNumber": {"type": ["string", "null"]},
                "originalWorkstationId": {"type": ["integer", "null"]},
                "originalDrawerId": {"type": ["integer", "null"]},
                "internalMemo": {"type": ["string", "null"]},
                "statusEmployeeId": {"type": ["integer", "null"]},
                "url": {"type": ["string", "null"]},
                "dropshipmentType": {"type": ["integer", "null"]},
                "purchaseOrderNumber": {"type": ["string", "null"]},
                "officeId": {"type": ["integer", "null"]},
                "employeeId": {"type": ["integer", "null"]},
                "creationDatetime": {"type": ["string", "null"], "format": "date-time"},
                "lastModified": {"type": ["string", "null"], "format": "date-time"},
                "unix_timestamp": {"type": ["integer", "null"]},
                "ecommerceWebsiteGuid": {"type": ["string", "null"]},
                "isEcommerce": {"type": ["boolean", "null"]},
                "invoiceDebitCustomerId": {"type": ["integer", "null"]}
            },
            "additionalProperties": True
        }

    def __init__(self, tap: "TapVendit"):
        super().__init__(tap)
        self.path = "/Optiply/GetOrdersFromDate"

    def get_url(self, unix_ms: int) -> str:
        return f"{self.config['api_url']}{self.path}/{unix_ms}/true"
    
    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Override get_records to add data cleaning and orderDetails field extraction."""
        # Get records from parent class
        for record in super().get_records(context):
            if isinstance(record, dict):
                # Clean empty strings that should be nulls
                for key, value in record.items():
                    if value == "":
                        record[key] = None
                    # Handle literal "string" values that should be null
                    elif key == "optiplyId" and value == "string":
                        record[key] = None
                    # Extract items from orderDetails object if it exists
                    elif key == "orderDetails" and isinstance(value, dict) and "items" in value:
                        record[key] = value["items"]
            yield record


class StockChangesStream(BaseOptiplyStream):
    """Stream for stock changes using Optiply endpoint."""
    name = "stock_changes"
    primary_keys = ["productStockId"]
    
    @property
    def schema(self):
        """Return schema specific to stock changes data."""
        return {
            "type": "object",
            "properties": {
                "stock": {"type": ["number", "null"]},
                "availableStock": {"type": ["number", "null"]},
                "locationName": {"type": ["string", "null"]},
                "productStockId": {"type": ["integer", "null"]},
                "stockLocationId": {"type": ["integer", "null"]},
                "storageLocationId": {"type": ["integer", "null"]},
                "officeId": {"type": ["integer", "null"]},
                "productId": {"type": ["integer", "null"]},
                "storageName": {"type": ["string", "null"]},
                "lastModified": {"type": ["string", "null"], "format": "date-time"},
                "unix_timestamp": {"type": ["integer", "null"]}
            },
            "additionalProperties": True
        }

    def get_url(self, unix_ms: int) -> str:
        return f"{self.config['api_url']}/VenditPublicApi/ProductStock/GetChangedStockFromDate/{unix_ms}"


class PrePurchaseOrdersStream(BaseStream):
    """Pre Purchase Orders stream using GetAll endpoint."""
    name = "pre_purchase_orders"
    primary_keys = ["productPreorderId"]
    replication_key = None  # No replication key for GetAll
    records_jsonpath = "$.items[*]"  # Use standard Singer SDK record processing
    
    @property
    def schema(self):
        """Return schema specific to pre purchase orders data."""
        return {
            "type": "object",
            "properties": {
                "productPreorderId": {"type": ["integer", "null"]},
                "isManual": {"type": ["boolean", "null"]},
                "officeId": {"type": ["integer", "null"]},
                "employeeId": {"type": ["integer", "null"]},
                "productId": {"type": ["integer", "null"]},
                "supplierProductNumber": {"type": ["string", "null"]},
                "productNumber": {"type": ["string", "null"]},
                "productType": {"type": ["string", "null"]},
                "productDescription": {"type": ["string", "null"]},
                "productSubdescription": {"type": ["string", "null"]},
                "productExtraInfo": {"type": ["string", "null"]},
                "targetSupplierId": {"type": ["integer", "null"]},
                "targetOfficeId": {"type": ["integer", "null"]},
                "amount": {"type": ["number", "null"]},
                "purchasePriceEx": {"type": ["number", "null"]},
                "orderReference": {"type": ["string", "null"]},
                "minOrderQuantity": {"type": ["number", "null"]},
                "extraPriceInfo": {"type": ["string", "null"]},
                "promotionProductId": {"type": ["integer", "null"]},
                "lineId": {"type": ["string", "null"]},
                "creationDatetime": {"format": "date-time", "type": ["string", "null"]},
                "serialNumber": {"type": ["string", "null"]},
                "frameNumber": {"type": ["string", "null"]},
                "imeiNumber": {"type": ["string", "null"]},
                "certificateNumber": {"type": ["string", "null"]},
                "optiplyId": {"type": ["string", "null"]},
                "brutoPurchasePriceEx": {"type": ["number", "null"]},
                "useFormula": {"type": ["boolean", "null"]}
            },
            "additionalProperties": True
        }

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
        
        # Parse the response manually since the parent parse_response might not work
        data = self._parse_json_response(response, "fetching pre purchase orders")
        items = data.get("items", [])
        
        self.logger.info(f"📊 Retrieved {len(items)} pre purchase orders")
        
        # Clean and yield each record
        for record in items:
            if isinstance(record, dict):
                # Clean empty strings that should be nulls
                for key, value in record.items():
                    if value == "":
                        record[key] = None
                    # Handle literal "string" values that should be null
                    elif key == "optiplyId" and value == "string":
                        record[key] = None
                yield record
        
        total_elapsed = time.time() - start_time
        self.logger.info(f"🎉 PrePurchaseOrders sync completed! Total time: {total_elapsed:.2f}s")


class HistoryPurchaseOrdersStream(BaseFindGetWithDetailsStream):
    """History Purchase Orders stream using Find → GetWithDetails pattern.
    
    Uses deliveryDatetime for incremental sync since that's when purchase orders
    are completed and appear in history.
    """
    name = "history_purchase_orders"
    primary_keys = ["productPurchaseHeaderId"]
    replication_key = "deliveryDatetime"  # Use actual delivery date for incremental sync
    records_jsonpath = "$"
    
    @property
    def schema(self):
        """Return schema specific to history purchase orders data."""
        return {
            "type": "object",
            "properties": {
                "productPurchaseHeaderId": {"type": ["integer", "null"]},
                "officeId": {"type": ["integer", "null"]},
                "supplierId": {"type": ["integer", "null"]},
                "employeeId": {"type": ["integer", "null"]},
                "deliveryDatetime": {"type": ["string", "null"], "format": "date-time"},
                "deliveryDocumentNumber": {"type": ["string", "null"]},
                "details": {"type": ["array", "null"]}
            },
            "additionalProperties": True
        }

    @property
    def path(self):
        return "/VenditPublicApi/HistoryPurchaseOrders/GetWithDetails"

    def get_records(self, context: Optional[Dict]) -> Iterable[Dict[str, Any]]:
        """Override to use deliveryDatetime field (401) for history purchase orders.
        
        Uses deliveryDatetime for filtering since that's when purchase orders are
        completed and appear in history - more reliable than orderDateTime for
        incremental sync.
        """
        start_time = time.time()
        self.logger.info("🚀 Starting HistoryPurchaseOrders incremental sync...")
        self.logger.info(f"📡 Find endpoint: {self.config['api_url']}/VenditPublicApi/HistoryPurchaseOrders/Find")
        self.logger.info(f"📡 Details endpoint: {self.config['api_url']}{self.path}")
        
        start_date = self.get_starting_time(context)
        self.logger.info(f"📅 Sync start date (deliveryDatetime >=): {start_date.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Step 1: Find IDs using deliveryDatetime filter
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
                        "field": FIELD_IDS["DELIVERY_DATE_TIME"],  # Field 401 (deliveryDatetime)
                        "value": start_date.strftime("%Y-%m-%dT%H:%M:%S.000"),
                        "filterComparison": FILTER_COMPARISONS["GREATER_OR_EQUAL"]
                    }
                ],
                "paginationOffset": offset,
                "paginationLimit": DEFAULT_PAGE_SIZE,
                "operator": 0
            }
            
            # Debug logging to see what's being sent
            if page_count == 1:
                self.logger.info(f"🔍 Filter: deliveryDatetime >= {start_date}")
                self.logger.debug(f"🔍 DEBUG: Find payload: {payload}")
            
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
                # Clean empty strings that should be nulls
                for key, value in data.items():
                    if value == "":
                        data[key] = None
                    # Handle literal "string" values that should be null
                    elif key == "optiplyId" and value == "string":
                        data[key] = None
                
                # Fix details field - extract items from the object
                if "details" in data and isinstance(data["details"], dict) and "items" in data["details"]:
                    data["details"] = data["details"]["items"]
                
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
        if all_ids:
            self.logger.info(f"   • Average time per record: {total_elapsed/len(all_ids):.3f}s")
    
    # Uses base class get_starting_time which handles deliveryDatetime replication key


class SellOrderTransactionsStream(BaseFindGetWithDetailsStream):
    """Sell Order Transactions stream using Find → GetWithDetails pattern."""
    name = "sell_order_transactions"
    primary_keys = ["saleHeaderId"]
    replication_key = "transactionDatetime"
    records_jsonpath = "$"
    
    @property
    def schema(self):
        """Return schema specific to transactions data based on actual API response."""
        return {
            "type": "object",
            "properties": {
                "saleHeaderId": {"type": ["integer", "null"]},
                "totalPriceExVat": {"type": ["number", "null"]},
                "totalPriceIncVat": {"type": ["number", "null"]},
                "totalVat": {"type": ["number", "null"]},
                "invoiceNumber": {"type": ["string", "null"]},
                "receiptNumber": {"type": ["string", "null"]},
                "transactionDatetime": {"type": ["string", "null"], "format": "date-time"},
                "employeeId": {"type": ["integer", "null"]},
                "workstationName": {"type": ["string", "null"]},
                "workstationCode": {"type": ["string", "null"]},
                "officeId": {"type": ["integer", "null"]},
                "saleExVat": {"type": ["boolean", "null"]},
                "baselineReferenceNumber": {"type": ["string", "null"]},
                "telecomReferenceNumber": {"type": ["string", "null"]},
                "bonusText": {"type": ["string", "null"]},
                "returnReasonId": {"type": ["integer", "null"]},
                "returnExtraInformation": {"type": ["string", "null"]},
                "isEcommerce": {"type": ["boolean", "null"]},
                "isCustomerOrderInvoice": {"type": ["boolean", "null"]},
                "planInfoDeliveryDatetime": {"type": ["string", "null"], "format": "date-time"},
                "planInfoEmployeeId": {"type": ["integer", "null"]},
                "customer": {
                    "type": ["object", "null"],
                    "properties": {
                        "items": {
                            "type": ["array", "null"],
                            "items": {
                                "type": "object",
                                "properties": {
                                    "saleCustomerId": {"type": ["integer", "null"]},
                                    "saleHeaderId": {"type": ["integer", "null"]},
                                    "customerId": {"type": ["integer", "null"]},
                                    "invoiceZipcode": {"type": ["string", "null"]},
                                    "invoiceCity": {"type": ["string", "null"]},
                                    "invoiceAddressId": {"type": ["integer", "null"]},
                                    "invoiceContactId": {"type": ["integer", "null"]},
                                    "deliveryAddressId": {"type": ["integer", "null"]},
                                    "deliveryContactId": {"type": ["integer", "null"]},
                                    "invoiceCountryId": {"type": ["integer", "null"]}
                                },
                                "additionalProperties": True
                            }
                        }
                    },
                    "additionalProperties": True
                },
                "saleDetails": {
                    "type": ["object", "null"],
                    "properties": {
                        "items": {
                            "type": ["array", "null"],
                            "items": {
                                "type": "object",
                                "properties": {
                                    "saleDetailId": {"type": ["integer", "null"]},
                                    "saleHeaderId": {"type": ["integer", "null"]},
                                    "productId": {"type": ["integer", "null"]},
                                    "productQuantity": {"type": ["number", "null"]},
                                    "productNumber": {"type": ["string", "null"]},
                                    "productType": {"type": ["string", "null"]},
                                    "productKindDescription": {"type": ["string", "null"]},
                                    "productDescription": {"type": ["string", "null"]},
                                    "productSubdescription": {"type": ["string", "null"]},
                                    "brancheName": {"type": ["string", "null"]},
                                    "productGroupId": {"type": ["integer", "null"]},
                                    "brandName": {"type": ["string", "null"]},
                                    "productColorCode": {"type": ["string", "null"]},
                                    "productColorName": {"type": ["string", "null"]},
                                    "productSizeName": {"type": ["string", "null"]},
                                    "supplierNumber": {"type": ["string", "null"]},
                                    "supplierName": {"type": ["string", "null"]},
                                    "frameNumber": {"type": ["string", "null"]},
                                    "lockNumber": {"type": ["string", "null"]},
                                    "keyNumber": {"type": ["string", "null"]},
                                    "dataTagNumber": {"type": ["string", "null"]},
                                    "serialNumber": {"type": ["string", "null"]},
                                    "imeiNumber": {"type": ["string", "null"]},
                                    "licensePlateNumber": {"type": ["string", "null"]},
                                    "productPurchasePriceEx": {"type": ["number", "null"]},
                                    "productPurchasePriceInc": {"type": ["number", "null"]},
                                    "productSalesPriceEx": {"type": ["number", "null"]},
                                    "productSalesPriceInc": {"type": ["number", "null"]},
                                    "productVatPercentage": {"type": ["number", "null"]},
                                    "productTotalVatAmount": {"type": ["number", "null"]},
                                    "productTotalSalesPriceEx": {"type": ["number", "null"]},
                                    "productTotalSalesPriceInc": {"type": ["number", "null"]},
                                    "productTotalMarginProEx": {"type": ["number", "null"]},
                                    "productTotalMarginEx": {"type": ["number", "null"]},
                                    "productTotalMarginProInc": {"type": ["number", "null"]},
                                    "productTotalMarginInc": {"type": ["number", "null"]},
                                    "productVatDescription": {"type": ["string", "null"]},
                                    "vatId": {"type": ["integer", "null"]},
                                    "productSizeColorId": {"type": ["integer", "null"]},
                                    "licensePlateMeldcode": {"type": ["string", "null"]},
                                    "motorNumber": {"type": ["string", "null"]},
                                    "turnoverEmployeeId": {"type": ["integer", "null"]},
                                    "brancheId": {"type": ["integer", "null"]},
                                    "linkId": {"type": ["integer", "null"]},
                                    "processedSalePriceIncForVat": {"type": ["number", "null"]},
                                    "exportBit": {"type": ["integer", "null"]},
                                    "passportNumber": {"type": ["string", "null"]},
                                    "stockOfficeId": {"type": ["integer", "null"]},
                                    "returnFromSaleDetailId": {"type": ["integer", "null"]},
                                    "batteryNumber": {"type": ["string", "null"]},
                                    "displayNumber": {"type": ["string", "null"]},
                                    "extraProductCostsId": {"type": ["integer", "null"]},
                                    "extraProductCostsGroupGuid": {"type": ["string", "null"]},
                                    "orderTurnoverDatetime": {"type": ["string", "null"], "format": "date-time"},
                                    "customerOrderNumber": {"type": ["string", "null"]},
                                    "customerOrderCreationDatetime": {"type": ["string", "null"], "format": "date-time"},
                                    "assortmentCode": {"type": ["string", "null"]},
                                    "mileage": {"type": ["string", "null"]},
                                    "resaleBonus": {"type": ["number", "null"]},
                                    "combinationDiscountId": {"type": ["integer", "null"]},
                                    "combinationDiscountQuantity": {"type": ["number", "null"]},
                                    "registeredDiscountType": {"type": ["integer", "null"]},
                                    "leaseContractNumber": {"type": ["string", "null"]},
                                    "leaseVehicleNumber": {"type": ["string", "null"]}
                                },
                                "additionalProperties": True
                            }
                        }
                    },
                    "additionalProperties": True
                }
            },
            "additionalProperties": True
        }

    @property
    def path(self):
        return "/VenditPublicApi/Transactions/GetWithDetails"

    def get_records(self, context: Optional[Dict]) -> Iterable[Dict[str, Any]]:
        """Override to use correct field ID for transactions."""
        start_time = time.time()
        self.logger.info(f"🚀 Starting {self.name} sync using Find → GetWithDetails pattern...")
        
        start_date = self.get_starting_time(context)
        self.logger.info(f"📅 Sync start date: {start_date.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Step 1: Find IDs
        self.logger.info(f"🔍 Step 1: Finding {self.name} IDs...")
        find_url = f"{self.config['api_url']}/VenditPublicApi/Transactions/Find"
        all_ids = []
        offset = 0
        page_count = 0
        
        while True:
            page_count += 1
            payload = {
                "fieldFilters": [
                    {
                        "field": FIELD_IDS["TRANSACTION_FILTER_154"],
                        "filterComparison": FILTER_COMPARISONS["IS_NOT_NULL"],
                        "value": ""
                    },
                    {
                        "field": FIELD_IDS["TRANSACTION_DATETIME"],
                        "filterComparison": FILTER_COMPARISONS["GREATER_OR_EQUAL"],
                        "value": start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    }
                ],
                "paginationOffset": offset,
                "paginationLimit": DEFAULT_PAGE_SIZE,
                "operator": 0
            }
            
            self.logger.debug(f"📄 Fetching page {page_count} (offset: {offset})...")
            response = self._request("POST", find_url, json=payload)
            data = self._parse_json_response(response, "finding transaction IDs")
            
            ids = data.get("results", [])
            if not ids:
                self.logger.debug(f"📄 Page {page_count}: No results")
                break
                
            all_ids.extend([str(i) for i in ids if i])
            self.logger.debug(f"📄 Page {page_count}: Found {len(ids)} IDs (total: {len(all_ids)})")
            
            if len(ids) < DEFAULT_PAGE_SIZE:
                self.logger.debug(f"📄 Page {page_count}: Last page (less than {DEFAULT_PAGE_SIZE} results)")
                break
            offset += DEFAULT_PAGE_SIZE
            
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
                # Clean empty strings that should be nulls
                for key, value in data.items():
                    if value == "":
                        data[key] = None
                    # Handle literal "string" values that should be null
                    elif key == "optiplyId" and value == "string":
                        data[key] = None
                
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


class TransactionsStream(BaseFindGetWithDetailsStream):
    """Transactions stream using Find → GetWithDetails pattern (all transactions without filters)."""
    name = "transactions"
    primary_keys = ["saleHeaderId"]
    replication_key = "transactionDatetime"
    records_jsonpath = "$"
    
    @property
    def schema(self):
        """Return schema specific to transactions data based on actual API response."""
        return {
            "type": "object",
            "properties": {
                "saleHeaderId": {"type": ["integer", "null"]},
                "totalPriceExVat": {"type": ["number", "null"]},
                "totalPriceIncVat": {"type": ["number", "null"]},
                "totalVat": {"type": ["number", "null"]},
                "invoiceNumber": {"type": ["string", "null"]},
                "receiptNumber": {"type": ["string", "null"]},
                "transactionDatetime": {"type": ["string", "null"], "format": "date-time"},
                "employeeId": {"type": ["integer", "null"]},
                "workstationName": {"type": ["string", "null"]},
                "workstationCode": {"type": ["string", "null"]},
                "officeId": {"type": ["integer", "null"]},
                "saleExVat": {"type": ["boolean", "null"]},
                "baselineReferenceNumber": {"type": ["string", "null"]},
                "telecomReferenceNumber": {"type": ["string", "null"]},
                "bonusText": {"type": ["string", "null"]},
                "returnReasonId": {"type": ["integer", "null"]},
                "returnExtraInformation": {"type": ["string", "null"]},
                "isEcommerce": {"type": ["boolean", "null"]},
                "isCustomerOrderInvoice": {"type": ["boolean", "null"]},
                "planInfoDeliveryDatetime": {"type": ["string", "null"], "format": "date-time"},
                "planInfoEmployeeId": {"type": ["integer", "null"]},
                "customer": {
                    "type": ["object", "null"],
                    "properties": {
                        "items": {
                            "type": ["array", "null"],
                            "items": {
                                "type": "object",
                                "properties": {
                                    "saleCustomerId": {"type": ["integer", "null"]},
                                    "saleHeaderId": {"type": ["integer", "null"]},
                                    "customerId": {"type": ["integer", "null"]},
                                    "invoiceZipcode": {"type": ["string", "null"]},
                                    "invoiceCity": {"type": ["string", "null"]},
                                    "invoiceAddressId": {"type": ["integer", "null"]},
                                    "invoiceContactId": {"type": ["integer", "null"]},
                                    "deliveryAddressId": {"type": ["integer", "null"]},
                                    "deliveryContactId": {"type": ["integer", "null"]},
                                    "invoiceCountryId": {"type": ["integer", "null"]}
                                },
                                "additionalProperties": True
                            }
                        }
                    },
                    "additionalProperties": True
                },
                "saleDetails": {
                    "type": ["object", "null"],
                    "properties": {
                        "items": {
                            "type": ["array", "null"],
                            "items": {
                                "type": "object",
                                "properties": {
                                    "saleDetailId": {"type": ["integer", "null"]},
                                    "saleHeaderId": {"type": ["integer", "null"]},
                                    "productId": {"type": ["integer", "null"]},
                                    "productQuantity": {"type": ["number", "null"]},
                                    "productNumber": {"type": ["string", "null"]},
                                    "productType": {"type": ["string", "null"]},
                                    "productKindDescription": {"type": ["string", "null"]},
                                    "productDescription": {"type": ["string", "null"]},
                                    "productSubdescription": {"type": ["string", "null"]},
                                    "brancheName": {"type": ["string", "null"]},
                                    "productGroupId": {"type": ["integer", "null"]},
                                    "brandName": {"type": ["string", "null"]},
                                    "productColorCode": {"type": ["string", "null"]},
                                    "productColorName": {"type": ["string", "null"]},
                                    "productSizeName": {"type": ["string", "null"]},
                                    "supplierNumber": {"type": ["string", "null"]},
                                    "supplierName": {"type": ["string", "null"]},
                                    "frameNumber": {"type": ["string", "null"]},
                                    "lockNumber": {"type": ["string", "null"]},
                                    "keyNumber": {"type": ["string", "null"]},
                                    "dataTagNumber": {"type": ["string", "null"]},
                                    "serialNumber": {"type": ["string", "null"]},
                                    "imeiNumber": {"type": ["string", "null"]},
                                    "licensePlateNumber": {"type": ["string", "null"]},
                                    "productPurchasePriceEx": {"type": ["number", "null"]},
                                    "productPurchasePriceInc": {"type": ["number", "null"]},
                                    "productSalesPriceEx": {"type": ["number", "null"]},
                                    "productSalesPriceInc": {"type": ["number", "null"]},
                                    "productVatPercentage": {"type": ["number", "null"]},
                                    "productTotalVatAmount": {"type": ["number", "null"]},
                                    "productTotalSalesPriceEx": {"type": ["number", "null"]},
                                    "productTotalSalesPriceInc": {"type": ["number", "null"]},
                                    "productTotalMarginProEx": {"type": ["number", "null"]},
                                    "productTotalMarginEx": {"type": ["number", "null"]},
                                    "productTotalMarginProInc": {"type": ["number", "null"]},
                                    "productTotalMarginInc": {"type": ["number", "null"]},
                                    "productVatDescription": {"type": ["string", "null"]},
                                    "vatId": {"type": ["integer", "null"]},
                                    "productSizeColorId": {"type": ["integer", "null"]},
                                    "licensePlateMeldcode": {"type": ["string", "null"]},
                                    "motorNumber": {"type": ["string", "null"]},
                                    "turnoverEmployeeId": {"type": ["integer", "null"]},
                                    "brancheId": {"type": ["integer", "null"]},
                                    "linkId": {"type": ["integer", "null"]},
                                    "processedSalePriceIncForVat": {"type": ["number", "null"]},
                                    "exportBit": {"type": ["integer", "null"]},
                                    "passportNumber": {"type": ["string", "null"]},
                                    "stockOfficeId": {"type": ["integer", "null"]},
                                    "returnFromSaleDetailId": {"type": ["integer", "null"]},
                                    "batteryNumber": {"type": ["string", "null"]},
                                    "displayNumber": {"type": ["string", "null"]},
                                    "extraProductCostsId": {"type": ["integer", "null"]},
                                    "extraProductCostsGroupGuid": {"type": ["string", "null"]},
                                    "orderTurnoverDatetime": {"type": ["string", "null"], "format": "date-time"},
                                    "customerOrderNumber": {"type": ["string", "null"]},
                                    "customerOrderCreationDatetime": {"type": ["string", "null"], "format": "date-time"},
                                    "assortmentCode": {"type": ["string", "null"]},
                                    "mileage": {"type": ["string", "null"]},
                                    "resaleBonus": {"type": ["number", "null"]},
                                    "combinationDiscountId": {"type": ["integer", "null"]},
                                    "combinationDiscountQuantity": {"type": ["number", "null"]},
                                    "registeredDiscountType": {"type": ["integer", "null"]},
                                    "leaseContractNumber": {"type": ["string", "null"]},
                                    "leaseVehicleNumber": {"type": ["string", "null"]}
                                },
                                "additionalProperties": True
                            }
                        }
                    },
                    "additionalProperties": True
                }
            },
            "additionalProperties": True
        }

    @property
    def path(self):
        return "/VenditPublicApi/Transactions/GetWithDetails"

    def get_records(self, context: Optional[Dict]) -> Iterable[Dict[str, Any]]:
        """Override to get all transactions without field 154 filter."""
        start_time = time.time()
        self.logger.info(f"🚀 Starting {self.name} sync using Find → GetWithDetails pattern...")
        
        start_date = self.get_starting_time(context)
        self.logger.info(f"📅 Sync start date: {start_date.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Step 1: Find IDs
        self.logger.info(f"🔍 Step 1: Finding {self.name} IDs...")
        find_url = f"{self.config['api_url']}/VenditPublicApi/Transactions/Find"
        all_ids = []
        offset = 0
        page_count = 0
        
        while True:
            page_count += 1
            payload = {
                "fieldFilters": [
                    {
                        "field": FIELD_IDS["TRANSACTION_DATETIME"],
                        "filterComparison": FILTER_COMPARISONS["GREATER_OR_EQUAL"],
                        "value": start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    }
                ],
                "paginationOffset": offset,
                "paginationLimit": DEFAULT_PAGE_SIZE,
                "operator": 0
            }
            
            self.logger.debug(f"📄 Fetching page {page_count} (offset: {offset})...")
            response = self._request("POST", find_url, json=payload)
            data = self._parse_json_response(response, "finding transaction IDs")
            
            ids = data.get("results", [])
            if not ids:
                self.logger.debug(f"📄 Page {page_count}: No results")
                break
                
            all_ids.extend([str(i) for i in ids if i])
            self.logger.debug(f"📄 Page {page_count}: Found {len(ids)} IDs (total: {len(all_ids)})")
            
            if len(ids) < DEFAULT_PAGE_SIZE:
                self.logger.debug(f"📄 Page {page_count}: Last page (less than {DEFAULT_PAGE_SIZE} results)")
                break
            offset += DEFAULT_PAGE_SIZE
            
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
                # Clean empty strings that should be nulls
                for key, value in data.items():
                    if value == "":
                        data[key] = None
                    # Handle literal "string" values that should be null
                    elif key == "optiplyId" and value == "string":
                        data[key] = None
                
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
    
     