"""Stream type classes for tap-vendit."""

from __future__ import annotations

import typing as t
from importlib import resources
from datetime import datetime
from typing import List, Dict, Any, Optional, Iterable
import time

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_vendit.client import VenditStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = resources.files(__package__) / "schemas"
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.

class DynamicStream(VenditStream):
    """Base stream that can switch between sync/bulk endpoints and DRY logic."""
    @property
    def sync_endpoint(self):
        return use_sync_endpoint

    def _ensure_authenticated_request(self, method: str, url: str, **kwargs) -> requests.Response:
        # DRY: shared token logic
        if not self._tap.authenticator.is_token_valid():
            self._tap.authenticator.update_access_token()
        token = self._tap._config.get("token")
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Token": token,
            "ApiKey": self._tap.config["api_key"]
        }
        if "headers" in kwargs:
            headers.update(kwargs["headers"])
        kwargs["headers"] = headers
        return requests.request(method, url, **kwargs)

    def get_starting_time(self, context: Optional[dict]) -> datetime:
        # DRY: shared incremental logic
        start_date = self.config.get("start_date")
        if start_date:
            return datetime.fromisoformat(start_date)
        return datetime(1970, 1, 1)

class BaseStream(VenditStream):
    """Base stream with DRY incremental and request logic."""
    def get_starting_time(self, context: Optional[dict]) -> datetime:
        replication_key_value = self.get_starting_replication_key_value(context)
        if replication_key_value:
            return datetime.fromisoformat(replication_key_value)
        start_date = self.config.get("start_date")
        if start_date:
            return datetime.fromisoformat(start_date)
        return datetime(1970, 1, 1)

    def _request(self, method, url, **kwargs):
        # Always ensure token is present and valid
        if not self.authenticator.is_token_valid():
            self.logger.info("Token missing or expired, fetching new token...")
            self.authenticator.update_access_token()
        # Inject token and api_key into headers
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

class BaseFindStream(BaseStream):
    """Base class for Find streams that only return IDs."""
    # Simple schema for Find streams - they only return IDs
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
    ).to_dict()
    
    def get_all_ids_with_filter(self, field_id: int, start_date: datetime, page_size: int = 100) -> list[str]:
        all_ids = []
        offset = 0
        while True:
            payload = {
                "fieldFilters": [
                    {
                        "field": field_id,
                        "value": start_date.strftime("%Y-%m-%dT%H:%M:%S.000"),
                        "filterComparison": 2
                    }
                ],
                "paginationOffset": offset,
                "operator": 0
            }
            url = f"{self.config['api_url']}{self.path}"
            response = self._request("POST", url, json=payload)
            try:
                data = response.json()
            except Exception as e:
                print(f"API response status: {response.status_code}")
                print(f"API response text: {response.text}")
                raise
            ids = data.get("results", [])
            if not ids:
                break
            all_ids.extend([str(i) for i in ids if i])
            if len(ids) < page_size:
                break
            offset += page_size
        return all_ids

class BaseGetMultipleStream(BaseStream):
    """Base class for GetMultiple streams (batch by IDs)."""
    BATCH_SIZE = 100
    def get_records_batch(self, context: Optional[dict], id_field: str) -> Iterable[Dict[str, Any]]:
        ids = context.get(f"{id_field}_ids", []) if context else []
        if not ids:
            self.logger.warning(f"No {id_field} IDs provided in context")
            return
        for i in range(0, len(ids), self.BATCH_SIZE):
            batch = ids[i:i + self.BATCH_SIZE]
            url = f"{self.config['api_url']}{self.path}"
            response = self._request("POST", url, json={"primaryKeys": batch})
            if response.status_code != 200:
                self.logger.error(f"Error fetching {self.name}: {response.status_code}")
                continue
            data = response.json()
            for item in data.get("items", []):
                yield item

class BaseGetWithDetailsStream(BaseStream):
    """Base class for GetWithDetails streams (fetch by ID)."""
    def get_records_individual(self, context: Optional[dict], id_field: str) -> Iterable[Dict[str, Any]]:
        ids = context.get(f"{id_field}_ids", []) if context else []
        if not ids:
            self.logger.warning(f"No {id_field} IDs provided in context")
            return
        for item_id in ids:
            url = f"{self.config['api_url']}{self.path}/{item_id}"
            response = self._request("GET", url)
            if response.status_code != 200:
                self.logger.error(f"Error fetching {self.name} {item_id}: {response.status_code}")
                continue
            data = response.json()
            if data:
                yield data

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
        # Get the unix timestamp to use for this run
        state = context or {}
        last_synced_unix = state.get("replication_key_value")
        
        if last_synced_unix is None:
            # First run: use default start date
            last_synced_unix = self.get_starting_unix()
            self.logger.info(f"First run: using default start unix {last_synced_unix}")
        else:
            self.logger.info(f"Incremental run: using saved unix {last_synced_unix}")
        
        # Call the API with the unix timestamp
        url = self.get_url(last_synced_unix)
        self.logger.info(f"Fetching data from {url}")
        
        response = self.session.get(url, headers=self.authenticator.auth_headers)
        if response.status_code != 200:
            self.logger.error(f"Error fetching data: {response.status_code}")
            self.logger.error(response.text)
            return
        
        data = response.json()
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
        """Override this method to provide the specific Optiply endpoint URL."""
        raise NotImplementedError("Subclasses must implement get_url method")

# Common schema components for reuse
def get_product_schema() -> th.PropertiesList:
    """Common product schema used across multiple streams."""
    return th.PropertiesList(
        th.Property("productId", th.IntegerType),
        th.Property("groupId", th.IntegerType),
        th.Property("brandId", th.IntegerType),
        th.Property("brancheId", th.IntegerType),
        th.Property("productNumber", th.StringType),
        th.Property("productType", th.StringType),
        th.Property("productKindId", th.IntegerType),
        th.Property("productDescription", th.StringType),
        th.Property("productSubdescription", th.StringType),
        th.Property("additionalInfo", th.StringType),
        th.Property("useStock", th.BooleanType),
        th.Property("salesVisibilityId", th.IntegerType),
        th.Property("availabilityStatusId", th.IntegerType),
        th.Property("creationDatetime", th.DateTimeType),
        th.Property("allowInvoiceDiscount", th.BooleanType),
        th.Property("lastModified", th.DateTimeType),
        th.Property("isModified", th.BooleanType),
        th.Property("productGuid", th.StringType),
        th.Property("memoCommon", th.StringType),
        th.Property("memoEcommerce", th.StringType),
        th.Property("isDeleted", th.BooleanType),
        th.Property("productSize", th.StringType),
        th.Property("modelSeason", th.StringType),
        th.Property("productColor", th.StringType),
        th.Property("useGroupInvoiceDiscount", th.BooleanType),
        th.Property("salesUnitId", th.IntegerType),
        th.Property("salesUnitQuantity", th.NumberType),
        th.Property("hintInfo", th.StringType),
        th.Property("productSearchCode", th.StringType),
        th.Property("isBaseProduct", th.BooleanType),
        th.Property("deliveryFromWarehouse", th.IntegerType),
        th.Property("purchaseFromWarehouse", th.IntegerType),
        th.Property("viaCollectionWarehouse", th.IntegerType),
        th.Property("bebat", th.IntegerType),
        th.Property("extraCostQuantity", th.NumberType),
        th.Property("mintatonPosDealId", th.StringType),
        th.Property("assortmentCode", th.StringType),
        th.Property("modifiedBy", th.StringType),
        th.Property("createdBy", th.StringType),
        th.Property("originCountryCode", th.StringType),
        th.Property("originCountry", th.StringType),
    )

def get_supplier_schema() -> th.PropertiesList:
    """Common supplier schema."""
    return th.PropertiesList(
        th.Property("supplierId", th.IntegerType),
        th.Property("officeId", th.IntegerType),
        th.Property("supplierNumber", th.StringType),
        th.Property("supplierName", th.StringType),
        th.Property("supplierType", th.StringType),
        th.Property("supplierDescription", th.StringType),
        th.Property("supplierSubdescription", th.StringType),
        th.Property("supplierExtraInfo", th.StringType),
        th.Property("supplierEmail", th.StringType),
        th.Property("supplierPhone", th.StringType),
        th.Property("supplierFax", th.StringType),
        th.Property("supplierWebsite", th.StringType),
        th.Property("supplierRemark", th.StringType),
        th.Property("supplierGuid", th.StringType),
        th.Property("isDeleted", th.BooleanType),
        th.Property("lastModified", th.DateTimeType),
        th.Property("isModified", th.BooleanType),
        th.Property("supplierSearchCode", th.StringType),
        th.Property("supplierAssortmentCode", th.StringType),
        th.Property("supplierCountryCode", th.StringType),
        th.Property("supplierCountry", th.StringType),
        th.Property("supplierVatNumber", th.StringType),
        th.Property("supplierBankAccount", th.StringType),
        th.Property("supplierBankName", th.StringType),
        th.Property("supplierBankBic", th.StringType),
        th.Property("supplierBankIban", th.StringType),
        th.Property("supplierBankSwift", th.StringType),
        th.Property("supplierBankAccountName", th.StringType),
        th.Property("supplierBankAccountNumber", th.StringType),
        th.Property("supplierBankAccountType", th.StringType),
        th.Property("supplierBankAccountCurrency", th.StringType),
        th.Property("supplierBankAccountDescription", th.StringType),
        th.Property("supplierBankAccountRemark", th.StringType),
        th.Property("supplierBankAccountGuid", th.StringType),
        th.Property("supplierBankAccountIsDeleted", th.BooleanType),
        th.Property("supplierBankAccountLastModified", th.DateTimeType),
        th.Property("supplierBankAccountIsModified", th.BooleanType),
        th.Property("supplierBankAccountSearchCode", th.StringType),
        th.Property("supplierBankAccountAssortmentCode", th.StringType),
        th.Property("supplierBankAccountCountryCode", th.StringType),
        th.Property("supplierBankAccountCountry", th.StringType),
        th.Property("supplierBankAccountVatNumber", th.StringType),
    )

def get_purchase_order_detail_schema() -> th.PropertiesList:
    """Common purchase order detail schema."""
    return th.PropertiesList(
        th.Property("productPurchaseOrderDetailId", th.IntegerType),
        th.Property("productPurchaseOrderId", th.IntegerType),
        th.Property("productId", th.IntegerType),
        th.Property("supplierProductNumber", th.StringType),
        th.Property("productNumber", th.StringType),
        th.Property("productType", th.StringType),
        th.Property("productDescription", th.StringType),
        th.Property("productSubdescription", th.StringType),
        th.Property("productExtraInfo", th.StringType),
        th.Property("amount", th.NumberType),
        th.Property("purchasePriceEx", th.NumberType),
        th.Property("minOrderQuantity", th.NumberType),
        th.Property("expectedDeliveryWeek", th.IntegerType),
        th.Property("expectedDeliveryDate", th.DateTimeType),
        th.Property("webserviceStatusEnum", th.IntegerType),
        th.Property("orderDetailRemark", th.StringType),
        th.Property("extraPriceInfo", th.StringType),
        th.Property("assortmentCode", th.StringType),
        th.Property("promotionProductId", th.IntegerType),
        th.Property("lineId", th.StringType),
    )

def get_order_schema() -> th.PropertiesList:
    """Common order schema."""
    return th.PropertiesList(
        th.Property("customerOrderHeaderId", th.IntegerType),
        th.Property("officeId", th.IntegerType),
        th.Property("customerOrderNumber", th.StringType),
        th.Property("customerId", th.IntegerType),
        th.Property("orderDatetime", th.DateTimeType),
        th.Property("orderReference", th.StringType),
        th.Property("preorderEmployeeId", th.IntegerType),
        th.Property("employeeId", th.IntegerType),
        th.Property("onlineOrderReference", th.StringType),
        th.Property("orderRemark", th.StringType),
        th.Property("orderStatusId", th.IntegerType),
        th.Property("orderStatusDescription", th.StringType),
        th.Property("orderStatusColor", th.StringType),
        th.Property("orderStatusIcon", th.StringType),
        th.Property("orderStatusSortOrder", th.IntegerType),
        th.Property("orderStatusIsDeleted", th.BooleanType),
        th.Property("orderStatusLastModified", th.DateTimeType),
        th.Property("orderStatusIsModified", th.BooleanType),
        th.Property("orderStatusSearchCode", th.StringType),
        th.Property("orderStatusAssortmentCode", th.StringType),
        th.Property("orderStatusCountryCode", th.StringType),
        th.Property("orderStatusCountry", th.StringType),
        th.Property("orderStatusVatNumber", th.StringType),
        th.Property("orderStatusBankAccount", th.StringType),
        th.Property("orderStatusBankName", th.StringType),
        th.Property("orderStatusBankBic", th.StringType),
        th.Property("orderStatusBankIban", th.StringType),
        th.Property("orderStatusBankSwift", th.StringType),
        th.Property("orderStatusBankAccountName", th.StringType),
        th.Property("orderStatusBankAccountNumber", th.StringType),
        th.Property("orderStatusBankAccountType", th.StringType),
        th.Property("orderStatusBankAccountCurrency", th.StringType),
        th.Property("orderStatusBankAccountDescription", th.StringType),
        th.Property("orderStatusBankAccountRemark", th.StringType),
        th.Property("orderStatusBankAccountGuid", th.StringType),
        th.Property("orderStatusBankAccountIsDeleted", th.BooleanType),
        th.Property("orderStatusBankAccountLastModified", th.DateTimeType),
        th.Property("orderStatusBankAccountIsModified", th.BooleanType),
        th.Property("orderStatusBankAccountSearchCode", th.StringType),
        th.Property("orderStatusBankAccountAssortmentCode", th.StringType),
        th.Property("orderStatusBankAccountCountryCode", th.StringType),
        th.Property("orderStatusBankAccountCountry", th.StringType),
        th.Property("orderStatusBankAccountVatNumber", th.StringType),
    )

def get_purchase_order_schema() -> th.PropertiesList:
    """Common purchase order schema."""
    return th.PropertiesList(
        th.Property("productPurchaseOrderId", th.IntegerType),
        th.Property("officeId", th.IntegerType),
        th.Property("purchaseOrderNumber", th.StringType),
        th.Property("supplierId", th.IntegerType),
        th.Property("orderDatetime", th.DateTimeType),
        th.Property("orderReference", th.StringType),
        th.Property("preorderEmployeeId", th.IntegerType),
        th.Property("employeeId", th.IntegerType),
        th.Property("onlineOrderReference", th.StringType),
        th.Property("orderRemark", th.StringType),
        th.Property(
            "details",
            th.ObjectType(
                th.Property(
                    "items",
                    th.ArrayType(
                        get_purchase_order_detail_schema()
                    )
                )
            )
        )
    )

# Stream implementations
class ProductsStream(BaseFindStream):
    """Combined Products stream that handles Find → GetMultiple internally."""
    name = "products"
    primary_keys = ["productId"]
    replication_key = "LastModified"
    records_jsonpath = "$.items[*]"
    schema = get_product_schema().to_dict()

    @property
    def path(self):
        return "/VenditPublicApi/Products/GetMultiple"

    def get_records(self, context: Optional[Dict]) -> Iterable[Dict[str, Any]]:
        self.logger.info("Step 1: Finding product IDs...")
        start_date = self.get_starting_time(context)
        # Use the correct Find endpoint for IDs
        find_url = f"{self.config['api_url']}/VenditPublicApi/Products/Find"
        all_ids = []
        offset = 0
        page_size = 100
        while True:
            payload = {
                "fieldFilters": [
                    {
                        "field": 204,
                        "value": start_date.strftime("%Y-%m-%dT%H:%M:%S.000"),
                        "filterComparison": 2
                    }
                ],
                "paginationOffset": offset,
                "operator": 0
            }
            response = self._request("POST", find_url, json=payload)
            try:
                data = response.json()
            except Exception as e:
                print(f"API response status: {response.status_code}")
                print(f"API response text: {response.text}")
                raise
            ids = data.get("results", [])
            if not ids:
                break
            all_ids.extend([str(i) for i in ids if i])
            if len(ids) < page_size:
                break
            offset += page_size
        product_ids = all_ids
        if not product_ids:
            self.logger.warning("No product IDs found")
            return
        self.logger.info(f"Found {len(product_ids)} product IDs")
        self.logger.info("Step 2: Getting product details...")
        batch_size = 100
        for i in range(0, len(product_ids), batch_size):
            batch = product_ids[i:i + batch_size]
            url = f"{self.config['api_url']}{self.path}"
            response = self._request("POST", url, json={"primaryKeys": batch})
            if response.status_code != 200:
                self.logger.error(f"Error fetching products batch: {response.status_code}")
                continue
            data = response.json()
            for item in data.get("items", []):
                yield item

class SuppliersStream(BaseFindStream):
    """Combined Suppliers stream that handles getAllIds → GetMultiple internally."""
    name = "suppliers"
    primary_keys = ["supplierId"]
    replication_key = None
    records_jsonpath = "$.items[*]"
    schema = get_supplier_schema().to_dict()

    @property
    def path(self):
        return "/VenditPublicApi/Suppliers/GetMultiple"

    def get_records(self, context: Optional[Dict]) -> Iterable[Dict[str, Any]]:
        self.logger.info("Step 1: Getting all supplier IDs...")
        # Use getAllIds endpoint instead of Find
        get_all_ids_url = f"{self.config['api_url']}/VenditPublicApi/Suppliers/GetAllIds"
        response = self._request("GET", get_all_ids_url)
        try:
            data = response.json()
        except Exception as e:
            print(f"API response status: {response.status_code}")
            print(f"API response text: {response.text}")
            raise
        supplier_ids = [str(i) for i in data if i]
        if not supplier_ids:
            self.logger.warning("No supplier IDs found")
            return
        self.logger.info(f"Found {len(supplier_ids)} supplier IDs")
        self.logger.info("Step 2: Getting supplier details...")
        batch_size = 100
        for i in range(0, len(supplier_ids), batch_size):
            batch = supplier_ids[i:i + batch_size]
            url = f"{self.config['api_url']}{self.path}"
            response = self._request("POST", url, json={"primaryKeys": batch})
            if response.status_code != 200:
                self.logger.error(f"Error fetching suppliers batch: {response.status_code}")
                continue
            data = response.json()
            for item in data.get("items", []):
                yield item

class OrdersStream(BaseFindStream):
    """Combined Orders stream that handles Find → GetWithDetails internally."""
    name = "orders"
    primary_keys = ["customerOrderHeaderId"]
    replication_key = "LastModified"
    records_jsonpath = "$"
    schema = get_order_schema().to_dict()

    @property
    def path(self):
        return "/VenditPublicApi/Orders/GetWithDetails"

    def get_records(self, context: Optional[Dict]) -> Iterable[Dict[str, Any]]:
        self.logger.info("Step 1: Finding order IDs...")
        start_date = self.get_starting_time(context)
        find_url = f"{self.config['api_url']}/VenditPublicApi/Orders/Find"
        all_ids = []
        offset = 0
        page_size = 100
        while True:
            payload = {
                "fieldFilters": [
                    {
                        "field": 204,
                        "value": start_date.strftime("%Y-%m-%dT%H:%M:%S.000"),
                        "filterComparison": 2
                    }
                ],
                "paginationOffset": offset,
                "operator": 0
            }
            response = self._request("POST", find_url, json=payload)
            try:
                data = response.json()
            except Exception as e:
                print(f"API response status: {response.status_code}")
                print(f"API response text: {response.text}")
                raise
            ids = data.get("results", [])
            if not ids:
                break
            all_ids.extend([str(i) for i in ids if i])
            if len(ids) < page_size:
                break
            offset += page_size
        order_ids = all_ids
        if not order_ids:
            self.logger.warning("No order IDs found")
            return
        self.logger.info(f"Found {len(order_ids)} order IDs")
        self.logger.info("Step 2: Getting order details...")
        for order_id in order_ids:
            url = f"{self.config['api_url']}{self.path}/{order_id}"
            response = self._request("GET", url)
            if response.status_code != 200:
                self.logger.error(f"Error fetching order {order_id}: {response.status_code}")
                continue
            data = response.json()
            if data:
                yield data

class PurchaseOrdersStream(BaseFindStream):
    """Combined Purchase Orders stream that handles Find → GetWithDetails internally."""
    name = "purchase_orders"
    primary_keys = ["productPurchaseOrderId"]
    replication_key = None
    records_jsonpath = "$"
    schema = get_purchase_order_schema().to_dict()

    @property
    def path(self):
        return "/VenditPublicApi/PurchaseOrders/GetWithDetails"

    def get_records(self, context: Optional[Dict]) -> Iterable[Dict[str, Any]]:
        self.logger.info("Step 1: Finding purchase order IDs...")
        start_date = self.config.get("start_date")
        end_date = self.config.get("end_date")
        if not start_date or not end_date:
            self.logger.error("start_date and end_date are required in config")
            return
        find_url = f"{self.config['api_url']}/VenditPublicApi/PurchaseOrders/Find"
        all_ids = []
        offset = 0
        page_size = 100
        while True:
            payload = {
                "fieldFilters": [
                    {
                        "field": 204,
                        "value": start_date,
                        "filterComparison": 2
                    }
                ],
                "paginationOffset": offset,
                "paginationLimit": page_size,
                "operator": 0
            }
            response = self._request("POST", find_url, json=payload)
            try:
                data = response.json()
            except Exception as e:
                print(f"API response status: {response.status_code}")
                print(f"API response text: {response.text}")
                raise
            ids = data.get("results", [])
            if not ids:
                break
            all_ids.extend([str(i) for i in ids if i])
            if len(ids) < page_size:
                break
            offset += page_size
        purchase_order_ids = all_ids
        if not purchase_order_ids:
            self.logger.warning("No purchase order IDs found")
            return
        self.logger.info(f"Found {len(purchase_order_ids)} purchase order IDs")
        self.logger.info("Step 2: Getting purchase order details...")
        for po_id in purchase_order_ids:
            url = f"{self.config['api_url']}{self.path}/{po_id}"
            response = self._request("GET", url)
            if response.status_code != 200:
                self.logger.error(f"Error fetching purchase order {po_id}: {response.status_code}")
                continue
            data = response.json()
            if data:
                yield data

class SupplierProductsStream(BaseOptiplyStream):
    """Stream for supplier-product relationships using /Optiply/GetProductSuppliersFromDate/{unix}."""
    name = "supplier_products"
    primary_keys = ["productSupplierId"]

    schema = th.PropertiesList(
        th.Property("productSupplierId", th.IntegerType),
        th.Property("productId", th.IntegerType),
        th.Property("supplierId", th.IntegerType),
        th.Property("supplierProductNumber", th.StringType),
        th.Property("minOrderQuantity", th.NumberType),
        th.Property("preferredDefaultSupplier", th.BooleanType),
        th.Property("recommendedSalesPriceInc", th.NumberType),
        th.Property("expectedArrivalDatetime", th.DateTimeType),
        th.Property("availabilityStatusId", th.IntegerType),
        th.Property("supplierStock2", th.StringType),
        th.Property("lastModified", th.DateTimeType),
        # Flattened productPurchasePrice fields
        th.Property("productPurchasePriceId", th.IntegerType),
        th.Property("purchasePriceEx", th.NumberType),
    ).to_dict()

    def __init__(self, tap: TapVendit):
        super().__init__(tap)
        self.tap = tap

    def get_url(self, unix_ms: int) -> str:
        return f"{self.tap.config['api_url']}/Optiply/GetProductSuppliersFromDate/{unix_ms}"

    def get_records(self, context: dict | None) -> Iterable[dict]:
        # Override to handle the flattened productPurchasePrice
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
        
        data = response.json()
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
    """Stream for purchase orders using /Optiply/GetProductPurchaseOrdersFromDate/{unix}."""
    name = "purchase_orders_optiply"
    primary_keys = ["productPurchaseOrderId"]

    schema = th.PropertiesList(
        # Purchase Order Header fields
        th.Property("productPurchaseOrderId", th.IntegerType),
        th.Property("officeId", th.IntegerType),
        th.Property("purchaseOrderNumber", th.StringType),
        th.Property("supplierId", th.IntegerType),
        th.Property("orderDatetime", th.DateTimeType),
        th.Property("orderReference", th.StringType),
        th.Property("preorderEmployeeId", th.IntegerType),
        th.Property("employeeId", th.IntegerType),
        th.Property("onlineOrderReference", th.StringType),
        th.Property("orderRemark", th.StringType),
        # Nested details structure
        th.Property(
            "details",
            th.ObjectType(
                th.Property(
                    "lookups",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("fieldName", th.StringType),
                            th.Property(
                                "lookupValues",
                                th.ObjectType(
                                    th.Property(
                                        "items",
                                        th.ArrayType(
                                            get_product_schema()
                                        )
                                    )
                                )
                            ),
                            th.Property("lookupType", th.StringType),
                            th.Property("pkType", th.StringType),
                        )
                    )
                ),
                th.Property(
                    "items",
                    th.ArrayType(
                        get_purchase_order_detail_schema()
                    )
                )
            )
        ),
        # Unix timestamp for incremental processing
        th.Property("unix_timestamp", th.IntegerType),
    ).to_dict()

    def __init__(self, tap: TapVendit):
        super().__init__(tap)
        self.tap = tap

    def get_url(self, unix_ms: int) -> str:
        return f"{self.tap.config['api_url']}/Optiply/GetProductPurchaseOrdersFromDate/{unix_ms}"
