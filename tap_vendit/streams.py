"""Stream type classes for tap-vendit."""

from __future__ import annotations

import typing as t
from importlib import resources
from datetime import datetime
from typing import List, Dict, Any, Optional, Iterable
import json
import os
import threading
import requests

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_vendit.client import VenditStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = resources.files(__package__) / "schemas"
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class UsersStream(VenditStream):
    """Define custom stream."""

    name = "users"
    path = "/users"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"  # noqa: ERA001
    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property(
            "id",
            th.StringType,
            description="The user's system ID",
        ),
        th.Property(
            "age",
            th.IntegerType,
            description="The user's age in years",
        ),
        th.Property(
            "email",
            th.StringType,
            description="The user's email address",
        ),
        th.Property("street", th.StringType),
        th.Property("city", th.StringType),
        th.Property(
            "state",
            th.StringType,
            description="State name in ISO 3166-2 format",
        ),
        th.Property("zip", th.StringType),
    ).to_dict()


class GroupsStream(VenditStream):
    """Define custom stream."""

    name = "groups"
    path = "/groups"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "modified"
    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("id", th.StringType),
        th.Property("modified", th.DateTimeType),
    ).to_dict()

class ProductsStream(VenditStream):
    """Products stream."""

    name = "products"
    path = "/VenditPublicApi/Products"
    primary_keys: t.ClassVar[list[str]] = ["Id"]
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("Id", th.IntegerType),
        th.Property("Code", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Created", th.DateTimeType),
        th.Property("Price", th.NumberType),
        th.Property("Stock", th.NumberType),
        th.Property("Status", th.StringType),
        th.Property("Category", th.StringType),
        th.Property("Brand", th.StringType),
        th.Property("Barcode", th.StringType),
        th.Property("Weight", th.NumberType),
        th.Property("Volume", th.NumberType),
        th.Property("TaxRate", th.NumberType),
        th.Property("GroupId", th.IntegerType),
        th.Property("SupplierId", th.IntegerType),
        th.Property("MinimumStock", th.NumberType),
        th.Property("MaximumStock", th.NumberType),
        th.Property("PurchasePrice", th.NumberType),
        th.Property("SalesPrice", th.NumberType),
    ).to_dict()

class ProductGroupsStream(VenditStream):
    """Product Groups stream."""

    name = "product_groups"
    path = "/VenditPublicApi/ProductGroups"
    primary_keys: t.ClassVar[list[str]] = ["Id"]
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("Id", th.IntegerType),
        th.Property("Code", th.StringType),
        th.Property("Name", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Created", th.DateTimeType),
        th.Property("ParentId", th.IntegerType),
        th.Property("Level", th.IntegerType),
        th.Property("Status", th.StringType),
    ).to_dict()

class SuppliersStream(VenditStream):
    """Suppliers stream."""

    name = "suppliers"
    path = "/VenditPublicApi/Suppliers"
    primary_keys: t.ClassVar[list[str]] = ["Id"]
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("Id", th.IntegerType),
        th.Property("Code", th.StringType),
        th.Property("Name", th.StringType),
        th.Property("Email", th.StringType),
        th.Property("Phone", th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Created", th.DateTimeType),
        th.Property("Status", th.StringType),
        th.Property("Type", th.StringType),
        th.Property("TaxNumber", th.StringType),
        th.Property("Notes", th.StringType),
        th.Property("Address", th.StringType),
        th.Property("City", th.StringType),
        th.Property("Country", th.StringType),
        th.Property("PostalCode", th.StringType),
    ).to_dict()

class ProductSuppliersStream(VenditStream):
    """Product Suppliers stream."""

    name = "product_suppliers"
    path = "/VenditPublicApi/ProductSuppliers"
    primary_keys: t.ClassVar[list[str]] = ["Id"]
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("Id", th.IntegerType),
        th.Property("ProductId", th.IntegerType),
        th.Property("SupplierId", th.IntegerType),
        th.Property("SupplierProductCode", th.StringType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Created", th.DateTimeType),
        th.Property("Status", th.StringType),
        th.Property("LeadTime", th.IntegerType),
        th.Property("MinimumOrderQuantity", th.NumberType),
        th.Property("PurchasePrice", th.NumberType),
        th.Property("Currency", th.StringType),
    ).to_dict()

class OrdersStream(VenditStream):
    """Orders stream."""

    name = "orders"
    path = "/VenditPublicApi/Orders"
    primary_keys: t.ClassVar[list[str]] = ["Id"]
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("Id", th.IntegerType),
        th.Property("OrderNumber", th.StringType),
        th.Property("CustomerId", th.IntegerType),
        th.Property("OrderDate", th.DateTimeType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Status", th.StringType),
        th.Property("TotalAmount", th.NumberType),
        th.Property("TaxAmount", th.NumberType),
        th.Property("ShippingAmount", th.NumberType),
        th.Property("DiscountAmount", th.NumberType),
        th.Property("PaymentMethod", th.StringType),
        th.Property("ShippingMethod", th.StringType),
        th.Property("Notes", th.StringType),
        th.Property("Currency", th.StringType),
        th.Property("ExchangeRate", th.NumberType),
        th.Property("DeliveryDate", th.DateTimeType),
        th.Property("InvoiceNumber", th.StringType),
    ).to_dict()

class PurchaseOrdersStream(VenditStream):
    """Purchase Orders stream."""

    name = "purchase_orders"
    path = "/VenditPublicApi/PurchaseOrders"
    primary_keys: t.ClassVar[list[str]] = ["Id"]
    replication_key = "Modified"

    schema = th.PropertiesList(
        th.Property("Id", th.IntegerType),
        th.Property("OrderNumber", th.StringType),
        th.Property("SupplierId", th.IntegerType),
        th.Property("OrderDate", th.DateTimeType),
        th.Property("Modified", th.DateTimeType),
        th.Property("Status", th.StringType),
        th.Property("TotalAmount", th.NumberType),
        th.Property("TaxAmount", th.NumberType),
        th.Property("ShippingAmount", th.NumberType),
        th.Property("DiscountAmount", th.NumberType),
        th.Property("PaymentMethod", th.StringType),
        th.Property("ShippingMethod", th.StringType),
        th.Property("Notes", th.StringType),
        th.Property("Currency", th.StringType),
        th.Property("ExchangeRate", th.NumberType),
        th.Property("ExpectedDeliveryDate", th.DateTimeType),
        th.Property("InvoiceNumber", th.StringType),
    ).to_dict()

class ProductsGetMultipleStream(VenditStream):
    """Products GetMultiple stream (POST with array of product IDs)."""

    name = "products_get_multiple"
    path = "/VenditPublicApi/Products/GetMultiple"
    primary_keys = ["productId"]
    replication_key = None
    records_jsonpath = "$.items[*]"
    BATCH_SIZE = 100  # Process IDs in batches of 100

    schema = th.PropertiesList(
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
        th.Property("productLabelLayoutId", th.IntegerType),
        th.Property("cabProfileCode", th.IntegerType),
        th.Property("allowInvoiceDiscount", th.BooleanType),
        th.Property("lastModified", th.DateTimeType),
        th.Property("productGuid", th.StringType),
        th.Property("memoCommon", th.StringType),
        th.Property("memoEcommerce", th.StringType),
        th.Property("productSize", th.StringType),
        th.Property("productColor", th.StringType),
        th.Property("useGroupInvoiceDiscount", th.BooleanType),
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
        th.Property("originCountryCode", th.StringType),
        th.Property("originCountry", th.StringType),
    ).to_dict()

    def __init__(self, tap: TapVendit):
        super().__init__(tap)
        self.requests = requests.Session()

    def request_method(self) -> str:
        """Return the HTTP method to use."""
        return "POST"

    def request_body_json(self, context: dict | None) -> dict | None:
        product_ids = context.get("product_ids", []) if context else []
        return {"primaryKeys": product_ids}

    def get_records(self, context: Optional[Dict]) -> Iterable[Dict[str, Any]]:
        """Get records from the stream."""
        product_ids = context.get("product_ids", []) if context else []
        if not product_ids:
            self.logger.warning("No product IDs provided in context")
            return

        # Process IDs in batches
        for i in range(0, len(product_ids), self.BATCH_SIZE):
            batch = product_ids[i:i + self.BATCH_SIZE]
            self.logger.info(f"Processing batch of {len(batch)} product IDs")
            
            try:
                # Convert IDs to integers if they're strings
                batch = [int(id) for id in batch]
                
                response = self.requests.post(
                    f"{self.config['api_url']}{self.path}",
                    json={"primaryKeys": batch},
                    headers=self.authenticator.auth_headers
                )
                
                if response.status_code == 401:
                    self.logger.warning("401 received, refreshing token...")
                    self.authenticator._get_token()
                    continue
                    
                if response.status_code != 200:
                    self.logger.error(f"Error fetching products: {response.status_code}")
                    self.logger.error(response.text)
                    continue
                    
                data = response.json()
                if not data or not data.get("items"):
                    self.logger.warning("No items found in response")
                    continue
                    
                # Yield each product record
                for product in data["items"]:
                    yield product
                    
            except Exception as e:
                self.logger.error(f"Error processing batch: {str(e)}")
                continue

def read_token_from_secrets():
    secrets_path = os.path.abspath('secrets.json')
    if not os.path.exists(secrets_path):
        return None, 0
    with open(secrets_path, 'r') as f:
        secrets = json.load(f)
    return secrets.get('token'), secrets.get('token_expire', 0)

def write_token_to_secrets(token, token_expire):
    secrets_path = os.path.abspath('secrets.json')
    lock = threading.Lock()
    with lock:
        if os.path.exists(secrets_path):
            with open(secrets_path, 'r') as f:
                secrets = json.load(f)
        else:
            secrets = {}
        secrets['token'] = token
        secrets['token_expire'] = token_expire
        with open(secrets_path, 'w') as f:
            json.dump(secrets, f, indent=4)

class ProductsFindStream(VenditStream):
    """Stream for finding product IDs."""
    name = "products_find"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
    ).to_dict()

    def __init__(self, tap: TapVendit):
        super().__init__(tap)
        self.tap = tap  # Ensure tap attribute is set
        self.requests = requests.Session()

    def get_url(self) -> str:
        """Get the URL for the stream."""
        return f"{self.tap.config['api_url']}/VenditPublicApi/Products/Find"

    def get_all_product_ids(self, start_date: str, end_date: str) -> List[str]:
        """Get all product IDs using pagination."""
        self.logger.info(f"Fetching all product IDs from {start_date} to {end_date}...")
        all_ids = []
        offset = 0
        page_size = 100  # API always returns 100 results per page
        
        while True:
            try:
                # Use the authenticated session from the parent class
                response = self.requests.post(
                    self.get_url(),
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
                        "paginationLimit": page_size,
                        "operator": 0
                    },
                    headers=self.authenticator.auth_headers
                )
                
                if response.status_code == 401:
                    self.logger.warning("[get_all_product_ids] 401 received, refreshing token...")
                    self.authenticator._get_token()
                    continue
                    
                if response.status_code != 200:
                    self.logger.error(f"Error fetching products: {response.status_code}")
                    self.logger.error(response.text)
                    break
                    
                data = response.json()
                if not data or not data.get("results"):  # No more results
                    break
                    
                # Extract IDs from the response
                ids = data["results"]
                all_ids.extend(ids)
                self.logger.info(f"Retrieved {len(ids)} IDs in current page, total so far: {len(all_ids)}")
                
                # Check if we've received fewer results than the page size
                if len(ids) < page_size:  # Last page
                    break
                    
                # Increment offset for next page
                offset += page_size
                
            except Exception as e:
                self.logger.error(f"Error during pagination: {str(e)}")
                break
                
        self.logger.info(f"Found total of {len(all_ids)} product IDs")
        return all_ids

    def get_records(self, context: Optional[Dict]) -> Iterable[Dict[str, Any]]:
        """Get records from the stream."""
        start_date = self.config.get("start_date")
        end_date = self.config.get("end_date")
        
        if not start_date or not end_date:
            self.logger.error("start_date and end_date are required in config")
            return
            
        # Get all product IDs first
        product_ids = self.get_all_product_ids(start_date, end_date)
        
        if not product_ids:
            self.logger.warning("No product IDs found")
            return
            
        # Return each product ID as a separate record
        for product_id in product_ids:
            yield {
                "id": product_id,
                "name": None,  # These fields will be populated by ProductsGetMultipleStream
                "description": None,
                "price": None,
                "created_at": None,
                "updated_at": None
            }
