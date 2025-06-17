"""Stream type classes for tap-vendit."""

from __future__ import annotations

import typing as t
from importlib import resources
from datetime import datetime
from typing import List, Dict, Any, Optional, Iterable
import json
import os
import threading

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

    def request_method(self) -> str:
        """Return the HTTP method to use."""
        return "POST"

    def request_body_json(self, context: Context | None) -> dict | None:
        """Return the request body as a JSON object."""
        # Get the product IDs from the context
        product_ids = context.get("product_ids", []) if context else []
        return {
            "primaryKeys": product_ids
        }

    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        """Request records from REST endpoint(s), returning response records."""
        if not context or not context.get("product_ids"):
            self.logger.warning("No product IDs provided in context, skipping request")
            return

        product_ids = context["product_ids"]
        total_ids = len(product_ids)
        self.logger.info(f"Processing {total_ids} product IDs in batches of {self.BATCH_SIZE}")

        # Process IDs in batches
        for i in range(0, total_ids, self.BATCH_SIZE):
            batch_ids = product_ids[i:i + self.BATCH_SIZE]
            self.logger.info(f"Processing batch {i//self.BATCH_SIZE + 1} with {len(batch_ids)} IDs")
            
            # Update context with current batch
            batch_context = context.copy()
            batch_context["product_ids"] = batch_ids
            
            prepared_request = self.prepare_request(batch_context)
            resp = self._request(prepared_request, batch_context)
            yield from self.parse_response(resp)

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
    """Stream for Products/Find endpoint with pagination and GetMultiple handling."""
    name = "products_find"
    tap_stream_id = "products_find"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_key = None
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "name": {"type": "string"},
            "description": {"type": ["string", "null"]},
            "price": {"type": ["number", "null"]},
            "created_at": {"type": ["string", "null"], "format": "date-time"},
            "updated_at": {"type": ["string", "null"], "format": "date-time"}
        }
    }

    def get_url(self):
        return f"{self.config['api_url']}/VenditPublicApi/Products/Find"

    def get_all_product_ids(self, start_date: str, end_date: str) -> List[str]:
        """Get all product IDs using pagination."""
        all_ids = []
        offset = 0
        page_size = 500
        token, token_expire = read_token_from_secrets()
        
        while True:
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Token": token,
                "ApiKey": self.config.get('api_key')
            }
            url = self.get_url()
            body = {
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
            self.logger.debug(f"[get_all_product_ids] Using token: {token}")
            self.logger.debug(f"[get_all_product_ids] Headers: {headers}")
            self.logger.debug(f"[get_all_product_ids] URL: {url}")
            self.logger.debug(f"[get_all_product_ids] Body: {body}")
            response = self.session.post(
                url,
                headers=headers,
                json=body
            )
            if response.status_code == 401:
                self.logger.warning("[get_all_product_ids] 401 received, refreshing token...")
                self.authenticator._get_token()
                token = self.authenticator._token
                token_expire = getattr(self.authenticator, '_token_expire', 0)
                write_token_to_secrets(token, token_expire)
                # Re-read token from secrets.json
                token, token_expire = read_token_from_secrets()
                headers["Token"] = token
                self.logger.debug(f"[get_all_product_ids] Retrying with new token: {token}")
                self.logger.debug(f"[get_all_product_ids] Headers: {headers}")
                self.logger.debug(f"[get_all_product_ids] URL: {url}")
                self.logger.debug(f"[get_all_product_ids] Body: {body}")
                response = self.session.post(
                    url,
                    headers=headers,
                    json=body
                )
            if response.status_code != 200:
                self.logger.error(f"Error fetching products: {response.status_code}")
                self.logger.error(response.text)
                break
            data = response.json()
            if not data or not data.get("results"):
                break
            ids = data["results"]
            all_ids.extend(ids)
            if len(ids) < page_size:
                break
            offset += page_size
        return all_ids

    def get_products_in_chunks(self, product_ids: List[str]) -> List[Dict[str, Any]]:
        """Get products in chunks of 100 IDs."""
        all_products = []
        chunk_size = 100
        token, token_expire = read_token_from_secrets()
        for i in range(0, len(product_ids), chunk_size):
            chunk = product_ids[i:i + chunk_size]
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Token": token,
                "ApiKey": self.config.get('api_key')
            }
            url = f"{self.config['api_url']}/VenditPublicApi/Products/GetMultiple"
            body = {"primaryKeys": chunk}
            self.logger.debug(f"[get_products_in_chunks] Using token: {token}")
            self.logger.debug(f"[get_products_in_chunks] Headers: {headers}")
            self.logger.debug(f"[get_products_in_chunks] URL: {url}")
            self.logger.debug(f"[get_products_in_chunks] Body: {body}")
            response = self.session.post(
                url,
                headers=headers,
                json=body
            )
            if response.status_code == 401:
                self.logger.warning("[get_products_in_chunks] 401 received, refreshing token...")
                self.authenticator._get_token()
                token = self.authenticator._token
                token_expire = getattr(self.authenticator, '_token_expire', 0)
                write_token_to_secrets(token, token_expire)
                # Re-read token from secrets.json
                token, token_expire = read_token_from_secrets()
                headers["Token"] = token
                self.logger.debug(f"[get_products_in_chunks] Retrying with new token: {token}")
                self.logger.debug(f"[get_products_in_chunks] Headers: {headers}")
                self.logger.debug(f"[get_products_in_chunks] URL: {url}")
                self.logger.debug(f"[get_products_in_chunks] Body: {body}")
                response = self.session.post(
                    url,
                    headers=headers,
                    json=body
                )
            if response.status_code != 200:
                self.logger.error(f"Error fetching products chunk: {response.status_code}")
                self.logger.error(response.text)
                continue
            products = response.json()
            if isinstance(products, dict) and 'items' in products:
                all_products.extend(products['items'])
            else:
                self.logger.warning('Warning: Unexpected response format from GetMultiple:', products)
        return all_products

    def get_records(self, context: Optional[Dict]) -> Iterable[Dict[str, Any]]:
        """Get all products for the given date range."""
        start_date = self.config.get('start_date')
        end_date = self.config.get('end_date')
        
        if not start_date or not end_date:
            self.logger.error("start_date and end_date must be set in config")
            return []
        
        # First get all IDs
        self.logger.info(f"Fetching all product IDs from {start_date} to {end_date}...")
        all_ids = self.get_all_product_ids(start_date, end_date)
        self.logger.info(f"Found {len(all_ids)} products")
        
        # Then get all products in chunks
        self.logger.info("Fetching product details...")
        all_products = self.get_products_in_chunks(all_ids)
        self.logger.info(f"Retrieved {len(all_products)} product details")
        
        for product in all_products:
            yield product
