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

class SuppliersGetMultipleStream(VenditStream):
    """Suppliers GetMultiple stream (POST with array of supplier IDs)."""

    name = "suppliers_get_multiple"
    path = "/VenditPublicApi/Suppliers/GetMultiple"
    primary_keys = ["supplierId"]
    replication_key = None
    records_jsonpath = "$.items[*]"
    BATCH_SIZE = 100  # Process IDs in batches of 100

    schema = th.PropertiesList(
        th.Property("supplierId", th.IntegerType),
        th.Property("supplierName", th.StringType),
        th.Property("supplierEmail", th.StringType),
        th.Property("supplierWebsite", th.StringType),
        th.Property("supplierBankAccount", th.StringType),
        th.Property("supplierInfo", th.StringType),
        th.Property("supplierNumber", th.StringType),
        th.Property("supplierDebtNumber", th.StringType),
        th.Property("orderSettingEnum", th.IntegerType),
        th.Property("orderCommunicationServiceId", th.IntegerType),
        th.Property("orderLayoutId", th.IntegerType),
        th.Property("orderEmailAttachmentFilename", th.StringType),
        th.Property("orderEmailTo", th.StringType),
        th.Property("orderEmailToCc", th.StringType),
        th.Property("stockinfoCommunicationServiceId", th.IntegerType),
        th.Property("stockinfoUrl", th.StringType),
        th.Property("deliveryCommunicationServiceId", th.IntegerType),
        th.Property("minOrderAmount", th.NumberType),
        th.Property("stockLookupTypeEnum", th.IntegerType),
        th.Property("supplierGroupBitvalue", th.IntegerType),
        th.Property("purchaseInvoiceAllSuppliers", th.BooleanType),
        th.Property("extraInfo", th.StringType),
        th.Property("orderCommunicationServiceLayoutId", th.IntegerType),
        th.Property("orderFtpServer", th.StringType),
        th.Property("orderFtpPort", th.IntegerType),
        th.Property("orderFtpPath", th.StringType),
        th.Property("orderFtpFilename", th.StringType),
        th.Property("orderFtpPassiveMode", th.BooleanType),
        th.Property("orderFtpSftp", th.BooleanType),
        th.Property("orderEmailSubject", th.StringType),
        th.Property("orderEmailAsAttachment", th.BooleanType),
        th.Property("orderLocalFilePath", th.StringType),
        th.Property("orderLocalFileFilename", th.StringType),
        th.Property("glnNumber", th.StringType),
        th.Property("supplierImageUrl", th.StringType),
        th.Property("minimumOrderAmount", th.NumberType),
        th.Property("extraCosts", th.NumberType),
        th.Property("productPriceCommunicationServiceId", th.IntegerType),
        th.Property("deliveryAutoAddProductSupplier", th.BooleanType),
        th.Property("isDosImport", th.BooleanType),
        th.Property("ibanNumber", th.StringType),
        th.Property("bicNumber", th.StringType),
        th.Property("swiftNumber", th.StringType),
        th.Property("vatNumber", th.StringType),
        th.Property("deliveryDays", th.IntegerType),
        th.Property("disableAutoPurchasePriceUpdate", th.BooleanType),
        th.Property("supplierGuid", th.StringType),
        th.Property("purchaseInvoiceSupplierId", th.IntegerType),
        th.Property("isNotRemovable", th.BooleanType),
        th.Property("orderConfirmationEmailTo", th.StringType),
        th.Property("orderFtpType", th.IntegerType),
        th.Property("orderFtpTimeout", th.IntegerType),
        th.Property("allowDropshipment", th.BooleanType),
        th.Property("dropshipmentLayoutid", th.IntegerType),
        th.Property("dropshipmentEmailasattachment", th.BooleanType),
        th.Property("dropshipmentEmailattachmentfilename", th.StringType),
        th.Property("dropshipmentEmailto", th.StringType),
        th.Property("dropshipmentEmailtoCc", th.StringType),
        th.Property("dropshipmentEmailtosubject", th.StringType),
        th.Property("orderFtpUseAscii", th.BooleanType),
        th.Property("bicAndSwift", th.StringType),
        th.Property("banknummerAndIBAN", th.StringType),
    ).to_dict()

    def __init__(self, tap: TapVendit):
        super().__init__(tap)
        self.requests = requests.Session()

    def request_method(self) -> str:
        """Return the HTTP method to use."""
        return "POST"

    def request_body_json(self, context: dict | None) -> dict | None:
        supplier_ids = context.get("supplier_ids", []) if context else []
        return {"primaryKeys": supplier_ids}

    def get_records(self, context: Optional[Dict]) -> Iterable[Dict[str, Any]]:
        """Get records from the stream."""
        supplier_ids = context.get("supplier_ids", []) if context else []
        if not supplier_ids:
            self.logger.warning("No supplier IDs provided in context")
            return

        # Process IDs in batches
        for i in range(0, len(supplier_ids), self.BATCH_SIZE):
            batch = supplier_ids[i:i + self.BATCH_SIZE]
            self.logger.info(f"Processing batch of {len(batch)} supplier IDs")
            
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
                    self.logger.error(f"Error fetching suppliers: {response.status_code}")
                    self.logger.error(response.text)
                    continue
                    
                data = response.json()
                if not data or not data.get("items"):
                    self.logger.warning("No items found in response")
                    continue
                    
                # Yield each supplier record
                for supplier in data["items"]:
                    yield supplier
                    
            except Exception as e:
                self.logger.error(f"Error processing batch: {str(e)}")
                continue

class SuppliersFindStream(VenditStream):
    """Stream for finding supplier IDs."""
    name = "suppliers_find"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
    ).to_dict()

    def __init__(self, tap: TapVendit):
        super().__init__(tap)
        self.tap = tap  # Ensure tap attribute is set
        self.requests = requests.Session()

    def get_url(self) -> str:
        """Get the URL for the stream."""
        return f"{self.tap.config['api_url']}/VenditPublicApi/Suppliers/Find"

    def get_all_supplier_ids(self, start_date: str, end_date: str) -> List[str]:
        """Get all supplier IDs using pagination."""
        self.logger.info(f"Fetching all supplier IDs from {start_date} to {end_date}...")
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
                                "field": 524,  # Assuming same field ID for date filtering
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
                    self.logger.warning("[get_all_supplier_ids] 401 received, refreshing token...")
                    self.authenticator._get_token()
                    continue
                    
                if response.status_code != 200:
                    self.logger.error(f"Error fetching suppliers: {response.status_code}")
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
                
        self.logger.info(f"Found total of {len(all_ids)} supplier IDs")
        return all_ids

    def get_records(self, context: Optional[Dict]) -> Iterable[Dict[str, Any]]:
        """Get records from the stream."""
        start_date = self.config.get("start_date")
        end_date = self.config.get("end_date")
        
        if not start_date or not end_date:
            self.logger.error("start_date and end_date are required in config")
            return
            
        # Get all supplier IDs first
        supplier_ids = self.get_all_supplier_ids(start_date, end_date)
        
        if not supplier_ids:
            self.logger.warning("No supplier IDs found")
            return
            
        # Use the GetMultiple stream to fetch supplier details
        get_multiple_stream = SuppliersGetMultipleStream(self.tap)
        yield from get_multiple_stream.get_records({"supplier_ids": supplier_ids})

class OrdersGetMultipleStream(VenditStream):
    """Orders GetMultiple stream (POST with array of order IDs)."""

    name = "orders_get_multiple"
    path = "/VenditPublicApi/Orders/GetMultiple"
    primary_keys = ["customerOrderHeaderId"]
    replication_key = None
    records_jsonpath = "$.items[*]"
    BATCH_SIZE = 100  # Process IDs in batches of 100

    schema = th.PropertiesList(
        th.Property("customerOrderHeaderId", th.IntegerType),
        th.Property("orderDetails", th.StringType, required=False),
        th.Property("customerId", th.IntegerType),
        th.Property("officeId", th.IntegerType),
        th.Property("employeeId", th.IntegerType),
        th.Property("planningId", th.IntegerType, required=False),
        th.Property("customerOrderNumber", th.StringType),
        th.Property("creationDatetime", th.DateTimeType),
        th.Property("orderTypeId", th.IntegerType),
        th.Property("orderPriorityId", th.IntegerType),
        th.Property("deliveryDate", th.DateTimeType),
        th.Property("activationDate", th.DateTimeType),
        th.Property("orderStatusId", th.IntegerType),
        th.Property("partialDeliveryNotification", th.BooleanType),
        th.Property("completeDelivery", th.BooleanType),
        th.Property("deliveryNotificationType", th.IntegerType),
        th.Property("partialCompletedOnly", th.BooleanType),
        th.Property("invoiceAddressId", th.IntegerType),
        th.Property("invoiceContactId", th.IntegerType),
        th.Property("deliveryAddressId", th.IntegerType),
        th.Property("deliveryContactId", th.IntegerType),
        th.Property("journalId", th.IntegerType),
        th.Property("stockStatusEnum", th.IntegerType),
        th.Property("saleExVat", th.BooleanType),
        th.Property("turnoverEmployeeId", th.IntegerType),
        th.Property("invoiceDiscountPercentage", th.NumberType, required=False),
        th.Property("invoiceDiscountAmount", th.NumberType, required=False),
        th.Property("discountIsPercentage", th.BooleanType, required=False),
        th.Property("baselineReferenceNumber", th.StringType),
        th.Property("orderLabelNumber", th.StringType),
        th.Property("orderStatusDate", th.DateTimeType),
        th.Property("orderStatusDateFormat", th.StringType),
        th.Property("orderSubTitle", th.StringType),
        th.Property("ecommerceWebsiteGuid", th.StringType, required=False),
        th.Property("invoiceDebitCustomerId", th.IntegerType),
        th.Property("telecomReferenceNumber", th.StringType),
        th.Property("originalWorkstationId", th.IntegerType, required=False),
        th.Property("originalDrawerId", th.IntegerType, required=False),
        th.Property("isEcommerce", th.BooleanType),
        th.Property("internalMemo", th.StringType),
        th.Property("statusEmployeeId", th.IntegerType, required=False),
        th.Property("url", th.StringType),
        th.Property("dropshipmentType", th.IntegerType, required=False),
        th.Property("intersolveLiabCardnumber", th.StringType),
        th.Property("purchaseOrderNumber", th.StringType),
        th.Property("intersolveLiabViaZipcode", th.StringType, required=False),
        th.Property("orderOrigin", th.StringType),
        th.Property("pickStatusEnum", th.IntegerType),
        th.Property(
            "downPayments",
            th.ObjectType(
                th.Property(
                    "items",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("orderDownPaymentId", th.IntegerType),
                            th.Property("customerOrderHeaderId", th.IntegerType),
                            th.Property("paymentDate", th.DateTimeType),
                            th.Property("paymentAmount", th.NumberType),
                            th.Property("restAmount", th.NumberType),
                            th.Property("paymentCodeId", th.IntegerType),
                            th.Property("processingDate", th.DateTimeType),
                            th.Property("paymentTypeId", th.IntegerType)
                        )
                    )
                )
            )
        )
    ).to_dict()

    def __init__(self, tap: TapVendit):
        super().__init__(tap)
        self.requests = requests.Session()

    def request_method(self) -> str:
        """Return the HTTP method to use."""
        return "POST"

    def request_body_json(self, context: dict | None) -> dict | None:
        order_ids = context.get("order_ids", []) if context else []
        return {"primaryKeys": order_ids}

    def get_records(self, context: Optional[Dict]) -> Iterable[Dict[str, Any]]:
        """Get records from the stream."""
        order_ids = context.get("order_ids", []) if context else []
        if not order_ids:
            self.logger.warning("No order IDs provided in context")
            return

        # Process IDs in batches
        for i in range(0, len(order_ids), self.BATCH_SIZE):
            batch = order_ids[i:i + self.BATCH_SIZE]
            self.logger.info(f"Processing batch of {len(batch)} order IDs")
            
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
                    self.logger.error(f"Error fetching orders: {response.status_code}")
                    self.logger.error(response.text)
                    continue
                    
                data = response.json()
                if not data or not data.get("items"):
                    self.logger.warning("No items found in response")
                    continue
                    
                # Yield each order record
                for order in data["items"]:
                    yield order
                    
            except Exception as e:
                self.logger.error(f"Error processing batch: {str(e)}")
                continue

class OrdersFindStream(VenditStream):
    """Stream for finding order IDs."""
    name = "orders_find"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
    ).to_dict()

    def __init__(self, tap: TapVendit):
        super().__init__(tap)
        self.tap = tap  # Ensure tap attribute is set
        self.requests = requests.Session()

    def get_url(self) -> str:
        """Get the URL for the stream."""
        return f"{self.tap.config['api_url']}/VenditPublicApi/Orders/Find"

    def get_all_order_ids(self, start_date: str, end_date: str) -> List[str]:
        """Get all order IDs using pagination."""
        self.logger.info(f"Fetching all order IDs from {start_date} to {end_date}...")
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
                                "field": 524,  # Assuming same field ID for date filtering
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
                    self.logger.warning("[get_all_order_ids] 401 received, refreshing token...")
                    self.authenticator._get_token()
                    continue
                    
                if response.status_code != 200:
                    self.logger.error(f"Error fetching orders: {response.status_code}")
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
                
        self.logger.info(f"Found total of {len(all_ids)} order IDs")
        return all_ids

    def get_records(self, context: Optional[Dict]) -> Iterable[Dict[str, Any]]:
        """Get records from the stream."""
        start_date = self.config.get("start_date")
        end_date = self.config.get("end_date")
        
        if not start_date or not end_date:
            self.logger.error("start_date and end_date are required in config")
            return
            
        # Get all order IDs first
        order_ids = self.get_all_order_ids(start_date, end_date)
        
        if not order_ids:
            self.logger.warning("No order IDs found")
            return
            
        # Use the GetMultiple stream to fetch order details
        get_multiple_stream = OrdersGetMultipleStream(self.tap)
        yield from get_multiple_stream.get_records({"order_ids": order_ids})
