"""Stream type classes for tap-vendit."""

from __future__ import annotations

import typing as t
from importlib import resources

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
    """Products GetMultiple stream (POST)."""

    name = "products_get_multiple"
    path = "/VenditPublicApi/Products/GetMultiple"
    primary_keys: t.ClassVar[list[str]] = ["Id"]
    replication_key = None  # Not incremental by default
    
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

    def request_body_json(self, context: t.Optional[dict]) -> dict:
        return {"primaryKeys": [1]}

    def request_method(self) -> str:
        return "POST"

    def request_records(self, context: t.Optional[dict]) -> t.Iterable[dict]:
        """Request records from the API."""
        prepared_request = self.prepare_request(context, next_page_token=None)
        print("Request URL:", prepared_request.url)
        print("Request Headers:", prepared_request.headers)
        resp = self._request(prepared_request, context)
        yield from self.parse_response(resp)
