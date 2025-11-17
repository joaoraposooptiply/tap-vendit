"""Vendit tap class."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Type

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.helpers._compat import final

# TODO: Import your custom stream types here:
from tap_vendit.streams import (
    ProductsStream,
    SuppliersStream,
    OrdersStream,
    PurchaseOrdersStream,
    PurchaseOrdersOptiplyStream,
    SupplierProductsStream,
    OrdersOptiplyStream,
    StockChangesStream,
    PrePurchaseOrdersStream,
    HistoryPurchaseOrdersStream,
    SellOrderTransactionsStream,
    TransactionsStream,
)

STREAM_TYPES = [
    ProductsStream,
    SuppliersStream,
    OrdersStream,
    PurchaseOrdersStream,
    PurchaseOrdersOptiplyStream,
    SupplierProductsStream,
    OrdersOptiplyStream,
    StockChangesStream,
    PrePurchaseOrdersStream,
    HistoryPurchaseOrdersStream,
    SellOrderTransactionsStream,
    TransactionsStream,
]

class TapVendit(Tap):
    """Vendit tap class."""

    name = "tap-vendit"

    def __init__(
        self,
        config=None,
        catalog=None,
        state=None,
        parse_env_config=False,
        validate_config=True,
        **kwargs,
    ) -> None:
        if config:
            self.config_file = config[0]
        super().__init__(config=config, catalog=catalog, state=state, parse_env_config=parse_env_config, validate_config=validate_config, **kwargs)

    # Only non-sensitive config remains in config_jsonschema
    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_url",
            th.StringType(),
            default="https://api2.vendit.online",
            description="The url for the Vendit API service",
        ),
        th.Property(
            "oauth_url",
            th.StringType(),
            description="The url for the Vendit OAuth token endpoint (optional, defaults to production if not provided)",
        ),
        th.Property(
            "start_date",
            th.DateTimeType(),
            description="The earliest record date to sync",
        ),
        th.Property(
            "end_date",
            th.DateTimeType(),
            description="The latest record date to sync",
        ),
        th.Property(
            "user_agent",
            th.StringType(),
            description=(
                "A custom User-Agent header to send with each request. Default is "
                "<tap_name>/<tap_version>"
            ),
        ),
        th.Property(
            "verify_ssl",
            th.BooleanType(),
            default=True,
            description="Whether to verify SSL certificates. Set to false for self-signed certificates.",
        ),
        th.Property(
            "connection_pool_size",
            th.IntegerType(),
            default=10,
            description="Number of connections to pool for HTTP requests.",
        ),
        th.Property(
            "max_retries",
            th.IntegerType(),
            default=3,
            description="Maximum number of retries for failed requests.",
        ),
        th.Property("state_file", th.StringType, required=False),
        th.Property("sync_endpoints", th.BooleanType, required=False),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
    
    def validate_config(self) -> None:
        """Validate the configuration."""
        super().validate_config()
        
        # Validate required fields
        required_fields = ["username", "password", "vendit_api_key"]
        missing_fields = [field for field in required_fields if not self.config.get(field)]
        
        if missing_fields:
            raise ValueError(f"Missing required configuration fields: {', '.join(missing_fields)}")
        
        # Validate API URL format
        api_url = self.config.get("api_url", "https://api2.vendit.online")
        if not api_url.startswith(("http://", "https://")):
            raise ValueError("api_url must start with http:// or https://")
        
        # Validate OAuth URL format if provided
        oauth_url = self.config.get("oauth_url")
        if oauth_url and not oauth_url.startswith(("http://", "https://")):
            raise ValueError("oauth_url must start with http:// or https://")
        
        # Validate date formats
        start_date = self.config.get("start_date")
        if start_date:
            try:
                from datetime import datetime
                if isinstance(start_date, str):
                    datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            except ValueError:
                raise ValueError("start_date must be in ISO format (e.g., '2020-01-01T00:00:00Z')")
        
        # Validate numeric fields
        connection_pool_size = self.config.get("connection_pool_size", 10)
        if not isinstance(connection_pool_size, int) or connection_pool_size < 1:
            raise ValueError("connection_pool_size must be a positive integer")
        
        max_retries = self.config.get("max_retries", 3)
        if not isinstance(max_retries, int) or max_retries < 0:
            raise ValueError("max_retries must be a non-negative integer")

if __name__ == "__main__":
    TapVendit.cli()
