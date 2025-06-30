"""Vendit tap class."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_vendit.streams import (
    ProductsStream,
    SuppliersStream,
    OrdersStream,
    PurchaseOrdersStream,
    PurchaseOrdersOptiplyStream,
    SupplierProductsStream,
    OrdersOptiplyStream,
)

STREAM_TYPES = [
    ProductsStream,
    SuppliersStream,
    OrdersStream,
    PurchaseOrdersStream,
    PurchaseOrdersOptiplyStream,
    SupplierProductsStream,
    OrdersOptiplyStream,
]

class TapVendit(Tap):
    """Vendit tap class."""

    name = "tap-vendit"

    # Only non-sensitive config remains in config_jsonschema
    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_url",
            th.StringType(),
            default="https://api.staging.vendit.online",
            description="The url for the Vendit API service",
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
        th.Property("state_file", th.StringType, required=False),
        th.Property("sync_endpoints", th.BooleanType, required=False),
    ).to_dict()

    def __init__(
        self,
        config: Dict[str, Any] | None = None,
        catalog: Dict[str, Any] | None = None,
        state: Dict[str, Any] | None = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        **kwargs,
    ) -> None:
        """Initialize the tap.

        Args:
            config: Tap configuration.
            catalog: Stream catalog.
            state: Tap state.
            parse_env_config: Whether to look for config values in environment variables.
            validate_config: Whether to validate the config.
        """
        super().__init__(
            config=config,
            catalog=catalog,
            state=state,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
            **kwargs,
        )
        # Make a mutable copy of the config
        self._config = dict(self.config)
        
        # Try to load secrets.json first, then fall back to config.json
        secrets_path = os.path.abspath("secrets.json")
        config_path = os.path.abspath("config.json")
        
        if os.path.exists(secrets_path):
            with open(secrets_path) as f:
                secrets = json.load(f)
            print(f"[DEBUG] Loaded secrets from {secrets_path}")
            self._config.update(secrets)
            self._config["config_file"] = secrets_path
        elif os.path.exists(config_path):
            with open(config_path) as f:
                config_data = json.load(f)
            print(f"[DEBUG] Loaded config from {config_path}")
            self._config.update(config_data)
            self._config["config_file"] = config_path
        else:
            raise RuntimeError("Neither secrets.json nor config.json found. Please provide credentials.")
        
        # Do NOT instantiate self.authenticator here; streams will handle their own authenticators.

    @property
    def config(self) -> dict:
        """Return the mutable config dictionary."""
        return self._config

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

    def sync_all(self) -> None:
        """Sync all streams."""
        try:
            # First get the product IDs
            find_stream = ProductsStream(self)
            product_ids = []
            
            # Collect all product IDs from the find stream
            for record in find_stream.get_records(context=None):
                if isinstance(record, dict) and "id" in record:
                    product_ids.append(record["id"])
            
            if not product_ids:
                self.logger.warning("No product IDs found from ProductsStream")
                return
                
            self.logger.info(f"Found {len(product_ids)} product IDs to process")
            
            # Then get the full product details using the stream's sync method
            get_multiple_stream = ProductsStream(self)
            get_multiple_stream.sync(context={"product_ids": product_ids})
            
            self.logger.info(f"Successfully processed product records")
            
        except Exception as e:
            self.logger.error(f"Error during sync: {str(e)}")
            raise


if __name__ == "__main__":
    TapVendit.cli()
