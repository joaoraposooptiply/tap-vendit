"""Vendit tap class."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_vendit import streams


class TapVendit(Tap):
    """Vendit tap class."""

    name = "tap-vendit"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType(),
            required=True,
            secret=True,
            description="The API key for Vendit API access",
        ),
        th.Property(
            "username",
            th.StringType(),
            required=True,
            secret=True,
            description="The username for Vendit API access",
        ),
        th.Property(
            "password",
            th.StringType(),
            required=True,
            secret=True,
            description="The password for Vendit API access",
        ),
        th.Property(
            "api_url",
            th.StringType(),
            default="https://api.vendit.com/v1",
            description="The base URL for the Vendit API",
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
                "'<tap_name>/<tap_version>'"
            ),
        ),
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

    def discover_streams(self) -> list[streams.VenditStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.ProductsFindStream(self),
            streams.ProductsGetMultipleStream(self),
            streams.ProductsStream(self),
            streams.PurchaseOrdersStream(self),
            streams.ProductsGetMultipleStream(self),
        ]

    def sync_all(self) -> None:
        """Sync all streams."""
        try:
            # First get the product IDs
            find_stream = streams.ProductsFindStream(self)
            product_ids = []
            
            # Collect all product IDs from the find stream
            for record in find_stream.get_records(context=None):
                if isinstance(record, dict) and "id" in record:
                    product_ids.append(record["id"])
            
            if not product_ids:
                self.logger.warning("No product IDs found from ProductsFindStream")
                return
                
            self.logger.info(f"Found {len(product_ids)} product IDs to process")
            
            # Then get the full product details using the stream's sync method
            get_multiple_stream = streams.ProductsGetMultipleStream(self)
            get_multiple_stream.sync(context={"product_ids": product_ids})
            
            self.logger.info(f"Successfully processed product records")
            
        except Exception as e:
            self.logger.error(f"Error during sync: {str(e)}")
            raise


if __name__ == "__main__":
    TapVendit.cli()
