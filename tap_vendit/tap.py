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

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

if __name__ == "__main__":
    TapVendit.cli()
