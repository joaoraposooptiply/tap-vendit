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

    @final
    def load_streams(self) -> List[Stream]:
        """Load streams from discovery and initialize DAG.

        Return the output of `self.discover_streams()` to enumerate
        discovered streams.

        Returns:
            A list of discovered streams, ordered by name.
        """
        # Build the parent-child dependency DAG

        # Index streams by type
        streams_by_type: Dict[Type[Stream], List[Stream]] = {}
        for stream in self.discover_streams():
            stream_type = type(stream)
            if stream_type not in streams_by_type:
                streams_by_type[stream_type] = []
            streams_by_type[stream_type].append(stream)

        # Initialize child streams list for parents
        for stream_type, streams in streams_by_type.items():
            if stream_type.parent_stream_type and not stream_type.ignore_parent_stream:
                parents = streams_by_type[stream_type.parent_stream_type]
                for parent in parents:
                    for stream in streams:
                        parent.child_streams.append(stream)
                        self.logger.info(
                            f"Added '{stream.name}' as child stream to '{parent.name}'"
                        )

        streams = [stream for streams in streams_by_type.values() for stream in streams]
        return sorted(
            streams,
            key=lambda x: x.name,
            reverse=False,
        )

    @final
    def sync_all(self) -> None:
        """Sync all streams."""
        self._reset_state_progress_markers()
        self._set_compatible_replication_methods()
        stream: "Stream"
        
        # Use the streams from load_streams method
        ordered_streams = self.load_streams()
        
        for stream in ordered_streams:
            if not stream.selected and not stream.has_selected_descendents:
                self.logger.info(f"Skipping deselected stream '{stream.name}'.")
                continue

            if not stream.ignore_parent_stream and stream.parent_stream_type:
                self.logger.debug(
                    f"Child stream '{type(stream).__name__}' is expected to be called "
                    f"by parent stream '{stream.parent_stream_type.__name__}'. "
                    "Skipping direct invocation."
                )
                continue

            stream.sync()
            stream.finalize_state_progress_markers()


if __name__ == "__main__":
    TapVendit.cli()
