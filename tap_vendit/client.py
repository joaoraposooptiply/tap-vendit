"""REST client handling, including VenditStream base class."""

from __future__ import annotations

import decimal
import logging
import typing as t
from datetime import datetime, timedelta
from importlib import resources
import time

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.streams import RESTStream

from tap_vendit.auth import VenditAuthenticator

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

# Set up logging
logger = logging.getLogger(__name__)

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = resources.files(__package__) / "schemas"


class VenditStream(RESTStream):
    """Vendit stream class."""

    records_jsonpath = "$.data[*]"
    next_page_token_jsonpath = "$.pagination.next_page"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = requests.Session()
        self.session.verify = False  # Disable SSL verification for all requests
        self._authenticator = VenditAuthenticator(
            stream=self,
            config_file=self.config.get("config_file")
        )

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config.get("api_url", "https://api.vendit.com/v1")

    @property
    def authenticator(self) -> VenditAuthenticator:
        """Return the authenticator."""
        return self._authenticator

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        return headers

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        yield from extract_jsonpath(
            self.records_jsonpath,
            input=response.json(parse_float=decimal.Decimal),
        )

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure."""
        return row
