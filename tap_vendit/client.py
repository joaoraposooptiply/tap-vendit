"""REST client handling, including VenditStream base class."""

from __future__ import annotations

import decimal
import typing as t
from datetime import datetime, timedelta
from importlib import resources

import requests
from singer_sdk.authenticators import APIAuthenticatorBase
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.streams import RESTStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = resources.files(__package__) / "schemas"


class VenditAuthenticator(APIAuthenticatorBase):
    """Vendit authenticator."""

    def __init__(self, config: dict) -> None:
        """Initialize the authenticator."""
        self._api_key = config["api_key"]
        self._username = config["username"]
        self._password = config["password"]
        self._token_url = "https://oauth.staging.vendit.online/Api/GetToken"
        self._token = None
        self._token_expiry = None
        self.requests_session = requests.Session()
        self.requests_session.verify = False  # Disable SSL verification

    @property
    def auth_headers(self) -> dict:
        """Return the auth headers.

        Returns:
            A dictionary of auth headers.
        """
        if not self._token or not self._token_expiry or datetime.utcnow() >= self._token_expiry:
            self._get_token()
        
        return {
            "Token": self._token,
            "ApiKey": self._api_key,
        }

    def _get_token(self) -> None:
        """Get a new token."""
        params = {
            "apiKey": self._api_key,
            "username": self._username,
            "password": self._password,
        }
        
        response = self.requests_session.post(self._token_url, data=params)
        response.raise_for_status()
        
        token_data = response.json()
        print("Token response:", token_data)  # Log the response content
        self._token = token_data["token"]
        # Set token expiry to 1 hour from now (typical token lifetime)
        self._token_expiry = datetime.utcnow() + timedelta(hours=1)

    @property
    def auth_params(self):
        return {}


class VenditStream(RESTStream):
    """Vendit stream class."""

    records_jsonpath = "$.data[*]"
    next_page_token_jsonpath = "$.pagination.next_page"

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config.get("api_url", "https://api.vendit.com/v1")

    @property
    def authenticator(self) -> VenditAuthenticator:
        """Return a new authenticator object."""
        return VenditAuthenticator(self.config)

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
