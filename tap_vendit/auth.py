"""Authentication handling for the Vendit API."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional, TypeVar, Generic
import os

import backoff
import requests
from requests.exceptions import RequestException
from singer_sdk.authenticators import APIAuthenticatorBase
from singer_sdk.streams import Stream as RESTStreamBase

logger = logging.getLogger(__name__)

T = TypeVar('T')

class EmptyResponseError(Exception):
    """Raised when the response is empty"""
    pass

class TokenRefreshError(Exception):
    """Raised when token refresh fails"""
    pass

class FileHandler(Generic[T]):
    """Generic file handling operations."""
    
    def __init__(self, file_path: Optional[str] = None) -> None:
        """Initialize file handler.
        
        Args:
            file_path: Optional path to file
        """
        self._file_path = file_path
        
    def _read_file(self) -> Dict[str, Any]:
        """Read data from file.
        
        Returns:
            Dictionary containing file data
        """
        if not self._file_path:
            return {}
            
        try:
            with open(self._file_path) as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.warning(f"Failed to read file {self._file_path}: {str(e)}")
            return {}
            
    def _write_file(self, data: Dict[str, Any]) -> None:
        """Write data to file.
        
        Args:
            data: Dictionary containing data to write
        """
        if not self._file_path:
            return
            
        try:
            # Read existing data
            existing_data = self._read_file()
            
            # Update with new data
            existing_data.update(data)
            
            # Write back to file
            with open(self._file_path, "w") as f:
                json.dump(existing_data, f, indent=4)
                
            logger.info(f"Successfully wrote data to {self._file_path}")
        except Exception as e:
            logger.error(f"Failed to write to file {self._file_path}: {str(e)}")

class TokenStorage(FileHandler):
    """Handles token storage and persistence in the config file."""
    
    def __init__(self, file_path: Optional[str] = None) -> None:
        # Use the provided file path or default to secrets.json
        super().__init__(file_path or os.path.abspath("secrets.json"))
    
    def read_token(self) -> Dict[str, Any]:
        config = self._read_file()
        return {
            "token": config.get("token"),
            "token_expire": config.get("token_expire")
        }
    
    def write_token(self, token_data: Dict[str, Any]) -> None:
        self._write_file(token_data)

class RequestHandler:
    """Handles HTTP requests with authentication."""
    
    def __init__(self, verify_ssl: bool = False) -> None:
        """Initialize request handler.
        
        Args:
            verify_ssl: Whether to verify SSL certificates
        """
        self.session = requests.Session()
        self.session.verify = verify_ssl
        
    def make_request(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        data: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> requests.Response:
        """Make an HTTP request.
        
        Args:
            method: HTTP method
            url: Request URL
            headers: Request headers
            data: Optional request data
            **kwargs: Additional request arguments
            
        Returns:
            Response from the request
        """
        try:
            response = self.session.request(
                method,
                url,
                headers=headers,
                json=data,
                **kwargs
            )
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            raise

class VenditAuthenticator(APIAuthenticatorBase):
    """Authenticator class that handles Vendit API authentication.
    
    This class manages token-based authentication including:
    - Token acquisition
    - Token refresh
    - Token persistence
    - Error handling with retries
    """

    def __init__(
        self,
        stream: RESTStreamBase,
        config_file: Optional[str] = None,
        auth_endpoint: Optional[str] = None,
    ) -> None:
        """Initialize the authenticator.
        
        Args:
            stream: The stream instance this authenticator is attached to
            config_file: Optional path to config file for token persistence
            auth_endpoint: Optional override for the auth endpoint URL
        """
        super().__init__(stream=stream)
        self._auth_endpoint = auth_endpoint or "https://oauth.staging.vendit.online/Api/GetToken"
        self._tap = stream._tap
        # Use the config file path from the tap, or fall back to secrets.json
        config_file_path = config_file or self._tap._config.get("config_file") or os.path.abspath("secrets.json")
        self._token_storage = TokenStorage(config_file_path)
        self._request_handler = RequestHandler(verify_ssl=False)
        # Load any existing token data
        token_data = self._token_storage.read_token()
        if token_data:
            self._tap._config.update(token_data)

    @property
    def auth_headers(self) -> dict:
        """Get the authentication headers.
        
        Returns:
            A dictionary of headers needed for authentication.
        """
        if not self.is_token_valid():
            self.update_access_token()
        
        return {
            "Token": self._tap._config.get("token"),
            "ApiKey": self._tap._config.get("vendit_api_key"),
            "Content-Type": "application/json"
        }

    @property
    def auth_params(self) -> dict:
        """Get the authentication URL parameters.
        
        Returns:
            An empty dict as auth is handled via headers.
        """
        return {}

    def is_token_valid(self) -> bool:
        """Check if the current token is valid.
        
        Returns:
            True if token exists and is not near expiration, False otherwise.
        """
        token = self._tap._config.get("token")
        now = round(datetime.utcnow().timestamp())
        expires_in = self._tap._config.get("token_expire")
        
        if not token or not expires_in:
            return False

        # Consider token invalid if within 2 minutes of expiration
        return not ((expires_in - now) < 120)

    @property
    def oauth_request_body(self) -> dict:
        """Get the OAuth request body for token acquisition/refresh.
        
        Returns:
            Dictionary containing the required OAuth parameters.
        """
        return {
            "apiKey": self._tap._config.get("vendit_api_key"),
            "username": self._tap._config.get("username"),
            "password": self._tap._config.get("password"),
        }

    @backoff.on_exception(
        backoff.expo,
        (EmptyResponseError, RequestException, TokenRefreshError),
        max_tries=5,
        factor=2
    )
    def update_access_token(self) -> None:
        """Update the access token.
        
        This method handles token refresh with retry logic and error handling.
        The new token is stored in the config and optionally persisted to file.
        
        Raises:
            TokenRefreshError: If token refresh fails after retries
            EmptyResponseError: If the response is empty or invalid
        """
        # Build the URL with credentials as query parameters
        url = (
            f"{self._auth_endpoint}?"
            f"apiKey={self._tap._config.get('vendit_api_key')}"
            f"&username={self._tap._config.get('username')}"
            f"&password={self._tap._config.get('password')}"
        )
        headers = {
            "Accept": "application/json",
            # Content-Type can be omitted or set to application/json
        }
        try:
            response = self._request_handler.session.post(url, headers=headers)
            response.raise_for_status()
            try:
                token_data = response.json()
            except json.JSONDecodeError:
                raise EmptyResponseError("Failed converting response to json, because response is empty")
            if not isinstance(token_data, dict) or "token" not in token_data:
                raise TokenRefreshError("Invalid token response format")
            # Update config with new token info
            token_info = {
                "token": token_data["token"],
                "token_expire": token_data.get("expire", 0)
            }
            self._tap._config.update(token_info)
            # Persist token data
            self._token_storage.write_token(token_info)
            logger.info("Successfully obtained new token")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to refresh token: {str(e)}")
            raise TokenRefreshError(f"Token refresh failed: {str(e)}") 