# tap-vendit

`tap-vendit` is a Singer tap for Vendit, a comprehensive e-commerce and inventory management platform.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Features

- **Multiple Data Streams**: Products, suppliers, orders, purchase orders, supplier products, and Optiply-specific streams
- **Robust Authentication**: Token-based authentication with automatic refresh and persistence
- **Incremental Sync**: Support for both timestamp-based and ID-based incremental synchronization
- **Comprehensive Testing**: Built-in testing utilities for all streams with CSV and JSON output
- **Error Handling**: Graceful handling of API errors with retry logic and exponential backoff

## Available Streams

### Core Streams
- **Products**: Product catalog and inventory information
- **Suppliers**: Supplier details and contact information
- **Orders**: Customer order data with line items
- **Purchase Orders**: Purchase order management data
- **Supplier Products**: Product-supplier relationships and pricing

### Optiply Streams (Advanced Analytics)
- **OrdersOptiply**: Enhanced order data with nested details using unix timestamp incremental sync
- **PurchaseOrdersOptiply**: Enhanced purchase order data with nested details using unix timestamp incremental sync

## Installation

```bash
pipx install tap-vendit
```

## Credentials

### Create a Config file

```json
{
  "api_key": "your_api_key",
  "username": "your_username",
  "password": "your_password",
  "api_url": "https://api.staging.vendit.online",
  "start_date": "2024-01-01T00:00:00Z",
  "config_file": "/path/to/config.json"  // Optional: for token persistence
}
```

The `api_key`, `username`, and `password` are your Vendit API credentials. These are used to authenticate with the Vendit API and obtain an access token.

The `api_url` is the base URL for the Vendit API. The default is set to the staging environment.

The `start_date` is used by the tap to fetch records from that date on. This should be an [RFC3339](https://www.ietf.org/rfc/rfc3339.txt) formatted date-time, like "2024-01-01T00:00:00Z". For more details, see the [Singer best practices for dates](https://github.com/singer-io/getting-started/blob/master/BEST_PRACTICES.md#dates).

The optional `config_file` parameter specifies where to store and load the authentication token. If not provided, it defaults to the `config.json` file in the tap's root directory.

## Authentication

The tap uses token-based authentication with the Vendit API. The authentication process:

1. Uses the provided API key, username, and password to obtain an access token
2. Caches the token and its expiration time in the config file (if specified)
3. Automatically refreshes the token before it expires
4. Implements retry logic with exponential backoff for token requests
5. Handles various error conditions gracefully

The token management is fully automated - you don't need to manually handle token refresh or expiration. The tap will:

- Check token validity before each request
- Refresh expired tokens automatically
- Persist tokens to the config file (if specified) for reuse across runs
- Handle token-related errors with retries and proper error reporting

## Configuration

### Accepted Config Options

A full list of supported settings and capabilities for this tap is available by running:

```bash
tap-vendit --about
```

## Usage

You can easily run `tap-vendit` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-vendit --version
tap-vendit --help
tap-vendit --config CONFIG --discover > ./catalog.json
```
## Developer Resources

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_vendit/tests` subfolder and then run:

```bash
poetry run pytest
```

You can also test the `tap-vendit` CLI interface directly using `poetry run`:

```bash
poetry run tap-vendit --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-vendit
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-vendit --version
# OR run a test `elt` pipeline:
meltano elt tap-vendit target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to 
develop your own taps and targets.

## Stream Details

### Core Streams

#### ProductsStream
- **Endpoint**: `/Products/GetAll`
- **Sync Type**: Full table
- **Description**: Retrieves all products with their basic information

#### SuppliersStream
- **Endpoint**: `/Suppliers/GetAll`
- **Sync Type**: Full table
- **Description**: Retrieves all suppliers with their contact information

#### OrdersStream
- **Endpoint**: `/Orders/GetAll`
- **Sync Type**: Incremental (ID-based)
- **Description**: Retrieves orders with incremental sync using order IDs

#### PurchaseOrdersStream
- **Endpoint**: `/PurchaseOrders/GetAll`
- **Sync Type**: Incremental (ID-based)
- **Description**: Retrieves purchase orders with incremental sync using purchase order IDs

#### SupplierProductsStream
- **Endpoint**: `/SupplierProducts/GetAll`
- **Sync Type**: Full table
- **Description**: Retrieves supplier-product relationships and pricing information

### Optiply Streams

#### OrdersOptiplyStream
- **Endpoint**: `/Optiply/GetOrdersFromDate/{unix}/true`
- **Sync Type**: Incremental (Unix timestamp-based)
- **Description**: Retrieves enhanced order data with nested details using unix timestamp incremental sync
- **Schema**: `order_optiply.json` - Includes nested order details and line items

#### PurchaseOrdersOptiplyStream
- **Endpoint**: `/Optiply/GetPurchaseOrdersFromDate/{unix}/true`
- **Sync Type**: Incremental (Unix timestamp-based)
- **Description**: Retrieves enhanced purchase order data with nested details using unix timestamp incremental sync
- **Schema**: `purchase_order_optiply.json` - Includes nested purchase order details and line items