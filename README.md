# tap-vendit

`tap-vendit` is a Singer tap for Vendit.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

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
  "start_date": "2024-01-01T00:00:00Z"
}
```

The `api_key`, `username`, and `password` are your Vendit API credentials. These are used to authenticate with the Vendit API and obtain an access token.

The `api_url` is the base URL for the Vendit API. The default is set to the staging environment.

The `start_date` is used by the tap to fetch records from that date on. This should be an [RFC3339](https://www.ietf.org/rfc/rfc3339.txt) formatted date-time, like "2024-01-01T00:00:00Z". For more details, see the [Singer best practices for dates](https://github.com/singer-io/getting-started/blob/master/BEST_PRACTICES.md#dates).

## Configuration

### Accepted Config Options

A full list of supported settings and capabilities for this tap is available by running:

```bash
tap-vendit --about
```

### Source Authentication and Authorization

The tap uses token-based authentication with the Vendit API. The authentication process:
1. Uses the provided API key, username, and password to obtain an access token
2. Uses this token for subsequent API requests
3. Automatically refreshes the token when it expires

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
