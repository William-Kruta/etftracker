# ETFTracker

ETFTracker is a small Python package for collecting ETF holdings data, normalizing
the results, and storing the holdings in a local DuckDB database for reuse.

## Features

- Scrapes ETF holdings tables into a dataframe with `symbol`, `name`, `weight`,
  `shares_owned`, and `shares_value`.
- Normalizes holdings data into a consistent schema with `etf_ticker` and
  `collected_at`.
- Stores holdings in DuckDB with parsed numeric fields for percentages, share
  counts, and dollar values.
- Uses `PRIMARY KEY (etf_ticker, symbol)` so each ETF/holding pair is stored
  once and can be refreshed in place.
- Reads holdings back from DuckDB with a simple ETF ticker filter.
- Deletes individual holdings rows by `etf_ticker` and `symbol`.
- Refreshes stale data automatically based on a configurable `stale_threshold`.
- Supports fetching one ETF or multiple ETFs in a single call.

## Installation

```
pip install etftracker
```

## Requirements

- Python 3.12+
- Firefox installed for Selenium WebDriver usage
- A working geckodriver / Selenium Firefox setup on the machine

## Quick Start

```python
import datetime as dt
from etftracker.etftracker import get_etf_holdings

df = get_etf_holdings("SPY", stale_threshold=dt.timedelta(days=7))
print(df.head())
```

Fetch multiple ETFs:

```python
from etftracker.etftracker import get_etf_holdings

df = get_etf_holdings(["SPY", "VTI", "VOO"])
print(df[["etf_ticker", "symbol", "name"]].head())
```

## Database Helpers

Read holdings for a single ETF:

```python
from etftracker.db import read_holdings

df = read_holdings("SPY")
```

Delete a single holding:

```python
from etftracker.db import delete_holding

deleted = delete_holding("SPY", "AAPL")
print(deleted)
```

Save a freshly scraped dataframe manually:

```python
from etftracker.scraper import pipeline
from etftracker.db import save_holdings

df = pipeline("SPY")
save_holdings(df, "SPY")
```

## Data Model

The normalized holdings table includes:

- `etf_ticker`
- `collected_at`
- `symbol`
- `name`
- `weight`
- `weight_pct`
- `shares_owned`
- `shares_owned_num`
- `shares_value`
- `shares_value_num`

## Database Location

By default the DuckDB database is stored in a user config directory:

- Linux/BSD: `~/.config/etftracker/etftracker.duckdb`
- macOS: `~/Library/Application Support/etftracker/etftracker.duckdb`
- Windows: `%APPDATA%/etftracker/etftracker.duckdb`

You can override that path with the `ETFTRACKER_DB` environment variable.

## License

This repository's source code is licensed under the MIT License. See
[LICENSE](LICENSE).

## Third-Party Data Notice

This package license applies only to the code in this repository. It does not
grant any rights to data obtained from third-party websites or services.

Users are responsible for ensuring their use of this package complies with the
terms of service, contracts, licenses, and other restrictions that apply to
any data source they access.

This repository does not ship third-party holdings datasets, cached database
files, or redistributed source data as part of the package itself.
