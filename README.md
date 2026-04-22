# ETFTracker

ETFTracker is a small Python package for collecting ETF holdings data, normalizing
the results, and storing the holdings in a local DuckDB database for reuse.

## Features

- Scrapes ETF holdings tables into a dataframe with `symbol`, `name`, `weight`,
  `shares_owned`, and `shares_value`.
- Normalizes holdings data into a consistent schema with `etf_ticker` and
  `collected_at`.
- Stores companies separately from ETF holdings so repeated company metadata is
  not copied for every ETF.
- Stores holdings in DuckDB with parsed numeric fields for percentages, share
  counts, and dollar values.
- Uses `PRIMARY KEY (etf_ticker, collected_at, symbol)` so ETF holdings can be
  tracked historically across scrape runs.
- Reads holdings back from DuckDB with a simple ETF ticker filter.
- Deletes individual holdings rows by `etf_ticker` and `symbol`.
- Refreshes stale data automatically based on a configurable `stale_threshold`.
- Supports fetching one ETF or multiple ETFs in a single call.

## Installation

```
pip install etftracker
```

## Command Line

After installation, the package exposes an `etftracker` command.

Fetch one ticker and print the normalized holdings to stdout:

```bash
etftracker SPY --headless
```

Fetch multiple tickers and write the result to CSV:

```bash
etftracker SPY VTI VOO --headless --csv holdings.csv
```

Force a fresh scrape instead of using cached database rows:

```bash
etftracker SPY --headless --force-update
```

## Requirements

- Python 3.12+
- Firefox installed for Selenium WebDriver usage
- A working geckodriver / Selenium Firefox setup on the machine

## Quick Start

```python
import datetime as dt
from etftracker import get_etf_holdings

df = get_etf_holdings("SPY", stale_threshold=dt.timedelta(days=7))
print(df.head())
```

Fetch multiple ETFs:

```python
from etftracker import get_etf_holdings

df = get_etf_holdings(["SPY", "VTI", "VOO"])
print(df[["etf_ticker", "symbol", "name"]].head())
```

## Database Helpers

Read holdings for a single ETF:

```python
from etftracker import read_holdings

df = read_holdings("SPY")
```

Read historical holdings for a single ETF:

```python
from etftracker import read_holdings_history

df = read_holdings_history("SPY")
```

Delete a single holding:

```python
from etftracker import delete_holding

deleted = delete_holding("SPY", "AAPL")
print(deleted)
```

Delete all holdings for one or more ETFs:

```python
from etftracker import delete_etf_holdings

deleted = delete_etf_holdings(["SPY", "VTI"])
print(deleted)
```

Delete every holding row in the database:

```python
from etftracker import delete_all_holdings

deleted = delete_all_holdings()
print(deleted)
```

Save a freshly scraped dataframe manually:

```python
from etftracker import pipeline, save_holdings

df = pipeline("SPY")
save_holdings(df, "SPY")
```

For a quick script entry point, the repository also includes `main.py`, but the
packaged interface is the `etftracker` command above.

## Data Model

The DuckDB schema stores company metadata once:

```sql
CREATE TABLE companies (
    symbol TEXT PRIMARY KEY,
    name TEXT NOT NULL
);
```

ETF holdings reference those company symbols and keep one row per ETF, scrape
timestamp, and holding symbol:

```sql
CREATE TABLE etf_holdings (
    etf_ticker TEXT NOT NULL,
    collected_at TIMESTAMP NOT NULL,
    symbol TEXT NOT NULL,
    weight TEXT,
    weight_pct DOUBLE,
    shares_owned TEXT,
    shares_owned_num DOUBLE,
    shares_value TEXT,
    shares_value_num DOUBLE,
    PRIMARY KEY (etf_ticker, collected_at, symbol),
    FOREIGN KEY (symbol) REFERENCES companies(symbol)
);
```

`read_holdings()` returns the latest snapshot joined with company names. The
returned dataframe includes:

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
