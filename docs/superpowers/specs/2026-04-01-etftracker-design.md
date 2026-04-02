# ETFTracker Scraper Design

**Date:** 2026-04-01

## Overview

A Python package that scrapes ETF holdings data from Schwab's research site and returns it as a list of dicts, one per holding row.

## Public API

```python
from etftracker.scraper import get_holdings

holdings = get_holdings("VOO")
# returns [{"Name": "Apple Inc", "Weight": "7.01%", ...}, ...]
```

Single ticker per call. Returns a list of dicts where keys are the column headers from the holdings table.

## Architecture

Two files of substance:

- `etftracker/scraper.py` — contains `get_holdings(symbol: str) -> list[dict]`
- `main.py` — CLI entry point for ad-hoc use, calls `get_holdings` and prints results

Dependency added to `pyproject.toml`: `playwright`

## URL Construction

Base URL template with `symbol=` substituted at the end:

```
https://www.schwab.wallst.com/schwab/Prospect/research/etfs/schwabETF/index.asp?YYY101_z5K6INmijHnvYhO4I8RduTIXt6f1MqU35TkNiQeohmHVWPKKtRCNK4kwkd6iiTworpIn1cCysHYjKPXGCBZNRMGjL9bK1MM/2kMhavL5Jzw=&type=holdings&symbol={symbol}
```

> Note: The long token in the URL may be session-specific or expire over time. If the page stops loading correctly, this token will need to be updated.

## Data Flow

1. Build URL from base template + symbol
2. Launch headless Chromium via Playwright (sync API)
3. Navigate to URL, wait for the holdings table to appear
4. Click the row-count button: try "60" first, then "40", then "20"; if none found, log a warning and proceed with whatever is shown
5. Wait for the table to re-render after clicking the button
6. Extract column headers from `<thead>`
7. Extract all rows from `<tbody>`, zip each row's cells with headers → dict
8. Append dicts to the running results list
9. Check for an enabled "next page" control — if present, click it, wait for re-render, repeat from step 6
10. Close browser, return combined results list

## Error Handling

| Condition | Behavior |
|---|---|
| Table not found after navigation | Raise `RuntimeError` with descriptive message |
| "60" button not found | Fall back to "40" |
| "40" button not found | Fall back to "20" |
| "20" button not found | Log warning, scrape whatever rows are visible |
| No next-page button found | End pagination, return what was collected |

## Testing

No automated unit tests for now — the live site dependency makes mocking impractical without significant setup. `main.py` serves as the manual integration test: run with a known ticker (e.g. `VOO`) and verify the output shape and content look correct.
