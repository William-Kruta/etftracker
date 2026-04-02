# ETFTracker Scraper Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `get_holdings(symbol)` that scrapes all ETF holdings rows from Schwab's research site and returns them as a list of dicts.

**Architecture:** Playwright sync API drives a headless Chromium browser to the holdings page, clicks the "60" rows button (falling back to "40" then "20"), scrapes the table row-by-row, and paginates until all rows are collected.

**Tech Stack:** Python 3.12, Playwright (sync API), uv package manager

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `pyproject.toml` | Modify | Add `playwright` dependency |
| `etftracker/scraper.py` | Modify | `get_holdings` + private helpers |
| `main.py` | Modify | CLI entry point for manual integration testing |

---

### Task 1: Install Playwright

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add playwright to dependencies**

Edit `pyproject.toml` to match:

```toml
[project]
name = "etftracker"
version = "0.1.0"
description = "ETF holdings scraper"
readme = "README.md"
requires-python = ">=3.12"
dependencies = [
    "playwright>=1.40.0",
]
```

- [ ] **Step 2: Install the package**

```bash
source .venv/bin/activate && uv pip install playwright
```

Expected: resolves and installs playwright with no errors.

- [ ] **Step 3: Install the Chromium browser binary**

```bash
playwright install chromium
```

Expected: downloads Chromium binary, ends with something like `Chromium X.X.X downloaded`.

- [ ] **Step 4: Verify the install**

```bash
source .venv/bin/activate && python -c "from playwright.sync_api import sync_playwright; print('OK')"
```

Expected: prints `OK`.

---

### Task 2: Implement the scraper

**Files:**
- Modify: `etftracker/scraper.py`

- [ ] **Step 1: Write the full implementation**

Replace the contents of `etftracker/scraper.py` with:

```python
import warnings
from playwright.sync_api import sync_playwright, Page

# NOTE: The long token embedded in this URL may expire or rotate over time.
# If the page stops loading the holdings table, update this base URL.
_BASE_URL = (
    "https://www.schwab.wallst.com/schwab/Prospect/research/etfs/schwabETF/"
    "index.asp?YYY101_z5K6INmijHnvYhO4I8RduTIXt6f1MqU35TkNiQeohmHVWPKKtRCNK4kwkd6ii"
    "TworpIn1cCysHYjKPXGCBZNRMGjL9bK1MM/2kMhavL5Jzw=&type=holdings&symbol={symbol}"
)


def get_holdings(symbol: str) -> list[dict]:
    """Scrape all holdings rows for the given ETF symbol from Schwab's research site.

    Returns a list of dicts, one per holding row, with keys matching the table
    column headers. Paginates automatically until all rows are collected.
    """
    url = _BASE_URL.format(symbol=symbol.upper())
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="networkidle")

        try:
            page.wait_for_selector("table", timeout=15000)
        except Exception:
            raise RuntimeError(
                f"Holdings table not found for symbol {symbol!r}. "
                "The URL token may have expired or the symbol is invalid."
            )

        _set_row_count(page)

        while True:
            results.extend(_scrape_page(page))
            if not _go_to_next_page(page):
                break

        browser.close()

    return results


def _set_row_count(page: Page) -> None:
    """Click the highest available row-count button (60 → 40 → 20)."""
    for count in ("60", "40", "20"):
        btn = page.query_selector(f"button:has-text('{count}')")
        if btn and btn.is_visible():
            btn.click()
            page.wait_for_load_state("networkidle")
            return
    warnings.warn(
        "Could not find row-count buttons (60/40/20). "
        "Scraping with the default row count shown on the page."
    )


def _scrape_page(page: Page) -> list[dict]:
    """Extract all rows from the current page of the holdings table."""
    headers = [
        th.inner_text().strip()
        for th in page.query_selector_all("thead th")
    ]
    result = []
    for row in page.query_selector_all("tbody tr"):
        cells = [td.inner_text().strip() for td in row.query_selector_all("td")]
        if cells:
            result.append(dict(zip(headers, cells)))
    return result


def _go_to_next_page(page: Page) -> bool:
    """Click the next-page control if one exists and is enabled. Returns True if clicked."""
    # Try common patterns for next-page buttons on financial sites
    for selector in (
        "button[aria-label='Next page']",
        "a[aria-label='Next page']",
        "button[aria-label='Next']",
        "a[aria-label='Next']",
    ):
        btn = page.query_selector(selector)
        if btn and btn.is_visible() and btn.is_enabled():
            btn.click()
            page.wait_for_load_state("networkidle")
            return True
    return False
```

- [ ] **Step 2: Verify syntax**

```bash
source .venv/bin/activate && python -c "from etftracker.scraper import get_holdings; print('OK')"
```

Expected: prints `OK`.

---

### Task 3: Update main.py CLI entry point

**Files:**
- Modify: `main.py`

- [ ] **Step 1: Write the CLI**

Replace the contents of `main.py` with:

```python
import sys
from etftracker.scraper import get_holdings


def main():
    symbol = sys.argv[1] if len(sys.argv) > 1 else "VOO"
    print(f"Fetching holdings for {symbol}...")
    holdings = get_holdings(symbol)
    print(f"Total holdings scraped: {len(holdings)}")
    for row in holdings[:5]:
        print(row)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the integration test**

```bash
source .venv/bin/activate && python main.py VOO
```

Expected output (values will differ, but shape should match):
```
Fetching holdings for VOO...
Total holdings scraped: 503
{'Name': 'Apple Inc', 'Symbol': 'AAPL', 'Weight': '7.01%', ...}
{'Name': 'Microsoft Corp', 'Symbol': 'MSFT', 'Weight': '6.33%', ...}
...
```

If `Total holdings scraped: 0` or a `RuntimeError` is raised, proceed to Task 4.

---

### Task 4: Adjust selectors if needed

The exact selectors for Schwab's table, row-count buttons, and next-page control can only be confirmed against the live page. If Task 3 returns zero rows or errors, use this task to inspect and fix them.

**Files:**
- Modify: `etftracker/scraper.py`

- [ ] **Step 1: Open the page in a headed browser to inspect it**

```bash
source .venv/bin/activate && python - <<'EOF'
from playwright.sync_api import sync_playwright

url = (
    "https://www.schwab.wallst.com/schwab/Prospect/research/etfs/schwabETF/"
    "index.asp?YYY101_z5K6INmijHnvYhO4I8RduTIXt6f1MqU35TkNiQeohmHVWPKKtRCNK4kwkd6ii"
    "TworpIn1cCysHYjKPXGCBZNRMGjL9bK1MM/2kMhavL5Jzw=&type=holdings&symbol=VOO"
)

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()
    page.goto(url, wait_until="networkidle")
    input("Inspect the page, then press Enter to close...")
    browser.close()
EOF
```

Use browser DevTools (right-click → Inspect) to find the correct selectors for:
1. The holdings table (used in `wait_for_selector` and `query_selector_all`)
2. The row-count buttons (20 / 40 / 60)
3. The next-page button

- [ ] **Step 2: Update selectors in `etftracker/scraper.py`**

Based on what DevTools shows, update any of these lines as needed:

| Location | Current selector | Replace if wrong |
|---|---|---|
| `get_holdings` | `"table"` | More specific selector if multiple tables exist |
| `_set_row_count` | `f"button:has-text('{count}')"` | Correct element type / selector for the row buttons |
| `_scrape_page` | `"thead th"` | Correct selector for column headers |
| `_scrape_page` | `"tbody tr"` | Correct selector for data rows |
| `_scrape_page` | `"td"` | Correct selector for cells within a row |
| `_go_to_next_page` | `"button[aria-label='Next page']"` etc. | Correct selector for the next-page control |

- [ ] **Step 3: Re-run the integration test**

```bash
source .venv/bin/activate && python main.py VOO
```

Expected: `Total holdings scraped` is > 0 and first 5 rows print as dicts with correct column names.
