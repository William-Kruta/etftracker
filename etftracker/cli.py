from __future__ import annotations

import argparse
from pathlib import Path

from .etftracker import get_etf_holdings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch ETF holdings")
    parser.add_argument(
        "symbols",
        nargs="+",
        help="One or more ETF ticker symbols to scrape",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run Firefox in headless mode",
    )
    parser.add_argument(
        "--force-update",
        action="store_true",
        help="Ignore cached holdings and scrape fresh data",
    )
    parser.add_argument(
        "--stale-days",
        type=int,
        default=120,
        help="Refresh holdings if cached data is older than this many days",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        help="Optional path to write the result as CSV",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    import datetime as dt

    df = get_etf_holdings(
        args.symbols if len(args.symbols) > 1 else args.symbols[0],
        stale_threshold=dt.timedelta(days=args.stale_days),
        force_update=args.force_update,
        headless=args.headless,
    )

    if args.csv is not None:
        df.to_csv(args.csv, index=False)
    else:
        print(df.to_string(index=False))

    return 0
