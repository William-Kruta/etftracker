import pandas as pd
import datetime as dt
from .scraper import pipeline
from .db import (
    create_holdings_table,
    save_holdings,
    read_holdings,
    get_connection,
    normalize_holdings_frame,
    TABLE_NAME,
)


def get_etf_holdings(
    etf_symbol: list | str,
    stale_threshold: dt.timedelta = dt.timedelta(days=120),
    force_update: bool = False,
    headless: bool = False,
):
    """
    etf_symbol: str, Ticker symbol of the ETF
    stale_threshold: dt.timedelta, determines if new data is collected.
    force_update: bool, override threshold logic and force an update to the database.
    """
    if isinstance(etf_symbol, str):
        etf_symbol = [etf_symbol]
    conn = get_connection()
    try:
        create_holdings_table(conn)
    finally:
        conn.close()

    data = []
    for t in etf_symbol:
        df = _get(
            t,
            stale_threshold=stale_threshold,
            force_update=force_update,
            headless=headless,
        )
        data.append(df)
    df = pd.concat(data)
    bad_symbols = {"--", ""}
    df = df[df["symbol"].notna() & ~df["symbol"].str.strip().isin(bad_symbols)]
    return df


def _get(
    etf_symbol: str,
    stale_threshold: dt.timedelta,
    force_update: bool,
    headless: bool = False,
):
    df = read_holdings(etf_symbol)
    if df.empty or force_update:
        fresh_df = pipeline(etf_symbol, headless=headless)
        save_holdings(fresh_df, etf_symbol)
        return normalize_holdings_frame(fresh_df, etf_symbol)

    latest_collected_at = pd.to_datetime(df["collected_at"], utc=True).max()
    current_time = pd.Timestamp.now(tz="UTC")
    data_age = current_time - latest_collected_at

    if data_age > stale_threshold:
        fresh_df = pipeline(etf_symbol, headless=headless)
        save_holdings(fresh_df, etf_symbol)
        return normalize_holdings_frame(fresh_df, etf_symbol)

    return df


def get_currently_tracked_etfs(conn=None):
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    try:
        create_holdings_table(conn)
        query = f"SELECT DISTINCT etf_ticker FROM {TABLE_NAME} ORDER BY etf_ticker"
        result = conn.execute(query)
        return [r[0] for r in result.fetchall()]
    finally:
        if should_close:
            conn.close()
