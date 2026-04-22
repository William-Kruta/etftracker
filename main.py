from etftracker import (
    get_etf_holdings,
    get_currently_tracked_etfs,
    export_to_csv,
    delete_etf_holdings,
)


if __name__ == "__main__":
    symbol = "SCHG"
    hodlings = get_etf_holdings(symbol, headless=True)
