from .etftracker import get_etf_holdings


def export_to_csv(etf_ticker: str, csv_path: str):
    if ".csv" not in csv_path:
        csv_path += ".csv"
    holdings = get_etf_holdings(etf_symbol=etf_ticker)
    holdings.to_csv(csv_path)
