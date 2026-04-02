from etftracker.etftracker import get_etf_holdings


# Run the script
if __name__ == "__main__":
    # Replace with your actual ETF symbol
    etf_symbol = ["VTI"]
    df = get_etf_holdings(etf_symbol=etf_symbol)
    print(df)
