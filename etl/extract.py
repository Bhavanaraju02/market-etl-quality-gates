from __future__ import annotations
import pandas as pd
import requests
from io import StringIO

def _stooq_symbol(symbol: str) -> str:
    """
    Stooq expects lowercase. For US tickers: 'aapl.us'
    """
    return symbol.lower()

def fetch_stooq_daily(symbol: str, start: str | None, end: str | None) -> pd.DataFrame:
    """
    Fetch daily OHLCV from Stooq as CSV.
    Returns DF with columns: symbol, date, open, high, low, close, volume
    """
    sym = _stooq_symbol(symbol)
    url = f"https://stooq.com/q/d/l/?s={sym}&i=d"

    r = requests.get(url, timeout=30)
    r.raise_for_status()

    df = pd.read_csv(StringIO(r.text))
    # Stooq CSV columns: Date, Open, High, Low, Close, Volume
    df = df.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
    })

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["symbol"] = symbol.upper()

    # Optional date filtering
    if start:
        start_d = pd.to_datetime(start).date()
        df = df[df["date"] >= start_d]
    if end:
        end_d = pd.to_datetime(end).date()
        df = df[df["date"] <= end_d]

    return df[["symbol", "date", "open", "high", "low", "close", "volume"]]
