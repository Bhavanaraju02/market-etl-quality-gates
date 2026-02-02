from __future__ import annotations
import pandas as pd

def clean_prices(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for c in ["open","high","low","close","volume"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.dropna(subset=["symbol","date"])
    out = out.dropna(subset=["open","high","low","close","volume"])

    out = out[(out["close"] > 0) & (out["open"] > 0) & (out["high"] > 0) & (out["low"] > 0)]
    out["volume"] = out["volume"].astype("int64")
    return out
