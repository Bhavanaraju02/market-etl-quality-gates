from __future__ import annotations
import pandas as pd
from sqlalchemy import text
from etl.db import get_engine

def load_raw(df: pd.DataFrame, run_id: str):
    df2 = df.copy()
    df2["run_id"] = run_id
    eng = get_engine()
    df2.to_sql("raw_prices", eng, if_exists="append", index=False, method="multi")

def upsert_staging(df: pd.DataFrame, run_id: str):
    eng = get_engine()
    rows = df.copy()
    rows["last_run_id"] = run_id

    sql = """
    INSERT INTO stg_prices(symbol,date,open,high,low,close,volume,last_run_id)
    VALUES (:symbol,:date,:open,:high,:low,:close,:volume,:last_run_id)
    ON CONFLICT (symbol,date) DO UPDATE SET
      open=EXCLUDED.open,
      high=EXCLUDED.high,
      low=EXCLUDED.low,
      close=EXCLUDED.close,
      volume=EXCLUDED.volume,
      last_run_id=EXCLUDED.last_run_id;
    """
    with eng.begin() as conn:
        conn.execute(text(sql), rows.to_dict(orient="records"))

def build_curated_from_staging(run_id: str):
    eng = get_engine()
    sql = """
    INSERT INTO fct_prices_daily(symbol,date,open,high,low,close,volume,daily_return,typical_price,last_run_id)
    SELECT
      s.symbol,
      s.date,
      s.open, s.high, s.low, s.close, s.volume,
      CASE
        WHEN LAG(s.close) OVER (PARTITION BY s.symbol ORDER BY s.date) IS NULL THEN NULL
        ELSE (s.close / LAG(s.close) OVER (PARTITION BY s.symbol ORDER BY s.date)) - 1.0
      END AS daily_return,
      (s.high + s.low + s.close)/3.0 AS typical_price,
      :run_id AS last_run_id
    FROM stg_prices s
    ON CONFLICT (symbol,date) DO UPDATE SET
      open=EXCLUDED.open,
      high=EXCLUDED.high,
      low=EXCLUDED.low,
      close=EXCLUDED.close,
      volume=EXCLUDED.volume,
      daily_return=EXCLUDED.daily_return,
      typical_price=EXCLUDED.typical_price,
      last_run_id=EXCLUDED.last_run_id;
    """
    with eng.begin() as conn:
        conn.execute(text(sql), {"run_id": run_id})
