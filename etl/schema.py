from etl.db import exec_sql

DDL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
  run_id TEXT PRIMARY KEY,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  status TEXT NOT NULL,
  symbols TEXT NOT NULL,
  start_date DATE,
  end_date DATE,
  note TEXT
);

CREATE TABLE IF NOT EXISTS raw_prices (
  run_id TEXT NOT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  symbol TEXT NOT NULL,
  date DATE NOT NULL,
  open DOUBLE PRECISION,
  high DOUBLE PRECISION,
  low DOUBLE PRECISION,
  close DOUBLE PRECISION,
  volume BIGINT,
  PRIMARY KEY (symbol, date, run_id)
);

CREATE TABLE IF NOT EXISTS stg_prices (
  symbol TEXT NOT NULL,
  date DATE NOT NULL,
  open DOUBLE PRECISION NOT NULL,
  high DOUBLE PRECISION NOT NULL,
  low DOUBLE PRECISION NOT NULL,
  close DOUBLE PRECISION NOT NULL,
  volume BIGINT NOT NULL,
  last_run_id TEXT NOT NULL,
  PRIMARY KEY (symbol, date)
);

CREATE TABLE IF NOT EXISTS fct_prices_daily (
  symbol TEXT NOT NULL,
  date DATE NOT NULL,
  open DOUBLE PRECISION NOT NULL,
  high DOUBLE PRECISION NOT NULL,
  low DOUBLE PRECISION NOT NULL,
  close DOUBLE PRECISION NOT NULL,
  volume BIGINT NOT NULL,
  daily_return DOUBLE PRECISION,
  typical_price DOUBLE PRECISION,
  last_run_id TEXT NOT NULL,
  PRIMARY KEY (symbol, date)
);
"""

def ensure_schema():
    exec_sql(DDL)
