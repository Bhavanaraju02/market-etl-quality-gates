from __future__ import annotations
from sqlalchemy import text
from etl.db import get_engine

EXPECTED_FCT_COLS = {
    "symbol","date","open","high","low","close","volume","daily_return","typical_price","last_run_id"
}

def _fetch_one(sql: str, params=None):
    eng = get_engine()
    with eng.begin() as conn:
        return conn.execute(text(sql), params or {}).fetchone()

def schema_check_fct_prices_daily() -> list[str]:
    sql = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name='fct_prices_daily';
    """
    eng = get_engine()
    with eng.begin() as conn:
        cols = {r[0] for r in conn.execute(text(sql)).fetchall()}
    missing = EXPECTED_FCT_COLS - cols
    extra = cols - EXPECTED_FCT_COLS

    issues = []
    if missing:
        issues.append(f"Schema drift: missing columns {sorted(missing)}")
    if extra:
        issues.append(f"Schema drift: unexpected columns {sorted(extra)}")
    return issues

def run_sql_quality_checks() -> list[str]:
    issues = []

    dup = _fetch_one("""
      SELECT COUNT(*) FROM (
        SELECT symbol, date, COUNT(*) c
        FROM fct_prices_daily
        GROUP BY symbol, date
        HAVING COUNT(*) > 1
      ) t;
    """)[0]
    if dup > 0:
        issues.append(f"Duplicates found in curated table: {dup}")

    nulls = _fetch_one("""
      SELECT
        SUM(CASE WHEN symbol IS NULL THEN 1 ELSE 0 END) +
        SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END) AS null_key_count
      FROM fct_prices_daily;
    """)[0]
    if nulls > 0:
        issues.append(f"Nulls found in key columns: {nulls}")

    bad_range = _fetch_one("""
      SELECT COUNT(*)
      FROM fct_prices_daily
      WHERE close <= 0 OR open <= 0 OR high <= 0 OR low <= 0
         OR high < low
         OR volume < 0;
    """)[0]
    if bad_range > 0:
        issues.append(f"Range sanity violations: {bad_range}")

    return issues

def vendor_delivery_validation(run_id: str) -> list[str]:
    eng = get_engine()
    with eng.begin() as conn:
        prev = conn.execute(text("""
          SELECT run_id
          FROM pipeline_runs
          WHERE status='SUCCESS' AND run_id <> :run_id
          ORDER BY started_at DESC
          LIMIT 1;
        """), {"run_id": run_id}).fetchone()

        if not prev:
            return []

        prev_run = prev[0]
        cur_count = conn.execute(text("SELECT COUNT(*) FROM raw_prices WHERE run_id=:r;"), {"r": run_id}).fetchone()[0]
        prev_count = conn.execute(text("SELECT COUNT(*) FROM raw_prices WHERE run_id=:r;"), {"r": prev_run}).fetchone()[0]

    issues = []
    if prev_count > 0:
        change = (cur_count - prev_count) / float(prev_count)
        if abs(change) > 0.30:
            issues.append(f"Vendor delivery anomaly: raw row count changed by {change:.1%} vs previous run")
    return issues
