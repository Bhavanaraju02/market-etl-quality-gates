from __future__ import annotations
import argparse
import json
import os
import sys
from datetime import datetime, timezone

import pandas as pd

from etl.schema import ensure_schema
from etl.extract import fetch_stooq_daily
from etl.transform import clean_prices
from etl.load import load_raw, upsert_staging, build_curated_from_staging
from etl.db import exec_sql
from checks.quality_checks import schema_check_fct_prices_daily, run_sql_quality_checks, vendor_delivery_validation

def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def write_run(status: str, run_id: str, symbols: list[str], start: str|None, end: str|None, note: str|None=None):
    exec_sql("""
      INSERT INTO pipeline_runs(run_id,status,symbols,start_date,end_date,note)
      VALUES (:run_id,:status,:symbols,:start_date,:end_date,:note)
      ON CONFLICT (run_id) DO UPDATE SET status=EXCLUDED.status, note=EXCLUDED.note;
    """, {
        "run_id": run_id,
        "status": status,
        "symbols": ",".join(symbols),
        "start_date": start,
        "end_date": end,
        "note": note
    })

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="+", default=["AAPL.US","MSFT.US","NVDA.US"])
    ap.add_argument("--start", default="2023-01-01")
    ap.add_argument("--end", default=None)
    args = ap.parse_args()

    os.makedirs("reports", exist_ok=True)
    run_id = new_run_id()

    ensure_schema()
    write_run("STARTED", run_id, args.symbols, args.start, args.end)

    all_rows = []
    for sym in args.symbols:
        df = fetch_stooq_daily(sym, args.start, args.end)
        all_rows.append(df)

    combined = clean_prices(pd.concat(all_rows, ignore_index=True))

    load_raw(combined, run_id)
    upsert_staging(combined, run_id)
    build_curated_from_staging(run_id)

    issues: list[str] = []
    issues += schema_check_fct_prices_daily()
    issues += run_sql_quality_checks()
    issues += vendor_delivery_validation(run_id)

    report = {
        "run_id": run_id,
        "symbols": args.symbols,
        "start": args.start,
        "end": args.end,
        "status": "SUCCESS" if not issues else "FAILED",
        "issues": issues
    }

    report_path = f"reports/validation_report_{run_id}.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    if issues:
        write_run("FAILED", run_id, args.symbols, args.start, args.end, note="; ".join(issues))
        print(f"PIPELINE FAILED. Report: {report_path}")
        for i in issues:
            print(" -", i)
        sys.exit(2)

    write_run("SUCCESS", run_id, args.symbols, args.start, args.end, note=f"Report: {report_path}")
    print(f"PIPELINE SUCCESS. Report: {report_path}")

if __name__ == "__main__":
    main()
