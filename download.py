#!/usr/bin/env python3
"""
polygon_intraday.py
Download 1-minute intraday bars from Polygon.io.

Usage
-----
python polygon_intraday.py  --ticker SPY           \
                            --start 2025-06-01     \
                            --end   2025-06-27     \
                            --out   ./data

Notes
-----
* Works with **Stocks Basic** (free) for equities like SPY.
* Works with **Indices Basic** (free) for index tickers that Polygon exposes
  on the zero-cost plan, e.g. "I:SPX".  Some index symbols are paywalled.
* Free plans are limited to **5 REST calls per minute** and give **≈2 years**
  of minute bars.:contentReference[oaicite:0]{index=0}
"""
import argparse, os, time, sys
from datetime import datetime, timedelta, timezone
from pathlib     import Path

import pandas as pd
import requests
from pandas.tseries.offsets import BDay

# ----------------------------------------------------------------------
API_KEY_PATH = "api_key.txt"         # keep your key out of source control
with open(API_KEY_PATH) as f:
    API_KEY = f.read().strip()

ROOT_URL = "https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{day}/{day}"

def fetch_day(ticker: str, day: str) -> pd.DataFrame:
    """
    Fetch one calendar day of 1-minute bars.
    Returns empty DataFrame on holidays / no data.
    """
    url = ROOT_URL.format(ticker=ticker, day=day)
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50_000,      # Polygon max per request
        "apiKey": API_KEY,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    js = r.json()
    if js.get("resultsCount", 0) == 0:
        return pd.DataFrame()                       # market holiday or no bars
    df = pd.DataFrame(js["results"])
    df["datetime"] = (pd.to_datetime(df["t"], unit="ms", utc=True)
                        .dt.tz_convert("America/New_York"))
    return (df[["datetime", "o", "h", "l", "c", "v"]]
              .rename(columns={"o": "open", "h": "high",
                               "l": "low",  "c": "close",
                               "v": "volume"})
              .set_index("datetime"))

def daterange(start: str, end: str):
    cur = pd.to_datetime(start)
    end = pd.to_datetime(end)
    while cur <= end:
        if cur.weekday() < 5:        # Monday-Friday only
            yield cur.strftime("%Y-%m-%d")
        cur += BDay()

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--ticker", required=True,
                   help='e.g. "SPY" or "I:SPX"')
    p.add_argument("--start",  required=True,
                   help="YYYY-MM-DD (first trading day)")
    p.add_argument("--end",    required=True,
                   help="YYYY-MM-DD (last trading day)")
    p.add_argument("--out",    default=".",
                   help="output directory (CSV per day)")
    args = p.parse_args()

    outdir = Path(args.out)
    outdir.mkdir(parents=True, exist_ok=True)

    reqs_in_window = 0
    window_start   = time.time()

    for day in daterange(args.start, args.end):
        csv_path = outdir / f"{args.ticker.replace(':','_')}_{day}.csv"
        if csv_path.exists():
            print(f"[skip] {csv_path} already exists")
            continue

        # Rate-limit — simple 60-second sliding window
        if reqs_in_window == 5:
            delta = 60 - (time.time() - window_start)
            if delta > 0:
                time.sleep(delta + 0.1)
            window_start = time.time()
            reqs_in_window = 0

        try:
            df = fetch_day(args.ticker, day)
        except Exception as e:
            print(f"[warn] {day}: {e}", file=sys.stderr)
            continue

        if df.empty:
            print(f"[info] {day}: no data returned (holiday?)")
        else:
            df.to_csv(csv_path)
            print(f"[ok]   {csv_path}  ({len(df)} bars)")

        reqs_in_window += 1

if __name__ == "__main__":
    main()
