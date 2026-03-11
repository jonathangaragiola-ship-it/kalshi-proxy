"""
trade_sync.py
Pulls fill history from Kalshi portfolio API and writes to Supabase trades table.
Run on a schedule (every 30 min) or manually.
"""

import os
import re
from datetime import datetime, timezone
from kalshi_auth import kalshi_get
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

# Kalshi series → city key mapping
SERIES_TO_CITY = {
    "KXHIGHTATL":  "KATL", "KXHIGHAUS":   "KAUS", "KXHIGHTBOS":  "KBOS",
    "KXHIGHCHI":   "KORD", "KXHIGHTDAL":  "KDFW", "KXHIGHDEN":   "KDEN",
    "KXHIGHTHOU":  "KIAH", "KXHIGHTLV":   "KLAS", "KXHIGHLAX":   "KLAX",
    "KXHIGHMIA":   "KMIA", "KXHIGHTMIN":  "KMSP", "KXHIGHTNOLA": "KMSY",
    "KXHIGHNY":    "KNYC", "KXHIGHTOKC":  "KOKC", "KXHIGHPHIL":  "KPHL",
    "KXHIGHTPHX":  "KPHX", "KXHIGHTSATX": "KSAT", "KXHIGHTSFO":  "KSFO",
    "KXHIGHTSEA":  "KSEA", "KXHIGHTDC":   "KDCA",
}


def parse_ticker(ticker):
    """
    Extract city, trade_date, bracket_label from a Kalshi ticker.
    e.g. KXHIGHAUS-26MAR07-B79 → city=KAUS, date=2026-03-07, bracket=79+
    Returns dict or None if not a weather market.
    """
    # Match series-date-bracket pattern
    m = re.match(r"([A-Z]+)-(\d{2})([A-Z]{3})(\d{2})-(.+)", ticker)
    if not m:
        return None

    series, yy, mon, dd, bracket_code = m.groups()
    city = SERIES_TO_CITY.get(series)
    if not city:
        return None

    # Parse date
    try:
        date_str = f"20{yy}-{mon}-{dd}"
        trade_date = datetime.strptime(date_str, "%Y-%b-%d").date()
    except ValueError:
        return None

    # Parse bracket code → label and lo/hi
    lo, hi, label = None, None, bracket_code
    if bracket_code.startswith("B"):
        # Between bracket e.g. B79 = 79-80
        try:
            lo = int(bracket_code[1:])
            hi = lo + 2
            label = f"{lo}-{hi-1}"
        except ValueError:
            pass
    elif bracket_code.startswith("T"):
        # Above/below threshold
        try:
            val = int(bracket_code[1:])
            lo = val + 1
            hi = None
            label = f"{val+1}+"
        except ValueError:
            pass

    return {
        "city":          city,
        "trade_date":    trade_date.isoformat(),
        "bracket_label": label,
        "bracket_lo":    lo,
        "bracket_hi":    hi,
    }


def infer_strategy(price, side, is_maker):
    """
    Tag each trade with the most likely strategy based on price and side.
    edge1 = selling longshot YES (buying NO at high price)
    edge2 = model divergence trade
    edge3 = execution optimization (maker on any trade)
    """
    if side == "no" and price >= 75:
        return "edge1"
    if side == "yes" and price <= 25:
        return "edge1"
    return "edge2"


def sync_fills(cursor=None):
    """
    Pull fills from Kalshi and upsert to Supabase.
    cursor: ISO timestamp string to fetch fills after (for incremental sync).
    Returns number of fills synced.
    """
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Determine cursor — find most recent fill we already have
    if cursor is None:
        result = sb.table("trades") \
            .select("filled_at") \
            .order("filled_at", desc=True) \
            .limit(1) \
            .execute()
        if result.data:
            cursor = result.data[0]["filled_at"]
            print(f"Incremental sync from {cursor}")
        else:
            print("Full sync — no existing trades found")

    # Fetch fills from Kalshi
    params = {"limit": 200}
    if cursor:
        params["min_ts"] = cursor

    data  = kalshi_get("/trade-api/v2/portfolio/fills", params=params)
    fills = data.get("fills", [])
    print(f"Fetched {len(fills)} fills from Kalshi")

    if not fills:
        return 0

    # Build rows for Supabase
    rows = []
    for f in fills:
        ticker  = f.get("market_ticker", "")
        parsed  = parse_ticker(ticker)
        if not parsed:
            print(f"  Skipping non-weather ticker: {ticker}")
            continue

        side     = f.get("side", "").lower()       # 'yes' or 'no'
        action   = f.get("action", "").lower()      # 'buy' or 'sell'
        price    = f.get("yes_price", 0)            # cents
        count    = f.get("count", 0)                # contracts
        fees     = f.get("fees", 0)                 # cents
        is_maker = f.get("is_taker", True) == False
        fill_id  = f.get("fill_id") or f.get("id", "")
        filled_at = f.get("created_time") or f.get("timestamp", "")

        row = {
            "id":            fill_id,
            "city":          parsed["city"],
            "trade_date":    parsed["trade_date"],
            "bracket_label": parsed["bracket_label"],
            "bracket_lo":    parsed["bracket_lo"],
            "bracket_hi":    parsed["bracket_hi"],
            "side":          side,
            "action":        action,
            "is_maker":      is_maker,
            "price":         price,
            "contracts":     count,
            "fees":          fees,
            "strategy":      infer_strategy(price, side, is_maker),
            "kalshi_ticker": ticker,
            "filled_at":     filled_at,
        }
        rows.append(row)

    if not rows:
        print("No weather market fills to sync")
        return 0

    # Upsert to Supabase (on conflict update)
    result = sb.table("trades").upsert(rows).execute()
    print(f"Synced {len(rows)} trades to Supabase")
    return len(rows)


if __name__ == "__main__":
    n = sync_fills()
    print(f"Done — {n} trades synced")
