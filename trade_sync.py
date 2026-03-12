"""
trade_sync.py
Pulls fill history from Kalshi portfolio API and writes to Supabase trades table.
Run on a schedule or manually via /sync-trades endpoint.
"""

import os
import re
import requests
from datetime import datetime, timezone
from kalshi_auth import kalshi_get

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


def sb_upsert(table, rows):
    """Upsert rows to Supabase via REST API using plain requests."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "resolution=merge-duplicates,return=minimal"
    }
    r = requests.post(url, json=rows, headers=headers, timeout=15)
    r.raise_for_status()
    return r


def sb_select(table, select="*", order=None, limit=None, filters=None):
    """Select rows from Supabase via REST API."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json"
    }
    params = {"select": select}
    if order:
        params["order"] = order
    if limit:
        params["limit"] = str(limit)
    if filters:
        params.update(filters)
    r = requests.get(url, headers=headers, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def parse_ticker(ticker):
    """
    Extract city, trade_date, bracket_label, lo, hi from a Kalshi ticker.

    Ticker formats observed:
      KXHIGHAUS-26MAR07-B79      between, old format (integer)
      KXHIGHTDC-26MAR12-B74.5   between, new format (float midpoint)
      KXHIGHTDC-26MAR12-T75     greater than (above threshold)
      KXHIGHTDC-26MAR12-T68     less than (below threshold) — need market lookup
      KXHIGHAUS-26MAR07-B60.5   between, float midpoint

    For between brackets: ticker midpoint = (floor + cap) / 2
      B74.5 → floor=74, cap=75 → label='74-75'
      B79   → floor=79, cap=80 → label='79-80'
      B60.5 → floor=60, cap=61 → label='60-61'

    For threshold brackets (T prefix): direction (> or <) stored in
    strike_type on the market object, which we don't have here.
    We store lo/hi as best guess and label as the raw value.
    Settlement backfill will correct via market lookup if needed.
    """
    m = re.match(r"([A-Z]+)-(\d{2})([A-Z]{3})(\d{2})-(.+)", ticker)
    if not m:
        return None

    series, yy, mon, dd, bracket_code = m.groups()
    city = SERIES_TO_CITY.get(series)
    if not city:
        return None

    # Parse settlement date
    try:
        date_str = f"20{yy}-{mon}-{dd}"
        trade_date = datetime.strptime(date_str, "%Y-%b-%d").date()
    except ValueError:
        return None

    lo, hi, label = None, None, bracket_code  # fallback label = raw code

    if bracket_code.startswith("B"):
        # Between bracket — midpoint may be integer or float with .5
        # B74.5 → floor=74, cap=75
        # B79   → floor=79, cap=80
        try:
            mid = float(bracket_code[1:])
            floor = int(mid - 0.5) if mid % 1 == 0.5 else int(mid)
            cap   = floor + 1
            lo    = floor
            hi    = cap + 1   # hi is exclusive upper bound for range checks
            label = f"{floor}-{cap}"
        except ValueError:
            pass

    elif bracket_code.startswith("T"):
        # Threshold bracket — T75 could be >75 or <75
        # We don't know direction without the market object.
        # Store the strike value and let settlement backfill correct.
        try:
            strike = int(float(bracket_code[1:]))
            # Conservative guess: store as lo=strike+1 (greater than)
            # Settlement backfill will correct this via market lookup
            lo    = strike + 1
            hi    = None
            label = f"{strike}+"   # placeholder — may be ≤ direction
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
    Tag each trade with the most likely strategy.
    edge1 = selling longshot YES / buying NO on tails
    edge2 = model divergence trade
    """
    if side == "no" and price >= 75:
        return "edge1"
    if side == "yes" and price <= 25:
        return "edge1"
    return "edge2"


def sync_fills(cursor=None):
    """
    Pull fills from Kalshi and upsert to Supabase trades table.
    cursor: ISO timestamp to fetch fills after (incremental sync).
    Returns number of fills synced.
    """
    # Find most recent fill we already have for incremental sync
    if cursor is None:
        result = sb_select(
            "trades",
            select="filled_at",
            order="filled_at.desc",
            limit=1
        )
        if result:
            cursor = result[0]["filled_at"]
            print(f"Incremental sync from {cursor}")
        else:
            print("Full sync — no existing trades found")

    # Fetch fills from Kalshi portfolio API
    # min_ts must be Unix timestamp in seconds (integer), not ISO string
    params = {"limit": 200}
    if cursor:
        try:
            dt = datetime.fromisoformat(cursor.replace("Z", "+00:00"))
            params["min_ts"] = int(dt.timestamp())
        except Exception:
            pass  # if parsing fails, do a full sync

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

        # Normalize price — Kalshi returns yes_price in cents
        side      = f.get("side", "").lower()        # 'yes' or 'no'
        action    = f.get("action", "").lower()       # 'buy' or 'sell'
        price     = int(f.get("yes_price", 0))        # cents
        count     = int(f.get("count", 0))            # contracts
        fees      = int(f.get("fees", 0))             # cents
        is_maker  = not f.get("is_taker", True)
        fill_id   = f.get("fill_id") or f.get("id", ticker)
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

    sb_upsert("trades", rows)
    print(f"Synced {len(rows)} trades to Supabase")
    return len(rows)


if __name__ == "__main__":
    n = sync_fills()
    print(f"Done — {n} trades synced")
