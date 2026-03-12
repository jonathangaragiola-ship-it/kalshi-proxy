"""
settlement_backfill.py
Fetches NWS CLI settlement data from Iowa State Mesonet and updates
the trades table with cli_high, settled_bracket, won, and pnl_cents.

Run manually or on a nightly cron after midnight local time.
"""

import os
import requests
from datetime import datetime, date, timedelta

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

# Iowa State Mesonet ASOS station codes (drop the K prefix)
CITY_TO_ASOS = {
    "KATL": "ATL", "KAUS": "AUS", "KBOS": "BOS", "KORD": "ORD",
    "KDFW": "DFW", "KDEN": "DEN", "KIAH": "IAH", "KLAS": "LAS",
    "KLAX": "LAX", "KMIA": "MIA", "KMSP": "MSP", "KMSY": "MSY",
    "KNYC": "LGA", "KOKC": "OKC", "KPHL": "PHL", "KPHX": "PHX",
    "KSAT": "SAT", "KSFO": "SFO", "KSEA": "SEA", "KDCA": "DCA",
}


def sb_select(table, select="*", order=None, limit=None, filters=None):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json"
    }
    params = {"select": select}
    if order:   params["order"]  = order
    if limit:   params["limit"]  = str(limit)
    if filters: params.update(filters)
    r = requests.get(url, headers=headers, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def sb_update(table, match_col, match_val, updates):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal"
    }
    params = {match_col: f"eq.{match_val}"}
    r = requests.patch(url, json=updates, headers=headers, params=params, timeout=15)
    r.raise_for_status()
    return r


def fetch_cli_high(asos_station, target_date):
    """
    Fetch the daily high temperature from Iowa State Mesonet for a given
    ASOS station and date. Returns integer °F or None if unavailable.
    target_date: date object
    """
    # Mesonet daily summary endpoint
    url = "https://mesonet.agron.iastate.edu/cgi-bin/request/daily.py"
    params = {
        "network":  "ASOS",
        "station":  asos_station,
        "year1":    target_date.year,
        "month1":   target_date.month,
        "day1":     target_date.day,
        "year2":    target_date.year,
        "month2":   target_date.month,
        "day2":     target_date.day,
        "vars[]":   "max_tmpf",
        "what":     "view",
        "delim":    "comma",
        "gis":      "no",
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        lines = r.text.strip().split("\n")
        # Format: station,date,max_tmpf
        # First line is header
        if len(lines) < 2:
            return None
        for line in lines[1:]:
            parts = line.strip().split(",")
            if len(parts) >= 3 and parts[2] not in ("", "M", "None"):
                try:
                    return round(float(parts[2]))
                except ValueError:
                    return None
        return None
    except Exception as e:
        print(f"  Mesonet error for {asos_station} {target_date}: {e}")
        return None


def determine_settled_bracket(cli_high, bracket_label, bracket_lo, bracket_hi):
    """
    Given the CLI high and a bracket's bounds, determine if this bracket settled YES.
    bracket_lo: inclusive lower bound (None = no lower bound)
    bracket_hi: exclusive upper bound (None = no upper bound)

    Returns True if cli_high falls in this bracket, False otherwise.
    """
    if cli_high is None:
        return None

    # Between bracket: lo <= cli_high <= lo+1 (hi is exclusive, so cli_high < hi)
    if bracket_lo is not None and bracket_hi is not None:
        return bracket_lo <= cli_high < bracket_hi

    # Greater than bracket: cli_high >= lo (no upper bound)
    if bracket_lo is not None and bracket_hi is None:
        return cli_high >= bracket_lo

    # Less than or equal bracket: cli_high < hi (no lower bound)
    if bracket_lo is None and bracket_hi is not None:
        return cli_high < bracket_hi

    return None


def compute_pnl(side, action, price, contracts, fees, won):
    """
    Compute net P&L in cents for a settled trade.
    For NO buyer: win = (100 - price) * contracts - fees
                  lose = -price * contracts - fees
    price is in cents (what you paid per contract).
    """
    if won is None:
        return None

    if side == "no" and action == "buy":
        if won:
            # YES settled False → NO wins → collect (100 - price) per contract
            gross = (100 - price) * contracts
        else:
            # YES settled True → NO loses → lose price per contract
            gross = -price * contracts
        return gross - fees

    if side == "yes" and action == "buy":
        if won:
            gross = (100 - price) * contracts
        else:
            gross = -price * contracts
        return gross - fees

    return None


def run_backfill():
    """
    Find all unsettled trades with past trade_dates and fill in settlement data.
    """
    today = date.today()

    # Fetch all trades missing cli_high with trade_date in the past
    all_trades = sb_select("trades", select="*")
    unsettled = [
        t for t in all_trades
        if t.get("cli_high") is None
        and t.get("trade_date") is not None
    ]

    if not unsettled:
        print("No unsettled trades to backfill")
        return 0

    print(f"Found {len(unsettled)} unsettled trades")

    # Group by city + date to minimize Mesonet API calls
    city_date_pairs = list({
        (t["city"], t["trade_date"]) for t in unsettled
    })
    print(f"Fetching CLI data for {len(city_date_pairs)} city/date combinations")

    # Fetch CLI highs
    cli_cache = {}
    for city, trade_date_str in city_date_pairs:
        asos = CITY_TO_ASOS.get(city)
        if not asos:
            print(f"  No ASOS mapping for {city} — skipping")
            continue
        target_date = datetime.strptime(trade_date_str, "%Y-%m-%d").date()
        cli_high = fetch_cli_high(asos, target_date)
        cli_cache[(city, trade_date_str)] = cli_high
        status = f"{cli_high}°F" if cli_high is not None else "unavailable"
        print(f"  {city} {trade_date_str}: {status}")

    # Update each trade
    updated = 0
    for trade in unsettled:
        city       = trade["city"]
        trade_date = trade["trade_date"]
        cli_high   = cli_cache.get((city, trade_date))

        if cli_high is None:
            print(f"  Skipping {trade['id'][:8]} — no CLI data yet")
            continue

        # Determine if this bracket settled YES
        bracket_settled = determine_settled_bracket(
            cli_high,
            trade.get("bracket_label"),
            trade.get("bracket_lo"),
            trade.get("bracket_hi")
        )

        # For NO buyer: won = bracket did NOT settle YES
        side   = trade.get("side", "")
        action = trade.get("action", "")
        if side == "no":
            won = not bracket_settled if bracket_settled is not None else None
        else:
            won = bracket_settled

        pnl = compute_pnl(
            side, action,
            trade.get("price", 0),
            trade.get("contracts", 0),
            trade.get("fees", 0),
            won
        )

        # Build settled_bracket label
        settled_label = trade.get("bracket_label") if bracket_settled else None

        updates = {
            "cli_high":        cli_high,
            "settled_bracket": settled_label,
            "won":             won,
            "pnl_cents":       pnl,
        }

        try:
            sb_update("trades", "id", trade["id"], updates)
            result = "WON" if won else "LOST"
            print(f"  {city} {trade_date} {trade.get('bracket_label')} "
                  f"CLI={cli_high}°F → {result} pnl={pnl}¢")
            updated += 1
        except Exception as e:
            print(f"  Error updating {trade['id'][:8]}: {e}")

    print(f"\nBackfill complete — {updated} trades updated")
    return updated


if __name__ == "__main__":
    run_backfill()
