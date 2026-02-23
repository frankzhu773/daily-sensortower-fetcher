#!/usr/bin/env python3
"""
Sensor Tower Data Fetcher
Fetches top apps by downloads, download % increase, and top advertisers
from Sensor Tower API and stores them in Supabase.
"""

import os
import sys
import json
import time
import requests
from datetime import datetime, timedelta

# ─── Configuration ───────────────────────────────────────────────────────────
ST_API_KEY = os.environ.get("SENSORTOWER_API_KEY", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

ST_BASE = "https://api.sensortower.com"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}

# ─── Helper: Sensor Tower API call with retry ────────────────────────────────
def st_get(path, params):
    """Make a GET request to Sensor Tower API with retry logic."""
    params["auth_token"] = ST_API_KEY
    for attempt in range(5):
        try:
            resp = requests.get(f"{ST_BASE}{path}", params=params, timeout=60)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                wait = 10 * (attempt + 1)
                print(f"  Rate limited, waiting {wait}s... (attempt {attempt+1})")
                time.sleep(wait)
            else:
                print(f"  API error {resp.status_code}: {resp.text[:300]}")
                if attempt < 4:
                    time.sleep(3)
        except Exception as e:
            print(f"  Request error: {e}")
            if attempt < 4:
                time.sleep(5)
    return None


def lookup_app(app_id):
    """Look up app name and icon from Sensor Tower unified endpoint."""
    time.sleep(0.3)  # Rate limit: 6 req/s
    data = st_get(f"/v1/unified/apps/{app_id}", {})
    if data and isinstance(data, dict):
        name = data.get("name", "")
        if not name:
            # Fallback: try to get name from sub_apps
            sub_apps = data.get("sub_apps", [])
            if sub_apps:
                name = sub_apps[0].get("name", "Unknown")
            else:
                name = "Unknown"
        return {
            "name": name,
            "icon_url": data.get("icon_url", ""),
            "publisher": data.get("unified_publisher_name", data.get("publisher_name", "Unknown")),
        }
    return {"name": "Unknown", "icon_url": "", "publisher": "Unknown"}


def aggregate_entities(item):
    """
    Aggregate download/revenue data across all entities (platforms) for a unified app.
    The API returns per-platform data in the 'entities' array.
    We sum across all entities to get the true unified total.
    """
    entities = item.get("entities", [])
    if not entities:
        # No entities array — data is at the top level (non-unified response)
        return {
            "downloads": item.get("units_absolute", item.get("absolute", 0)),
            "prev_downloads": item.get("comparison_units_value", 0),
            "delta": item.get("units_delta", item.get("delta", 0)),
            "pct_change": item.get("units_transformed_delta", item.get("transformed_delta", 0)),
        }

    total_downloads = 0
    total_prev = 0
    total_delta = 0

    for ent in entities:
        total_downloads += ent.get("units_absolute", ent.get("absolute", 0)) or 0
        total_prev += ent.get("comparison_units_value", 0) or 0
        total_delta += ent.get("units_delta", ent.get("delta", 0)) or 0

    # For pct_change, compute from totals rather than averaging
    pct_change = 0
    if total_prev and total_prev > 0:
        pct_change = total_delta / total_prev
    else:
        # Use the first entity's transformed_delta as fallback
        pct_change = entities[0].get("units_transformed_delta", entities[0].get("transformed_delta", 0)) or 0

    return {
        "downloads": total_downloads,
        "prev_downloads": total_prev,
        "delta": total_delta,
        "pct_change": pct_change,
    }


# ─── Supabase helpers ────────────────────────────────────────────────────────
def ensure_table(table_name, sample_row):
    """Check if table exists by trying a select."""
    url = f"{SUPABASE_URL}/rest/v1/{table_name}?select=id&limit=1"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code == 200:
        print(f"  Table '{table_name}' exists.")
        return True
    elif resp.status_code in (404, 406):
        print(f"  Table '{table_name}' does not exist. Please create it in Supabase dashboard.")
        return False
    else:
        print(f"  Table check error {resp.status_code}: {resp.text[:200]}")
        return False


def upsert_rows(table_name, rows):
    """Insert or upsert rows into Supabase table."""
    if not rows:
        print(f"  No rows to insert into {table_name}")
        return

    url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    headers = {**HEADERS, "Prefer": "resolution=merge-duplicates,return=minimal"}

    batch_size = 50
    total_inserted = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        resp = requests.post(url, headers=headers, json=batch)
        if resp.status_code in (200, 201, 204):
            total_inserted += len(batch)
        else:
            print(f"  Insert error {resp.status_code}: {resp.text[:300]}")
            for row in batch:
                resp2 = requests.post(url, headers=headers, json=row)
                if resp2.status_code in (200, 201, 204):
                    total_inserted += 1
                else:
                    print(f"    Row insert error: {resp2.text[:200]}")

    print(f"  Inserted {total_inserted}/{len(rows)} rows into {table_name}")


# ─── Fetch 1: Top 15 Apps by Downloads (last 30 days) ───────────────────────
def fetch_top_downloads():
    """Fetch top 15 apps by absolute downloads in the last month."""
    print("\n=== Fetching Top 15 Apps by Downloads ===")

    now = datetime.utcnow()
    if now.day <= 5:
        first_of_month = (now.replace(day=1) - timedelta(days=1)).replace(day=1)
    else:
        first_of_month = now.replace(day=1)

    date_str = first_of_month.strftime("%Y-%m-%d")
    print(f"  Date: {date_str}")

    data = st_get("/v1/unified/sales_report_estimates_comparison_attributes", {
        "comparison_attribute": "absolute",
        "time_range": "month",
        "measure": "units",
        "category": "0",
        "date": date_str,
        "device_type": "total",
        "limit": 15,
        "regions": "WW",
    })

    if not data:
        print("  ERROR: No data returned")
        return []

    print(f"  Got {len(data)} apps")

    rows = []
    for rank, item in enumerate(data[:15], 1):
        unified_id = item.get("app_id", "")

        # Look up app name with robust retry
        app_info = lookup_app(unified_id)

        # Aggregate downloads across all platforms (iOS + Android + lite variants)
        agg = aggregate_entities(item)

        row = {
            "fetch_date": now.strftime("%Y-%m-%d"),
            "period_start": date_str,
            "rank": rank,
            "app_id": str(unified_id),
            "app_name": app_info["name"],
            "publisher": app_info["publisher"],
            "icon_url": app_info["icon_url"],
            "downloads": agg["downloads"],
            "previous_downloads": agg["prev_downloads"],
            "download_delta": agg["delta"],
            "download_pct_change": round(agg["pct_change"] * 100, 2),
        }
        rows.append(row)
        print(f"  #{rank}: {app_info['name']} — {agg['downloads']:,} downloads (prev: {agg['prev_downloads']:,}, delta: {agg['delta']:,})")

    return rows


# ─── Fetch 2: Top 15 Apps by Download % Increase ────────────────────────────
def fetch_top_download_growth():
    """Fetch top 15 apps by download percentage increase in the last month."""
    print("\n=== Fetching Top 15 Apps by Download % Increase ===")

    now = datetime.utcnow()
    if now.day <= 5:
        first_of_month = (now.replace(day=1) - timedelta(days=1)).replace(day=1)
    else:
        first_of_month = now.replace(day=1)

    date_str = first_of_month.strftime("%Y-%m-%d")
    print(f"  Date: {date_str}")

    data = st_get("/v1/unified/sales_report_estimates_comparison_attributes", {
        "comparison_attribute": "transformed_delta",
        "time_range": "month",
        "measure": "units",
        "category": "0",
        "date": date_str,
        "device_type": "total",
        "limit": 15,
        "regions": "WW",
    })

    if not data:
        print("  ERROR: No data returned")
        return []

    print(f"  Got {len(data)} apps")

    rows = []
    for rank, item in enumerate(data[:15], 1):
        unified_id = item.get("app_id", "")

        app_info = lookup_app(unified_id)
        agg = aggregate_entities(item)

        row = {
            "fetch_date": now.strftime("%Y-%m-%d"),
            "period_start": date_str,
            "rank": rank,
            "app_id": str(unified_id),
            "app_name": app_info["name"],
            "publisher": app_info["publisher"],
            "icon_url": app_info["icon_url"],
            "downloads": agg["downloads"],
            "previous_downloads": agg["prev_downloads"],
            "download_delta": agg["delta"],
            "download_pct_change": round(agg["pct_change"] * 100, 2),
        }
        rows.append(row)
        print(f"  #{rank}: {app_info['name']} — {agg['pct_change']*100:.1f}% increase ({agg['downloads']:,} downloads)")

    return rows


# ─── Fetch 3: Top 15 Advertisers by Spend ───────────────────────────────────
def fetch_top_advertisers():
    """Fetch top 15 advertisers by ad spend (Share of Voice) in the last month."""
    print("\n=== Fetching Top 15 Advertisers ===")

    now = datetime.utcnow()
    if now.day <= 5:
        first_of_month = (now.replace(day=1) - timedelta(days=1)).replace(day=1)
    else:
        first_of_month = now.replace(day=1)

    date_str = first_of_month.strftime("%Y-%m-%d")
    print(f"  Date: {date_str}")

    data = st_get("/v1/unified/ad_intel/top_apps", {
        "role": "advertisers",
        "date": date_str,
        "period": "month",
        "category": "0",
        "country": "US",
        "network": "All Networks",
        "limit": 25,
    })

    if not data:
        print("  ERROR: No data returned")
        return []

    apps = data.get("apps", [])
    print(f"  Got {len(apps)} advertisers")

    rows = []
    for rank, app in enumerate(apps[:15], 1):
        row = {
            "fetch_date": now.strftime("%Y-%m-%d"),
            "period_start": date_str,
            "rank": rank,
            "app_id": str(app.get("app_id", "")),
            "app_name": app.get("name", app.get("humanized_name", "Unknown")),
            "publisher": app.get("publisher_name", "Unknown"),
            "icon_url": app.get("icon_url", ""),
            "sov": app.get("sov", 0),
        }
        rows.append(row)
        print(f"  #{rank}: {row['app_name']} ({row['publisher']}) — SoV: {row['sov']:.3f}")

    return rows


# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("Sensor Tower Data Fetcher")
    print(f"Run time: {datetime.utcnow().isoformat()}")
    print("=" * 60)

    if not ST_API_KEY:
        print("ERROR: SENSORTOWER_API_KEY not set")
        sys.exit(1)
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set")
        sys.exit(1)

    for table in ["download_rank_30d", "download_percent_rank_30d", "advertiser_rank_30d"]:
        if not ensure_table(table, {}):
            print(f"WARNING: Table '{table}' may not exist. Will attempt inserts anyway.")

    # 1. Top downloads
    download_rows = fetch_top_downloads()
    if download_rows:
        upsert_rows("download_rank_30d", download_rows)

    # 2. Top download % increase
    growth_rows = fetch_top_download_growth()
    if growth_rows:
        upsert_rows("download_percent_rank_30d", growth_rows)

    # 3. Top advertisers
    advertiser_rows = fetch_top_advertisers()
    if advertiser_rows:
        upsert_rows("advertiser_rank_30d", advertiser_rows)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print(f"  Downloads ranking: {len(download_rows)} rows")
    print(f"  Download growth ranking: {len(growth_rows)} rows")
    print(f"  Advertiser ranking: {len(advertiser_rows)} rows")
    print("=" * 60)


if __name__ == "__main__":
    main()
