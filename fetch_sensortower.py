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
    for attempt in range(3):
        try:
            resp = requests.get(f"{ST_BASE}{path}", params=params, timeout=60)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                print(f"  Rate limited, waiting 10s... (attempt {attempt+1})")
                time.sleep(10)
            else:
                print(f"  API error {resp.status_code}: {resp.text[:300]}")
                if attempt < 2:
                    time.sleep(5)
        except Exception as e:
            print(f"  Request error: {e}")
            if attempt < 2:
                time.sleep(5)
    return None


def lookup_app(app_id):
    """Look up app name and icon from Sensor Tower."""
    data = st_get(f"/v1/unified/apps/{app_id}", {})
    if data:
        return {
            "name": data.get("name", "Unknown"),
            "icon_url": data.get("icon_url", ""),
            "publisher": data.get("unified_publisher_name", "Unknown"),
        }
    return {"name": "Unknown", "icon_url": "", "publisher": "Unknown"}


# ─── Supabase helpers ────────────────────────────────────────────────────────
def ensure_table(table_name, sample_row):
    """Check if table exists by trying a select. If not, create via CSV import approach."""
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

    # Insert in batches of 50
    batch_size = 50
    total_inserted = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        resp = requests.post(url, headers=headers, json=batch)
        if resp.status_code in (200, 201, 204):
            total_inserted += len(batch)
        else:
            print(f"  Insert error {resp.status_code}: {resp.text[:300]}")
            # Try individual inserts for the failed batch
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
    # Use previous month if we're early in the month (data may not be ready)
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
        entity = item.get("entities", [{}])[0] if "entities" in item else item

        # Look up app name
        time.sleep(0.2)  # Rate limit: 6 req/s
        app_info = lookup_app(unified_id)

        downloads = entity.get("units_absolute", 0)
        prev_downloads = entity.get("comparison_units_value", 0)
        delta = entity.get("units_delta", 0)
        pct_change = entity.get("units_transformed_delta", 0)

        row = {
            "fetch_date": now.strftime("%Y-%m-%d"),
            "period_start": date_str,
            "rank": rank,
            "app_id": unified_id,
            "app_name": app_info["name"],
            "publisher": app_info["publisher"],
            "icon_url": app_info["icon_url"],
            "downloads": downloads,
            "previous_downloads": prev_downloads,
            "download_delta": delta,
            "download_pct_change": round(pct_change * 100, 2),
        }
        rows.append(row)
        print(f"  #{rank}: {app_info['name']} — {downloads:,} downloads")

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
        entity = item.get("entities", [{}])[0] if "entities" in item else item

        time.sleep(0.2)
        app_info = lookup_app(unified_id)

        downloads = entity.get("units_absolute", 0)
        prev_downloads = entity.get("comparison_units_value", 0)
        delta = entity.get("units_delta", 0)
        pct_change = entity.get("units_transformed_delta", 0)

        row = {
            "fetch_date": now.strftime("%Y-%m-%d"),
            "period_start": date_str,
            "rank": rank,
            "app_id": unified_id,
            "app_name": app_info["name"],
            "publisher": app_info["publisher"],
            "icon_url": app_info["icon_url"],
            "downloads": downloads,
            "previous_downloads": prev_downloads,
            "download_delta": delta,
            "download_pct_change": round(pct_change * 100, 2),
        }
        rows.append(row)
        print(f"  #{rank}: {app_info['name']} — {pct_change*100:.1f}% increase ({downloads:,} downloads)")

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
            "app_id": app.get("app_id", ""),
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

    # Validate config
    if not ST_API_KEY:
        print("ERROR: SENSORTOWER_API_KEY not set")
        sys.exit(1)
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set")
        sys.exit(1)

    # Check tables exist
    for table in ["download_rank_30d", "download_percent_rank_30d", "advertiser_rank_30d"]:
        if not ensure_table(table, {}):
            print(f"WARNING: Table '{table}' may not exist. Will attempt inserts anyway.")

    # Fetch and store data
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
