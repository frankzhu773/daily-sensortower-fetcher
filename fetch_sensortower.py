#!/usr/bin/env python3
"""
Sensor Tower Data Fetcher
Fetches top apps by downloads (7-day daily avg), download % increase (7-day),
and top advertisers from Sensor Tower API and stores them in Supabase.

All download counts are stored as daily averages (7-day total / 7).
Percentage changes remain the same (WoW % is identical for totals vs averages).

Uses time_range=day with date+end_date for exact 7-day windows, avoiding
the Monday-snapping behavior of time_range=week. For example, if run on
Mar 2 with a 3-day data delay:
  Current period:  Feb 21 – Feb 27 (latest_date - 6 to latest_date)
  Previous period: Feb 14 – Feb 20 (auto-computed by the API)

Note: Sensor Tower data has a ~3-day delay, so we use (today - 3 days)
as the latest available date, and fetch the 7-day window ending on that date.
"""

import os
import sys
import json
import time
import re
import requests
from datetime import datetime, timedelta

# ─── Configuration ───────────────────────────────────────────────────────────
ST_API_KEY = os.environ.get("SENSORTOWER_API_KEY", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

ST_BASE = "https://api.sensortower.com"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}

DATA_DELAY_DAYS = 3  # Sensor Tower data is typically 3 days behind

GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"


def call_gemini(prompt, system_instruction, max_tokens=2000, use_search=False, retries=3):
    """Call Gemini API with retry logic and exponential backoff."""
    if not GEMINI_API_KEY:
        return None

    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "systemInstruction": {"parts": [{"text": system_instruction}]},
        "generationConfig": {"maxOutputTokens": max_tokens, "temperature": 0.3},
    }
    if use_search:
        body["tools"] = [{"google_search": {}}]

    for attempt in range(retries):
        try:
            resp = requests.post(
                f"{GEMINI_URL}?key={GEMINI_API_KEY}",
                headers={"Content-Type": "application/json"},
                json=body,
                timeout=30,
            )
            if resp.status_code == 200:
                data = resp.json()
                if "candidates" in data:
                    parts = data["candidates"][0]["content"]["parts"]
                    text_parts = [p["text"] for p in parts if "text" in p]
                    return " ".join(text_parts).strip()
            elif resp.status_code in (429, 500, 502, 503, 504):
                wait = 3 * (2 ** attempt)
                print(f"    Gemini {resp.status_code}, retrying in {wait}s (attempt {attempt+1}/{retries})...")
                time.sleep(wait)
            else:
                print(f"    Gemini error {resp.status_code}: {resp.text[:200]}")
                return None
        except Exception as e:
            print(f"    Gemini exception: {e}")
            if attempt < retries - 1:
                time.sleep(3)
    return None


def batch_summarize_descriptions(rows):
    """Use Gemini to summarize all app descriptions in a single batch call.
    
    Produces exactly 2 sentences per app in English. Non-English descriptions
    are translated. App names that are not in English are kept as-is.
    """
    if not rows or not GEMINI_API_KEY:
        return rows

    print(f"\n  Batch summarizing {len(rows)} app descriptions...")

    entries_text = ""
    for idx, row in enumerate(rows):
        raw_desc = row.get("app_description", "") or ""
        # Truncate raw description to 300 chars to keep prompt manageable
        raw_desc = raw_desc[:300].strip()
        entries_text += f"\n{idx + 1}. App: {row.get('app_name', 'Unknown')}\n   Description: {raw_desc if raw_desc else '(no description available)'}\n"

    prompt = f"""For each app below, write EXACTLY 2 sentences describing what the app does.

RULES:
- Write EXACTLY 2 sentences per app. Not 1, not 3. TWO sentences.
- Sentence 1: What the app is and its primary function.
- Sentence 2: A key feature or what makes it useful to users.
- ALL output MUST be in English. Translate any non-English descriptions to English.
- App names that are not in English should be kept in their original language (do NOT translate app names).
- Do NOT include: ranking data, pricing, update dates, chart positions, download counts.
- Do NOT start with "This app..." — start directly with the app name or a description of its function.
- If the description is empty or unhelpful, use your knowledge to describe the app.
- Keep each summary under 200 characters total.

Apps:
{entries_text}

Respond with ONLY a JSON array of objects, each with "index" (1-based) and "summary" (exactly 2 sentences in English).
Example: [{{"index": 1, "summary": "TikTok is a short-form video platform where users create and share entertaining clips. It features AI-powered recommendations, filters, effects, and a vast music library."}}]
No other text, no markdown code blocks."""

    system = "You are a professional app reviewer. Write exactly TWO sentences per app in English — no more, no less. Be specific and factual. Translate all non-English content to English except app names. Return valid JSON only."

    result = call_gemini(prompt, system, max_tokens=4000, use_search=True)

    if not result:
        print("    WARNING: Batch summarization failed, keeping raw descriptions")
        return rows

    # Parse JSON response
    cleaned = result.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned)
        cleaned = re.sub(r'\s*```$', '', cleaned)
        cleaned = cleaned.strip()

    summaries = []
    try:
        summaries = json.loads(cleaned)
    except json.JSONDecodeError:
        # Try to find JSON array
        match = re.search(r'\[\s*\{.*?\}\s*\]', result, re.DOTALL)
        if match:
            try:
                summaries = json.loads(match.group(0))
            except json.JSONDecodeError:
                pass

    if not summaries:
        # Regex fallback
        for m in re.finditer(r'"index"\s*:\s*(\d+)\s*,\s*"summary"\s*:\s*"((?:[^"\\]|\\.)*)"', result):
            try:
                summaries.append({"index": int(m.group(1)), "summary": m.group(2)})
            except (ValueError, IndexError):
                continue

    if not summaries:
        print("    WARNING: Failed to parse batch summarization response")
        return rows

    # Apply summaries to rows
    updated = 0
    for item in summaries:
        idx = item.get("index", 0) - 1
        summary = item.get("summary", "")
        if 0 <= idx < len(rows) and summary:
            rows[idx]["app_description"] = summary
            updated += 1
            print(f"    [{idx+1}] {rows[idx].get('app_name', '')[:30]}: {summary[:80]}...")

    print(f"  Summarized {updated}/{len(rows)} app descriptions")
    return rows


def get_latest_available_date():
    """Get the latest date with available data (today - 3 days delay)."""
    return datetime.utcnow() - timedelta(days=DATA_DELAY_DAYS)


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
    """Look up app name, icon, publisher, and description from Sensor Tower."""
    time.sleep(0.3)  # Rate limit: 6 req/s

    # Step 1: Get basic info from unified endpoint
    data = st_get(f"/v1/unified/apps/{app_id}", {})
    if not data or not isinstance(data, dict):
        return {"name": "Unknown", "icon_url": "", "publisher": "Unknown", "description": ""}

    name = data.get("name", "")
    if not name:
        sub_apps = data.get("sub_apps", [])
        if sub_apps:
            name = sub_apps[0].get("name", "Unknown")
        else:
            name = "Unknown"

    result = {
        "name": name,
        "icon_url": data.get("icon_url", ""),
        "publisher": data.get("unified_publisher_name", data.get("publisher_name", "Unknown")),
        "description": "",
    }

    # Step 2: Get description from platform-specific endpoint
    # Only use the FIRST iOS or Android sub_app (avoid iterating 100+ regional variants)
    sub_apps = data.get("sub_apps", [])
    if sub_apps:
        # Try iOS first (richer descriptions with subtitle), then Android
        ios_sub = next((sa for sa in sub_apps if sa.get("os") == "ios"), None)
        android_sub = next((sa for sa in sub_apps if sa.get("os") == "android"), None)
        target_sub = ios_sub or android_sub

        if target_sub:
            platform = target_sub.get("os", "ios")
            sub_id = target_sub.get("id", "")
            if sub_id:
                time.sleep(0.3)  # Rate limit
                platform_data = st_get(f"/v1/{platform}/apps/{sub_id}", {})
                if platform_data and isinstance(platform_data, dict):
                    desc_obj = platform_data.get("description", {})
                    if isinstance(desc_obj, dict):
                        # Priority: app_summary > subtitle > short_description > full_description
                        app_summary = (desc_obj.get("app_summary") or "").strip()
                        subtitle = (desc_obj.get("subtitle") or "").strip()
                        short_desc = (desc_obj.get("short_description") or "").strip()
                        full_desc = (desc_obj.get("full_description") or "").strip()

                        if app_summary:
                            result["description"] = app_summary[:500]
                        elif subtitle:
                            result["description"] = subtitle
                        elif short_desc:
                            result["description"] = short_desc[:500]
                        elif full_desc:
                            # Strip HTML tags and truncate
                            clean = re.sub(r'<[^>]+>', ' ', full_desc)
                            clean = re.sub(r'\s+', ' ', clean).strip()
                            result["description"] = clean[:500]
                    elif isinstance(desc_obj, str):
                        result["description"] = desc_obj[:500]

    return result


def aggregate_entities(item):
    """
    Aggregate download/revenue data across all entities (platforms) for a unified app.
    The API returns per-platform data in the 'entities' array.
    We sum across all entities to get the true unified total, then convert
    to daily averages by dividing by 7 (the 7-day window).
    """
    DAYS = 7  # 7-day window

    entities = item.get("entities", [])
    if not entities:
        # No entities array — data is at the top level (non-unified response)
        raw_downloads = item.get("units_absolute", item.get("absolute", 0)) or 0
        raw_prev = item.get("comparison_units_value", 0) or 0
        raw_delta = item.get("units_delta", item.get("delta", 0)) or 0
        return {
            "downloads": round(raw_downloads / DAYS),
            "prev_downloads": round(raw_prev / DAYS),
            "delta": round(raw_delta / DAYS),
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
    # (percentage change is the same whether using totals or averages)
    pct_change = 0
    if total_prev and total_prev > 0:
        pct_change = total_delta / total_prev
    else:
        # Use the first entity's transformed_delta as fallback
        pct_change = entities[0].get("units_transformed_delta", entities[0].get("transformed_delta", 0)) or 0

    # Convert totals to daily averages
    return {
        "downloads": round(total_downloads / DAYS),
        "prev_downloads": round(total_prev / DAYS),
        "delta": round(total_delta / DAYS),
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
    """Delete all existing data from the table, then insert new rows."""
    if not rows:
        print(f"  No rows to insert into {table_name}")
        return

    url = f"{SUPABASE_URL}/rest/v1/{table_name}"

    # Step 1: Delete all existing rows from the table
    # Use a filter that matches all rows (id > 0 matches everything with a positive id)
    delete_url = f"{url}?id=gt.0"
    delete_headers = {**HEADERS, "Prefer": "return=minimal"}
    del_resp = requests.delete(delete_url, headers=delete_headers)
    if del_resp.status_code in (200, 204):
        print(f"  Cleared all existing data from {table_name}")
    else:
        print(f"  Warning: Could not clear {table_name} (status {del_resp.status_code}): {del_resp.text[:200]}")

    # Step 2: Insert new rows
    insert_headers = {**HEADERS, "Prefer": "return=minimal"}
    batch_size = 50
    total_inserted = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        resp = requests.post(url, headers=insert_headers, json=batch)
        if resp.status_code in (200, 201, 204):
            total_inserted += len(batch)
        else:
            print(f"  Insert error {resp.status_code}: {resp.text[:300]}")
            for row in batch:
                resp2 = requests.post(url, headers=insert_headers, json=row)
                if resp2.status_code in (200, 201, 204):
                    total_inserted += 1
                else:
                    print(f"    Row insert error: {resp2.text[:200]}")

    print(f"  Inserted {total_inserted}/{len(rows)} rows into {table_name}")


# ─── Fetch 1: Top 15 Apps by Downloads (last 7 days) ─────────────────────────
def fetch_top_downloads():
    """Fetch top 15 apps by absolute downloads in the last 7 days (stored as daily avg).
    
    Uses time_range=day with date+end_date for exact 7-day windows.
    Current period: (latest_date - 6) to latest_date
    Previous period (auto-computed by API): the 7 days before that
    """
    print("\n=== Fetching Top 15 Apps by Downloads (7-day) ===")

    latest_date = get_latest_available_date()
    end_date_str = latest_date.strftime("%Y-%m-%d")
    period_start = (latest_date - timedelta(days=6)).strftime("%Y-%m-%d")
    prev_end = (latest_date - timedelta(days=7)).strftime("%Y-%m-%d")
    prev_start = (latest_date - timedelta(days=13)).strftime("%Y-%m-%d")
    print(f"  Current period: {period_start} to {end_date_str} (7 days)")
    print(f"  Previous period: {prev_start} to {prev_end} (7 days)")

    data = st_get("/v1/unified/sales_report_estimates_comparison_attributes", {
        "comparison_attribute": "absolute",
        "time_range": "day",
        "measure": "units",
        "category": "0",
        "date": period_start,
        "end_date": end_date_str,
        "device_type": "total",
        "limit": 15,
        "regions": "WW",
    })

    if not data:
        print("  ERROR: No data returned")
        return []

    print(f"  Got {len(data)} apps")

    now = datetime.utcnow()
    rows = []
    for rank, item in enumerate(data[:15], 1):
        unified_id = item.get("app_id", "")

        # Look up app name with robust retry
        app_info = lookup_app(unified_id)

        # Aggregate downloads across all platforms (iOS + Android + lite variants)
        agg = aggregate_entities(item)

        row = {
            "fetch_date": now.strftime("%Y-%m-%d"),
            "period_start": period_start,
            "period_end": end_date_str,
            "prev_period_start": prev_start,
            "prev_period_end": prev_end,
            "rank": rank,
            "app_id": str(unified_id),
            "app_name": app_info["name"],
            "publisher": app_info["publisher"],
            "icon_url": app_info["icon_url"],
            "downloads": agg["downloads"],
            "previous_downloads": agg["prev_downloads"],
            "download_delta": agg["delta"],
            "download_pct_change": round(agg["pct_change"] * 100, 2),
            "app_description": app_info["description"],
        }
        rows.append(row)
        print(f"  #{rank}: {app_info['name']} — {agg['downloads']:,} avg daily downloads (prev avg: {agg['prev_downloads']:,}, daily delta: {agg['delta']:,})")

    return rows


# ─── Fetch 2: Top 15 Apps by Download % Increase (last 7 days) ───────────────
def fetch_top_download_growth():
    """Fetch top 15 apps by download percentage increase in the last 7 days (stored as daily avg).
    
    Uses time_range=day with date+end_date for exact 7-day windows.
    """
    print("\n=== Fetching Top 15 Apps by Download % Increase (7-day) ===")

    latest_date = get_latest_available_date()
    end_date_str = latest_date.strftime("%Y-%m-%d")
    period_start = (latest_date - timedelta(days=6)).strftime("%Y-%m-%d")
    prev_end = (latest_date - timedelta(days=7)).strftime("%Y-%m-%d")
    prev_start = (latest_date - timedelta(days=13)).strftime("%Y-%m-%d")
    print(f"  Current period: {period_start} to {end_date_str} (7 days)")
    print(f"  Previous period: {prev_start} to {prev_end} (7 days)")

    data = st_get("/v1/unified/sales_report_estimates_comparison_attributes", {
        "comparison_attribute": "transformed_delta",
        "time_range": "day",
        "measure": "units",
        "category": "0",
        "date": period_start,
        "end_date": end_date_str,
        "device_type": "total",
        "limit": 15,
        "regions": "WW",
    })

    if not data:
        print("  ERROR: No data returned")
        return []

    print(f"  Got {len(data)} apps")

    now = datetime.utcnow()
    rows = []
    for rank, item in enumerate(data[:15], 1):
        unified_id = item.get("app_id", "")

        app_info = lookup_app(unified_id)
        agg = aggregate_entities(item)

        row = {
            "fetch_date": now.strftime("%Y-%m-%d"),
            "period_start": period_start,
            "period_end": end_date_str,
            "prev_period_start": prev_start,
            "prev_period_end": prev_end,
            "rank": rank,
            "app_id": str(unified_id),
            "app_name": app_info["name"],
            "publisher": app_info["publisher"],
            "icon_url": app_info["icon_url"],
            "downloads": agg["downloads"],
            "previous_downloads": agg["prev_downloads"],
            "download_delta": agg["delta"],
            "download_pct_change": round(agg["pct_change"] * 100, 2),
            "app_description": app_info["description"],
        }
        rows.append(row)
        print(f"  #{rank}: {app_info['name']} — {agg['pct_change']*100:.1f}% increase ({agg['downloads']:,} avg daily downloads)")

    return rows


# ─── Fetch 3: Top 15 Advertisers by Spend ───────────────────────────────────
def fetch_top_advertisers():
    """Fetch top 15 advertisers by ad spend (Share of Voice) in the last 7 days."""
    print("\n=== Fetching Top 15 Advertisers (7-day) ===")

    latest_date = get_latest_available_date()
    date_str = latest_date.strftime("%Y-%m-%d")
    period_start = (latest_date - timedelta(days=6)).strftime("%Y-%m-%d")
    print(f"  Period: {period_start} to {date_str} (7 days)")

    data = st_get("/v1/unified/ad_intel/top_apps", {
        "role": "advertisers",
        "date": date_str,
        "period": "week",
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

    now = datetime.utcnow()
    rows = []
    for rank, app in enumerate(apps[:15], 1):
        # The advertiser endpoint returns app_id but it may be a unified ID or platform ID
        # We need to look up the app to get the description
        app_id = str(app.get("app_id", ""))
        app_name = app.get("name", app.get("humanized_name", "Unknown"))
        publisher = app.get("publisher_name", "Unknown")
        icon_url = app.get("icon_url", "")

        # Look up description via the unified endpoint
        app_info = lookup_app(app_id)
        description = app_info.get("description", "")

        # Use the advertiser endpoint's name/publisher/icon if lookup returns Unknown
        if app_info["name"] == "Unknown":
            app_info["name"] = app_name
        if app_info["publisher"] == "Unknown":
            app_info["publisher"] = publisher
        if not app_info["icon_url"]:
            app_info["icon_url"] = icon_url

        row = {
            "fetch_date": now.strftime("%Y-%m-%d"),
            "period_start": period_start,
            "rank": rank,
            "app_id": app_id,
            "app_name": app_info["name"],
            "publisher": app_info["publisher"],
            "icon_url": app_info["icon_url"],
            "sov": app.get("sov", 0),
            "app_description": description,
        }
        rows.append(row)
        print(f"  #{rank}: {row['app_name']} ({row['publisher']}) — SoV: {row['sov']:.3f}")

    return rows


# ─── Fetch 4: Top 15 Apps by Absolute Download Change (last 7 days) ─────────
def fetch_top_download_delta():
    """Fetch top 15 apps by absolute download change (delta) in the last 7 days (stored as daily avg delta).
    
    Uses time_range=day with date+end_date for exact 7-day windows.
    """
    print("\n=== Fetching Top 15 Apps by Absolute Download Change (7-day) ===")

    latest_date = get_latest_available_date()
    end_date_str = latest_date.strftime("%Y-%m-%d")
    period_start = (latest_date - timedelta(days=6)).strftime("%Y-%m-%d")
    prev_end = (latest_date - timedelta(days=7)).strftime("%Y-%m-%d")
    prev_start = (latest_date - timedelta(days=13)).strftime("%Y-%m-%d")
    print(f"  Current period: {period_start} to {end_date_str} (7 days)")
    print(f"  Previous period: {prev_start} to {prev_end} (7 days)")

    data = st_get("/v1/unified/sales_report_estimates_comparison_attributes", {
        "comparison_attribute": "delta",
        "time_range": "day",
        "measure": "units",
        "category": "0",
        "date": period_start,
        "end_date": end_date_str,
        "device_type": "total",
        "limit": 15,
        "regions": "WW",
    })

    if not data:
        print("  ERROR: No data returned")
        return []

    print(f"  Got {len(data)} apps")

    now = datetime.utcnow()
    rows = []
    for rank, item in enumerate(data[:15], 1):
        unified_id = item.get("app_id", "")

        app_info = lookup_app(unified_id)
        agg = aggregate_entities(item)

        row = {
            "fetch_date": now.strftime("%Y-%m-%d"),
            "period_start": period_start,
            "period_end": end_date_str,
            "prev_period_start": prev_start,
            "prev_period_end": prev_end,
            "rank": rank,
            "app_id": str(unified_id),
            "app_name": app_info["name"],
            "publisher": app_info["publisher"],
            "icon_url": app_info["icon_url"],
            "downloads": agg["downloads"],
            "previous_downloads": agg["prev_downloads"],
            "download_delta": agg["delta"],
            "download_pct_change": round(agg["pct_change"] * 100, 2),
            "app_description": app_info["description"],
        }
        rows.append(row)
        print(f"  #{rank}: {app_info['name']} — daily avg delta: {agg['delta']:+,} ({agg['downloads']:,} avg daily downloads)")

    return rows


# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("Sensor Tower Data Fetcher (7-day window, daily averages)")
    print(f"Run time: {datetime.utcnow().isoformat()}")
    print(f"Data delay: {DATA_DELAY_DAYS} days")
    latest = get_latest_available_date()
    print(f"Latest available date: {latest.strftime('%Y-%m-%d')}")
    print(f"Current 7-day window: {(latest - timedelta(days=6)).strftime('%Y-%m-%d')} to {latest.strftime('%Y-%m-%d')}")
    print(f"Previous 7-day window: {(latest - timedelta(days=13)).strftime('%Y-%m-%d')} to {(latest - timedelta(days=7)).strftime('%Y-%m-%d')}")
    print("=" * 60)

    if not ST_API_KEY:
        print("ERROR: SENSORTOWER_API_KEY not set")
        sys.exit(1)
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set")
        sys.exit(1)

    for table in ["download_rank_7d", "download_percent_rank_7d", "advertiser_rank_7d", "download_delta_rank_7d"]:
        if not ensure_table(table, {}):
            print(f"WARNING: Table '{table}' may not exist. Will attempt inserts anyway.")

    # 1. Top downloads (7-day)
    download_rows = fetch_top_downloads()
    if download_rows:
        download_rows = batch_summarize_descriptions(download_rows)
        upsert_rows("download_rank_7d", download_rows)

    # 2. Top download % increase (7-day)
    growth_rows = fetch_top_download_growth()
    if growth_rows:
        growth_rows = batch_summarize_descriptions(growth_rows)
        upsert_rows("download_percent_rank_7d", growth_rows)

    # 3. Top advertisers (7-day)
    advertiser_rows = fetch_top_advertisers()
    if advertiser_rows:
        advertiser_rows = batch_summarize_descriptions(advertiser_rows)
        upsert_rows("advertiser_rank_7d", advertiser_rows)

    # 4. Top download absolute change (7-day)
    delta_rows = fetch_top_download_delta()
    if delta_rows:
        delta_rows = batch_summarize_descriptions(delta_rows)
        upsert_rows("download_delta_rank_7d", delta_rows)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print(f"  Downloads ranking (7-day): {len(download_rows)} rows")
    print(f"  Download growth ranking (7-day): {len(growth_rows)} rows")
    print(f"  Advertiser ranking (7-day): {len(advertiser_rows)} rows")
    print(f"  Download delta ranking (7-day): {len(delta_rows)} rows")
    print("=" * 60)


if __name__ == "__main__":
    main()
