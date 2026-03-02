#!/usr/bin/env python3
"""
Sensor Tower Data Fetcher (Optimized)
Fetches top apps by downloads (7-day daily avg), download % increase (7-day),
and top advertisers from Sensor Tower API and stores them in Supabase.

All download counts are stored as daily averages (7-day total / 7).
Percentage changes remain the same (WoW % is identical for totals vs averages).

Uses time_range=day with date+end_date for exact 7-day windows, avoiding
the Monday-snapping behavior of time_range=week. For example, if run on
Mar 2 with a 2-day data delay:
  Current period:  Feb 22 – Feb 28 (latest_date - 6 to latest_date)
  Previous period: Feb 15 – Feb 21 (auto-computed by the API)

Note: Sensor Tower data has a ~2-day delay, so we use (today - 2 days)
as the latest available date, and fetch the 7-day window ending on that date.

Performance optimizations:
  - App lookup results are cached across all 4 ranking functions
  - lookup_app() calls are parallelized with ThreadPoolExecutor (5 workers)
  - Rate limiting uses a threading lock instead of fixed sleeps
  - Gemini batch summarizations run in parallel across ranking types
"""

import os
import sys
import json
import time
import re
import threading
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

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

DATA_DELAY_DAYS = 2  # Sensor Tower data is typically 2 days behind

GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"

# ─── Rate limiter for SensorTower API (max ~5 req/s to stay safe) ────────────
_st_rate_lock = threading.Lock()
_st_last_call = 0.0
ST_MIN_INTERVAL = 0.2  # 200ms between ST API calls (5 req/s)

def _rate_limited_wait():
    """Wait if needed to respect SensorTower rate limits (thread-safe)."""
    global _st_last_call
    with _st_rate_lock:
        now = time.monotonic()
        elapsed = now - _st_last_call
        if elapsed < ST_MIN_INTERVAL:
            time.sleep(ST_MIN_INTERVAL - elapsed)
        _st_last_call = time.monotonic()


# ─── App lookup cache ────────────────────────────────────────────────────────
_app_cache = {}
_cache_lock = threading.Lock()


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

    print(f"  Summarized {updated}/{len(rows)} app descriptions")
    return rows


def get_latest_available_date():
    """Get the latest date with available data (today - 2 days delay)."""
    return datetime.utcnow() - timedelta(days=DATA_DELAY_DAYS)


# ─── Helper: Sensor Tower API call with retry ────────────────────────────────
def st_get(path, params):
    """Make a GET request to Sensor Tower API with retry logic."""
    params["auth_token"] = ST_API_KEY
    for attempt in range(5):
        try:
            _rate_limited_wait()
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
    """Look up app name, icon, publisher, and description from Sensor Tower.
    
    Results are cached so repeated lookups for the same app_id are instant.
    """
    app_id_str = str(app_id)

    # Check cache first
    with _cache_lock:
        if app_id_str in _app_cache:
            return _app_cache[app_id_str].copy()

    # Step 1: Get basic info from unified endpoint
    data = st_get(f"/v1/unified/apps/{app_id_str}", {})
    if not data or not isinstance(data, dict):
        result = {"name": "Unknown", "icon_url": "", "publisher": "Unknown", "description": "",
                  "ios_store_url": "", "android_store_url": ""}
        with _cache_lock:
            _app_cache[app_id_str] = result
        return result.copy()

    name = data.get("name", "")
    if not name:
        sub_apps = data.get("sub_apps", [])
        if sub_apps:
            name = sub_apps[0].get("name", "Unknown")
        else:
            name = "Unknown"

    # Build store URLs from sub_apps
    ios_store_url = ""
    android_store_url = ""
    sub_apps = data.get("sub_apps", [])
    for sa in sub_apps:
        sa_os = sa.get("os", "")
        sa_id = sa.get("id", "")
        if sa_os == "ios" and sa_id and not ios_store_url:
            ios_store_url = f"https://apps.apple.com/app/id{sa_id}"
        elif sa_os == "android" and sa_id and not android_store_url:
            android_store_url = f"https://play.google.com/store/apps/details?id={sa_id}"

    result = {
        "name": name,
        "icon_url": data.get("icon_url", ""),
        "publisher": data.get("unified_publisher_name", data.get("publisher_name", "Unknown")),
        "description": "",
        "ios_store_url": ios_store_url,
        "android_store_url": android_store_url,
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

    # Store in cache
    with _cache_lock:
        _app_cache[app_id_str] = result

    return result.copy()


def parallel_lookup_apps(app_ids):
    """Look up multiple apps in parallel using ThreadPoolExecutor.
    
    Uses 5 worker threads to parallelize API calls while respecting rate limits
    via the global rate limiter.
    """
    results = {}

    # Separate cached vs uncached
    uncached_ids = []
    for aid in app_ids:
        aid_str = str(aid)
        with _cache_lock:
            if aid_str in _app_cache:
                results[aid_str] = _app_cache[aid_str].copy()
            else:
                uncached_ids.append(aid_str)

    if uncached_ids:
        cache_hits = len(app_ids) - len(uncached_ids)
        if cache_hits > 0:
            print(f"    Cache hits: {cache_hits}, uncached lookups: {len(uncached_ids)}")

        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_id = {executor.submit(lookup_app, aid): aid for aid in uncached_ids}
            for future in as_completed(future_to_id):
                aid = future_to_id[future]
                try:
                    results[aid] = future.result()
                except Exception as e:
                    print(f"    Lookup error for {aid}: {e}")
                    results[aid] = {"name": "Unknown", "icon_url": "", "publisher": "Unknown",
                                    "description": "", "ios_store_url": "", "android_store_url": ""}

    return results


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


# ─── Fetch functions (with parallel app lookups) ────────────────────────────

def _build_rows_parallel(data, period_start, end_date_str, prev_start, prev_end, row_builder):
    """Common pattern: extract app_ids, parallel lookup, then build rows."""
    now = datetime.utcnow()
    
    # Extract all app IDs first
    app_ids = [str(item.get("app_id", "")) for item in data]
    
    # Parallel lookup all apps
    print(f"  Looking up {len(app_ids)} apps (parallel, cached)...")
    t0 = time.monotonic()
    app_infos = parallel_lookup_apps(app_ids)
    elapsed = time.monotonic() - t0
    print(f"  Lookups completed in {elapsed:.1f}s")
    
    rows = []
    for rank, item in enumerate(data, 1):
        unified_id = str(item.get("app_id", ""))
        app_info = app_infos.get(unified_id, {
            "name": "Unknown", "icon_url": "", "publisher": "Unknown",
            "description": "", "ios_store_url": "", "android_store_url": ""
        })
        agg = aggregate_entities(item)
        
        row = row_builder(rank, unified_id, app_info, agg, now, period_start, end_date_str, prev_start, prev_end)
        rows.append(row)
    
    return rows


def fetch_top_downloads():
    """Fetch top 50 apps by absolute downloads in the last 7 days (stored as daily avg)."""
    print("\n=== Fetching Top 50 Apps by Downloads (7-day) ===")

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
        "limit": 50,
        "regions": "WW",
    })

    if not data:
        print("  ERROR: No data returned")
        return []

    print(f"  Got {len(data)} apps from API")
    data = data[:50]

    def build_row(rank, unified_id, app_info, agg, now, ps, ed, pvs, pve):
        return {
            "fetch_date": now.strftime("%Y-%m-%d"),
            "period_start": ps,
            "period_end": ed,
            "prev_period_start": pvs,
            "prev_period_end": pve,
            "rank": rank,
            "app_id": unified_id,
            "app_name": app_info["name"],
            "publisher": app_info["publisher"],
            "icon_url": app_info["icon_url"],
            "downloads": agg["downloads"],
            "previous_downloads": agg["prev_downloads"],
            "download_delta": agg["delta"],
            "download_pct_change": round(agg["pct_change"] * 100, 2),
            "app_description": app_info["description"],
            "ios_store_url": app_info.get("ios_store_url", ""),
            "android_store_url": app_info.get("android_store_url", ""),
        }

    rows = _build_rows_parallel(data, period_start, end_date_str, prev_start, prev_end, build_row)
    for r in rows:
        print(f"  #{r['rank']}: {r['app_name']} — {r['downloads']:,} avg daily downloads")
    return rows


def fetch_top_download_growth():
    """Fetch top 50 apps by download percentage increase in the last 7 days (stored as daily avg)."""
    print("\n=== Fetching Top 50 Apps by Download % Increase (7-day) ===")

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
        "limit": 50,
        "regions": "WW",
    })

    if not data:
        print("  ERROR: No data returned")
        return []

    print(f"  Got {len(data)} apps from API")
    data = data[:50]

    def build_row(rank, unified_id, app_info, agg, now, ps, ed, pvs, pve):
        return {
            "fetch_date": now.strftime("%Y-%m-%d"),
            "period_start": ps,
            "period_end": ed,
            "prev_period_start": pvs,
            "prev_period_end": pve,
            "rank": rank,
            "app_id": unified_id,
            "app_name": app_info["name"],
            "publisher": app_info["publisher"],
            "icon_url": app_info["icon_url"],
            "downloads": agg["downloads"],
            "previous_downloads": agg["prev_downloads"],
            "download_delta": agg["delta"],
            "download_pct_change": round(agg["pct_change"] * 100, 2),
            "app_description": app_info["description"],
            "ios_store_url": app_info.get("ios_store_url", ""),
            "android_store_url": app_info.get("android_store_url", ""),
        }

    rows = _build_rows_parallel(data, period_start, end_date_str, prev_start, prev_end, build_row)
    for r in rows:
        print(f"  #{r['rank']}: {r['app_name']} — {r['download_pct_change']:.1f}% increase")
    return rows


def fetch_top_advertisers():
    """Fetch top 50 advertisers by ad spend (Share of Voice) in the last 7 days."""
    print("\n=== Fetching Top 50 Advertisers (7-day) ===")

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
        "limit": 50,
    })

    if not data:
        print("  ERROR: No data returned")
        return []

    apps = data.get("apps", [])
    print(f"  Got {len(apps)} advertisers from API")
    apps = apps[:50]

    # Extract app IDs and do parallel lookup
    app_ids = [str(app.get("app_id", "")) for app in apps]
    print(f"  Looking up {len(app_ids)} apps (parallel, cached)...")
    t0 = time.monotonic()
    app_infos = parallel_lookup_apps(app_ids)
    elapsed = time.monotonic() - t0
    print(f"  Lookups completed in {elapsed:.1f}s")

    now = datetime.utcnow()
    rows = []
    for rank, app in enumerate(apps, 1):
        app_id = str(app.get("app_id", ""))
        app_name = app.get("name", app.get("humanized_name", "Unknown"))
        publisher = app.get("publisher_name", "Unknown")
        icon_url = app.get("icon_url", "")

        app_info = app_infos.get(app_id, {
            "name": "Unknown", "icon_url": "", "publisher": "Unknown",
            "description": "", "ios_store_url": "", "android_store_url": ""
        })
        description = app_info.get("description", "")

        # Use the advertiser endpoint's name/publisher/icon if lookup returns Unknown
        if app_info.get("name") == "Unknown":
            app_info["name"] = app_name
        if app_info.get("publisher") == "Unknown":
            app_info["publisher"] = publisher
        if not app_info.get("icon_url"):
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
            "ios_store_url": app_info.get("ios_store_url", ""),
            "android_store_url": app_info.get("android_store_url", ""),
        }
        rows.append(row)
        print(f"  #{rank}: {row['app_name']} ({row['publisher']}) — SoV: {row['sov']:.3f}")

    return rows


def fetch_top_download_delta():
    """Fetch top 50 apps by absolute download change (delta) in the last 7 days (stored as daily avg delta)."""
    print("\n=== Fetching Top 50 Apps by Absolute Download Change (7-day) ===")

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
        "limit": 50,
        "regions": "WW",
    })

    if not data:
        print("  ERROR: No data returned")
        return []

    print(f"  Got {len(data)} apps from API")
    data = data[:50]

    def build_row(rank, unified_id, app_info, agg, now, ps, ed, pvs, pve):
        return {
            "fetch_date": now.strftime("%Y-%m-%d"),
            "period_start": ps,
            "period_end": ed,
            "prev_period_start": pvs,
            "prev_period_end": pve,
            "rank": rank,
            "app_id": unified_id,
            "app_name": app_info["name"],
            "publisher": app_info["publisher"],
            "icon_url": app_info["icon_url"],
            "downloads": agg["downloads"],
            "previous_downloads": agg["prev_downloads"],
            "download_delta": agg["delta"],
            "download_pct_change": round(agg["pct_change"] * 100, 2),
            "app_description": app_info["description"],
            "ios_store_url": app_info.get("ios_store_url", ""),
            "android_store_url": app_info.get("android_store_url", ""),
        }

    rows = _build_rows_parallel(data, period_start, end_date_str, prev_start, prev_end, build_row)
    for r in rows:
        print(f"  #{r['rank']}: {r['app_name']} — daily avg delta: {r['download_delta']:+,}")
    return rows


# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("Sensor Tower Data Fetcher (Optimized — parallel lookups + caching)")
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

    overall_start = time.monotonic()

    for table in ["download_rank_7d", "download_percent_rank_7d", "advertiser_rank_7d", "download_delta_rank_7d"]:
        if not ensure_table(table, {}):
            print(f"WARNING: Table '{table}' may not exist. Will attempt inserts anyway.")

    # ─── Phase 1: Fetch all 4 ranking lists from SensorTower API ─────────
    # These 4 API calls are sequential (each is a single request), but fast (~2s each)
    print("\n--- Phase 1: Fetching ranking data from SensorTower API ---")
    t0 = time.monotonic()

    dl_api_data = st_get("/v1/unified/sales_report_estimates_comparison_attributes", {
        "comparison_attribute": "absolute", "time_range": "day", "measure": "units",
        "category": "0", "date": (latest - timedelta(days=6)).strftime("%Y-%m-%d"),
        "end_date": latest.strftime("%Y-%m-%d"), "device_type": "total", "limit": 50, "regions": "WW",
    })
    growth_api_data = st_get("/v1/unified/sales_report_estimates_comparison_attributes", {
        "comparison_attribute": "transformed_delta", "time_range": "day", "measure": "units",
        "category": "0", "date": (latest - timedelta(days=6)).strftime("%Y-%m-%d"),
        "end_date": latest.strftime("%Y-%m-%d"), "device_type": "total", "limit": 50, "regions": "WW",
    })
    delta_api_data = st_get("/v1/unified/sales_report_estimates_comparison_attributes", {
        "comparison_attribute": "delta", "time_range": "day", "measure": "units",
        "category": "0", "date": (latest - timedelta(days=6)).strftime("%Y-%m-%d"),
        "end_date": latest.strftime("%Y-%m-%d"), "device_type": "total", "limit": 50, "regions": "WW",
    })
    adv_api_data = st_get("/v1/unified/ad_intel/top_apps", {
        "role": "advertisers", "date": latest.strftime("%Y-%m-%d"),
        "period": "week", "category": "0", "country": "US", "network": "All Networks", "limit": 50,
    })

    print(f"  Phase 1 completed in {time.monotonic() - t0:.1f}s")

    # ─── Phase 2: Collect ALL unique app IDs and do ONE parallel lookup pass ──
    print("\n--- Phase 2: Parallel app lookups (deduplicated across all rankings) ---")
    t0 = time.monotonic()

    all_app_ids = set()
    if dl_api_data:
        for item in (dl_api_data[:50]):
            all_app_ids.add(str(item.get("app_id", "")))
    if growth_api_data:
        for item in (growth_api_data[:50]):
            all_app_ids.add(str(item.get("app_id", "")))
    if delta_api_data:
        for item in (delta_api_data[:50]):
            all_app_ids.add(str(item.get("app_id", "")))
    if adv_api_data:
        for app in (adv_api_data.get("apps", [])[:50]):
            all_app_ids.add(str(app.get("app_id", "")))

    all_app_ids.discard("")
    print(f"  Total unique app IDs across all rankings: {len(all_app_ids)}")
    print(f"  (vs {50*4}=200 if done without dedup)")

    app_infos = parallel_lookup_apps(list(all_app_ids))
    print(f"  Phase 2 completed in {time.monotonic() - t0:.1f}s — {len(app_infos)} apps looked up")

    # ─── Phase 3: Build rows for each ranking type ───────────────────────
    print("\n--- Phase 3: Building rows ---")

    end_date_str = latest.strftime("%Y-%m-%d")
    period_start = (latest - timedelta(days=6)).strftime("%Y-%m-%d")
    prev_end = (latest - timedelta(days=7)).strftime("%Y-%m-%d")
    prev_start = (latest - timedelta(days=13)).strftime("%Y-%m-%d")
    now = datetime.utcnow()

    def _default_info():
        return {"name": "Unknown", "icon_url": "", "publisher": "Unknown",
                "description": "", "ios_store_url": "", "android_store_url": ""}

    # Downloads
    download_rows = []
    if dl_api_data:
        for rank, item in enumerate(dl_api_data[:50], 1):
            uid = str(item.get("app_id", ""))
            info = app_infos.get(uid, _default_info())
            agg = aggregate_entities(item)
            download_rows.append({
                "fetch_date": now.strftime("%Y-%m-%d"), "period_start": period_start,
                "period_end": end_date_str, "prev_period_start": prev_start, "prev_period_end": prev_end,
                "rank": rank, "app_id": uid, "app_name": info["name"], "publisher": info["publisher"],
                "icon_url": info["icon_url"], "downloads": agg["downloads"],
                "previous_downloads": agg["prev_downloads"], "download_delta": agg["delta"],
                "download_pct_change": round(agg["pct_change"] * 100, 2),
                "app_description": info["description"],
                "ios_store_url": info.get("ios_store_url", ""),
                "android_store_url": info.get("android_store_url", ""),
            })
        print(f"  Downloads: {len(download_rows)} rows")

    # Growth %
    growth_rows = []
    if growth_api_data:
        for rank, item in enumerate(growth_api_data[:50], 1):
            uid = str(item.get("app_id", ""))
            info = app_infos.get(uid, _default_info())
            agg = aggregate_entities(item)
            growth_rows.append({
                "fetch_date": now.strftime("%Y-%m-%d"), "period_start": period_start,
                "period_end": end_date_str, "prev_period_start": prev_start, "prev_period_end": prev_end,
                "rank": rank, "app_id": uid, "app_name": info["name"], "publisher": info["publisher"],
                "icon_url": info["icon_url"], "downloads": agg["downloads"],
                "previous_downloads": agg["prev_downloads"], "download_delta": agg["delta"],
                "download_pct_change": round(agg["pct_change"] * 100, 2),
                "app_description": info["description"],
                "ios_store_url": info.get("ios_store_url", ""),
                "android_store_url": info.get("android_store_url", ""),
            })
        print(f"  Growth: {len(growth_rows)} rows")

    # Delta
    delta_rows = []
    if delta_api_data:
        for rank, item in enumerate(delta_api_data[:50], 1):
            uid = str(item.get("app_id", ""))
            info = app_infos.get(uid, _default_info())
            agg = aggregate_entities(item)
            delta_rows.append({
                "fetch_date": now.strftime("%Y-%m-%d"), "period_start": period_start,
                "period_end": end_date_str, "prev_period_start": prev_start, "prev_period_end": prev_end,
                "rank": rank, "app_id": uid, "app_name": info["name"], "publisher": info["publisher"],
                "icon_url": info["icon_url"], "downloads": agg["downloads"],
                "previous_downloads": agg["prev_downloads"], "download_delta": agg["delta"],
                "download_pct_change": round(agg["pct_change"] * 100, 2),
                "app_description": info["description"],
                "ios_store_url": info.get("ios_store_url", ""),
                "android_store_url": info.get("android_store_url", ""),
            })
        print(f"  Delta: {len(delta_rows)} rows")

    # Advertisers
    advertiser_rows = []
    if adv_api_data:
        adv_apps = adv_api_data.get("apps", [])[:50]
        for rank, app in enumerate(adv_apps, 1):
            app_id = str(app.get("app_id", ""))
            info = app_infos.get(app_id, _default_info())
            # Fallback to advertiser endpoint data
            if info.get("name") == "Unknown":
                info["name"] = app.get("name", app.get("humanized_name", "Unknown"))
            if info.get("publisher") == "Unknown":
                info["publisher"] = app.get("publisher_name", "Unknown")
            if not info.get("icon_url"):
                info["icon_url"] = app.get("icon_url", "")
            advertiser_rows.append({
                "fetch_date": now.strftime("%Y-%m-%d"), "period_start": period_start,
                "rank": rank, "app_id": app_id, "app_name": info["name"],
                "publisher": info["publisher"], "icon_url": info["icon_url"],
                "sov": app.get("sov", 0), "app_description": info.get("description", ""),
                "ios_store_url": info.get("ios_store_url", ""),
                "android_store_url": info.get("android_store_url", ""),
            })
        print(f"  Advertisers: {len(advertiser_rows)} rows")

    # ─── Phase 4: Batch summarize descriptions (parallel across 4 lists) ──
    print("\n--- Phase 4: Batch summarizing descriptions (parallel) ---")
    t0 = time.monotonic()

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {}
        if download_rows:
            futures["downloads"] = executor.submit(batch_summarize_descriptions, download_rows)
        if growth_rows:
            futures["growth"] = executor.submit(batch_summarize_descriptions, growth_rows)
        if delta_rows:
            futures["delta"] = executor.submit(batch_summarize_descriptions, delta_rows)
        if advertiser_rows:
            futures["advertisers"] = executor.submit(batch_summarize_descriptions, advertiser_rows)

        for name, future in futures.items():
            try:
                result = future.result(timeout=120)
                if name == "downloads":
                    download_rows = result
                elif name == "growth":
                    growth_rows = result
                elif name == "delta":
                    delta_rows = result
                elif name == "advertisers":
                    advertiser_rows = result
            except Exception as e:
                print(f"  WARNING: Summarization failed for {name}: {e}")

    print(f"  Phase 4 completed in {time.monotonic() - t0:.1f}s")

    # ─── Phase 5: Upsert to Supabase ────────────────────────────────────
    print("\n--- Phase 5: Upserting to Supabase ---")

    if download_rows:
        upsert_rows("download_rank_7d", download_rows)
    if growth_rows:
        upsert_rows("download_percent_rank_7d", growth_rows)
    if advertiser_rows:
        upsert_rows("advertiser_rank_7d", advertiser_rows)
    if delta_rows:
        upsert_rows("download_delta_rank_7d", delta_rows)

    total_time = time.monotonic() - overall_start
    print("\n" + "=" * 60)
    print("SUMMARY")
    print(f"  Downloads ranking (7-day): {len(download_rows)} rows")
    print(f"  Download growth ranking (7-day): {len(growth_rows)} rows")
    print(f"  Advertiser ranking (7-day): {len(advertiser_rows)} rows")
    print(f"  Download delta ranking (7-day): {len(delta_rows)} rows")
    print(f"  Unique apps looked up: {len(all_app_ids)}")
    print(f"  Cache size: {len(_app_cache)} entries")
    print(f"  Total execution time: {total_time:.1f}s ({total_time/60:.1f} min)")
    print("=" * 60)


if __name__ == "__main__":
    main()
