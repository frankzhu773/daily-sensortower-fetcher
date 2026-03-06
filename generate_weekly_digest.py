"""
Weekly Digest Generator

Standalone script that:
1. Reads the past week's data from Supabase (rankings, news)
2. Uses Gemini to write a polished HTML digest with news categorized by product area
3. Stores the digest in a Supabase table (weekly_digests)
4. Generates a static RSS XML file for GitHub Pages deployment

Designed to run as a GitHub Actions cron job every Friday.
No dependency on the Manus workspace or Tech News Daily website.
"""

import os
import sys
import json
import html
import hashlib
import requests
from datetime import datetime, timezone, timedelta
from xml.etree.ElementTree import Element, SubElement, tostring, indent

# ─── Config ──────────────────────────────────────────────────────────────────

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

PAGES_BASE_URL = "https://frankzhu773.github.io/daily-sensortower-fetcher"


# ─── Date helpers ────────────────────────────────────────────────────────────

def get_week_range(reference_date=None):
    """Return (start_str, end_str) for the last 7 days ending on reference_date."""
    if reference_date is None:
        reference_date = datetime.now(timezone.utc)

    end_date = reference_date
    start_date = end_date - timedelta(days=6)

    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def format_week_range(week_start, week_end):
    """Format a date range for display, e.g. 'Mar 1-7, 2026'."""
    start = datetime.strptime(week_start, "%Y-%m-%d")
    end = datetime.strptime(week_end, "%Y-%m-%d")

    start_month = start.strftime("%b")
    end_month = end.strftime("%b")
    year = end.year

    if start_month == end_month:
        return f"{start_month} {start.day}-{end.day}, {year}"
    return f"{start_month} {start.day} - {end_month} {end.day}, {year}"


# ─── Supabase data fetching ─────────────────────────────────────────────────

def supabase_get(table, params):
    """Fetch data from a Supabase table."""
    url = f"{SUPABASE_URL}/rest/v1/{table}?{params}"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    if not resp.ok:
        print(f"  WARNING: Failed to fetch {table}: {resp.status_code} {resp.text[:200]}")
        return []
    return resp.json()


def fetch_weekly_data(week_start, week_end):
    """Fetch all data sources for the weekly digest."""
    print("  Fetching data from Supabase...")

    top_downloads = supabase_get(
        "download_rank_7d",
        "select=rank,app_name,publisher,downloads,download_pct_change"
        "&order=fetch_date.desc,rank.asc&limit=10"
    )
    print(f"    Top downloads: {len(top_downloads)} apps")

    biggest_movers = supabase_get(
        "download_delta_rank_7d",
        "select=rank,app_name,publisher,download_delta,download_pct_change"
        "&order=fetch_date.desc,rank.asc&limit=10"
    )
    print(f"    Biggest movers: {len(biggest_movers)} apps")

    top_advertisers = supabase_get(
        "advertiser_rank_7d",
        "select=rank,app_name,publisher,sov"
        "&order=fetch_date.desc,rank.asc&limit=10"
    )
    print(f"    Top advertisers: {len(top_advertisers)} apps")

    # Fetch ALL news from the last 7 days (no limit)
    news = supabase_get(
        "news_raw",
        f"select=title,source,category,url,date_of_news,news_content"
        f"&date_of_news=gte.{week_start}&date_of_news=lte.{week_end}"
        f"&order=datetime_of_news.desc&limit=500"
    )
    print(f"    News articles: {len(news)} articles")

    return {
        "top_downloads": top_downloads,
        "biggest_movers": biggest_movers,
        "top_advertisers": top_advertisers,
        "news": news,
    }


# ─── LLM digest generation ──────────────────────────────────────────────────

def format_num(n):
    """Format large numbers for readability."""
    n = float(n)
    if abs(n) >= 1e9:
        return f"{n / 1e9:.1f}B"
    if abs(n) >= 1e6:
        return f"{n / 1e6:.1f}M"
    if abs(n) >= 1e3:
        return f"{n / 1e3:.1f}K"
    return f"{n:,.0f}"


def build_prompt(data, week_start, week_end):
    """Build the LLM prompt from the fetched data."""
    range_label = format_week_range(week_start, week_end)

    lines = [
        f'You are the editor of "Tech News Daily Weekly Digest". Write a polished, engaging weekly digest for the week of {range_label}.',
        "",
        "The digest should be written in HTML format (no <html>, <head>, or <body> tags — just the inner content). "
        "Use semantic HTML: <h2> for section headers, <h3> for subsections, <p> for paragraphs, <ul>/<li> for lists, <strong> for emphasis, and <a> for links.",
        "",
        "The tone should be professional but accessible — like a well-written tech newsletter (think The Verge or Platformer). "
        "Use specific numbers and app names. Keep it concise but insightful.",
        "",
        "Structure the digest with these sections:",
        "1. **Opening paragraph** — A brief 2-3 sentence overview of the week's biggest themes.",
        "2. **Top App Downloads** — Highlight the top apps by download volume and any notable changes.",
        "3. **Biggest Movers** — Apps with the largest download surges or declines. Call out interesting stories.",
        "4. **News Highlights** — This is the main section. Categorize ALL the news articles below by PRODUCT AREA by analysing the summary of the news"
        "(e.g., 'AI Creative Tools', 'AI Productivity Tool', 'AI Health App', "
        "'E-Commerce & Retail', 'Fintech & Payments', 'Health & Biotech', 'Gaming & Entertainment', "
        "'Developer Tools', 'Enterprise & SaaS', 'Hardware & Devices', 'Startups & Funding', etc.). "
        "Use <h3> for each product area subsection. Pick the most relevant and interesting articles for each area. "
        "Include links to the original articles. Do NOT use the original categories (New Product, New Feature, New VC Investment) — "
        "group purely by product/technology area.",
        "5. **Closing** — A brief forward-looking sentence or two.",
        "",
        "--- DATA ---",
        "",
    ]

    # Top Downloads
    lines.append("TOP DOWNLOADS (7-day avg daily):")
    for d in data["top_downloads"]:
        pct = d.get("download_pct_change", 0)
        sign = "+" if pct >= 0 else ""
        lines.append(
            f"  #{d['rank']} {d['app_name']} ({d['publisher']}) — "
            f"{format_num(d['downloads'])} avg/day, {sign}{pct:.1f}% WoW"
        )
    lines.append("")

    # Biggest Movers
    lines.append("BIGGEST MOVERS (by absolute download change):")
    for d in data["biggest_movers"]:
        delta = d.get("download_delta", 0)
        pct = d.get("download_pct_change", 0)
        d_sign = "+" if delta >= 0 else ""
        p_sign = "+" if pct >= 0 else ""
        lines.append(
            f"  #{d['rank']} {d['app_name']} ({d['publisher']}) — "
            f"{d_sign}{format_num(delta)} avg delta/day, {p_sign}{pct:.1f}% WoW"
        )
    lines.append("")

    # Top Advertisers
    lines.append("TOP ADVERTISERS (by share of voice):")
    for d in data["top_advertisers"]:
        lines.append(
            f"  #{d['rank']} {d['app_name']} ({d['publisher']}) — {d['sov']:.3f}% SOV"
        )
    lines.append("")

    # News — provide ALL articles with their original category for context
    news_count = len(data["news"])
    lines.append(f"ALL NEWS ARTICLES FROM THIS WEEK ({news_count} total):")
    lines.append("(Original category shown in brackets for context — but you must re-categorize by product area)")
    lines.append("")
    for n in data["news"]:
        title = n.get("title", "")
        source = n.get("source", "")
        cat = n.get("category", "")
        url = n.get("url", "")
        lines.append(f"  - [{cat}] \"{title}\" ({source}) {url}")

    lines.append("")
    lines.append("--- END DATA ---")
    lines.append("")
    lines.append(
        "Now write the HTML digest. Include links to original articles where available. "
        "For the News Highlights section, group articles by product/technology area (NOT by the original categories). "
        "Pick the most notable articles — you don't need to include every single one, but cover all major areas. "
        "Do NOT include a title heading (it will be added separately). Start directly with the opening paragraph."
    )

    return "\n".join(lines)


def generate_digest_via_gemini(prompt):
    """Call Gemini API to generate the digest HTML content."""
    print("  Calling Gemini API for digest generation...")

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.0-flash:generateContent?key={GEMINI_API_KEY}"

    payload = {
        "contents": [
            {
                "parts": [
                    {
                        "text": (
                            "You are a professional tech newsletter editor. "
                            "Write clean, semantic HTML content for a weekly digest. "
                            "Do not wrap in code blocks. Output raw HTML only.\n\n"
                            + prompt
                        )
                    }
                ]
            }
        ],
        "generationConfig": {
            "maxOutputTokens": 8192,
            "temperature": 0.7,
        },
    }

    resp = requests.post(url, json=payload, timeout=120)
    resp.raise_for_status()
    result = resp.json()

    content_html = ""
    try:
        content_html = result["candidates"][0]["content"]["parts"][0]["text"].strip()
    except (KeyError, IndexError):
        print("  ERROR: Unexpected Gemini response structure")
        print(f"  Response: {json.dumps(result)[:500]}")
        sys.exit(1)

    # Strip markdown code fences if Gemini wrapped it
    if content_html.startswith("```html"):
        content_html = content_html[7:]
    if content_html.startswith("```"):
        content_html = content_html[3:]
    if content_html.endswith("```"):
        content_html = content_html[:-3]
    content_html = content_html.strip()

    # Generate plain-text excerpt
    import re
    plain_text = re.sub(r"<[^>]*>", " ", content_html)
    plain_text = re.sub(r"\s+", " ", plain_text).strip()
    excerpt = plain_text[:297] + "..." if len(plain_text) > 300 else plain_text

    print(f"  Generated {len(content_html)} chars of HTML content")
    return content_html, excerpt


# ─── Supabase storage ────────────────────────────────────────────────────────

def check_existing_digest(week_start):
    """Check if a digest already exists for this week."""
    url = (
        f"{SUPABASE_URL}/rest/v1/weekly_digests"
        f"?week_start=eq.{week_start}&limit=1"
    )
    resp = requests.get(url, headers=HEADERS, timeout=30)
    if not resp.ok:
        return None
    data = resp.json()
    return data[0] if data else None


def save_digest_to_supabase(digest_data):
    """Insert or update the digest in Supabase."""
    url = f"{SUPABASE_URL}/rest/v1/weekly_digests"

    # Use upsert with on_conflict on week_start
    upsert_headers = {
        **HEADERS,
        "Prefer": "resolution=merge-duplicates",
    }

    resp = requests.post(url, headers=upsert_headers, json=digest_data, timeout=30)
    if not resp.ok:
        print(f"  WARNING: Failed to save digest to Supabase: {resp.status_code} {resp.text[:200]}")
        return False

    print("  Digest saved to Supabase (weekly_digests table)")
    return True


def fetch_all_digests():
    """Fetch all digests from Supabase for RSS generation."""
    url = (
        f"{SUPABASE_URL}/rest/v1/weekly_digests"
        f"?select=*&order=published_at.desc&limit=52"
    )
    resp = requests.get(url, headers=HEADERS, timeout=30)
    if not resp.ok:
        print(f"  WARNING: Failed to fetch digests: {resp.status_code}")
        return []
    return resp.json()


# ─── RSS XML generation ──────────────────────────────────────────────────────

def generate_digest_rss(digests, output_path="public/weekly-digest.xml"):
    """Generate RSS 2.0 XML feed from weekly digests."""
    print(f"  Generating weekly digest RSS with {len(digests)} entries...")

    now = datetime.now(timezone.utc)
    pub_date = now.strftime("%a, %d %b %Y %H:%M:%S +0000")

    rss = Element("rss", version="2.0")
    rss.set("xmlns:atom", "http://www.w3.org/2005/Atom")

    channel = SubElement(rss, "channel")

    SubElement(channel, "title").text = "Tech News Daily — Weekly Digest"
    SubElement(channel, "link").text = PAGES_BASE_URL
    SubElement(channel, "description").text = (
        "A weekly summary of the top app movers and interesting "
        "tech news from Tech News Daily. Published every Friday."
    )
    SubElement(channel, "language").text = "en-us"

    if digests:
        last_pub = digests[0].get("published_at", pub_date)
        try:
            dt = datetime.fromisoformat(last_pub.replace("Z", "+00:00"))
            SubElement(channel, "lastBuildDate").text = dt.strftime(
                "%a, %d %b %Y %H:%M:%S +0000"
            )
        except (ValueError, AttributeError):
            SubElement(channel, "lastBuildDate").text = pub_date
    else:
        SubElement(channel, "lastBuildDate").text = pub_date

    SubElement(channel, "ttl").text = "1440"

    atom_link = SubElement(channel, "atom:link")
    atom_link.set("href", f"{PAGES_BASE_URL}/weekly-digest.xml")
    atom_link.set("rel", "self")
    atom_link.set("type", "application/rss+xml")

    for d in digests:
        item = SubElement(channel, "item")

        title = d.get("title", "Weekly Digest")
        content_html = d.get("content_html", "")
        week_start = d.get("week_start", "")
        digest_id = d.get("id", hashlib.md5(week_start.encode()).hexdigest()[:8])

        SubElement(item, "title").text = title
        SubElement(item, "link").text = PAGES_BASE_URL

        guid = SubElement(item, "guid", isPermaLink="false")
        guid.text = f"{PAGES_BASE_URL}/digest/{digest_id}"

        # pubDate
        published_at = d.get("published_at", "")
        if published_at:
            try:
                dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                SubElement(item, "pubDate").text = dt.strftime(
                    "%a, %d %b %Y %H:%M:%S +0000"
                )
            except (ValueError, AttributeError):
                SubElement(item, "pubDate").text = pub_date
        else:
            SubElement(item, "pubDate").text = pub_date

        # Full HTML content in description via CDATA
        SubElement(item, "description").text = content_html

        SubElement(item, "category").text = "Tech News"
        SubElement(item, "category").text = "Weekly Digest"

    indent(rss, space="  ")
    xml_bytes = tostring(rss, encoding="unicode", xml_declaration=False)
    xml_content = '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_bytes

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(xml_content)

    print(f"  RSS feed written to {output_path}")
    return output_path


def update_index_html():
    """Update the GitHub Pages index to include the weekly digest feed link."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Tech News Daily — RSS Feeds</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 640px;
            margin: 80px auto;
            padding: 0 20px;
            color: #1a1a1a;
            line-height: 1.6;
        }}
        h1 {{ font-size: 1.5rem; margin-bottom: 0.5rem; }}
        h2 {{ font-size: 1.2rem; margin-top: 2rem; color: #333; }}
        .subtitle {{ color: #666; margin-bottom: 2rem; }}
        a {{ color: #0d9488; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        .feed-link {{
            display: inline-block;
            background: #0d9488;
            color: white;
            padding: 10px 24px;
            border-radius: 8px;
            font-weight: 600;
            margin-top: 0.5rem;
            margin-right: 0.5rem;
        }}
        .feed-link:hover {{ background: #0f766e; text-decoration: none; }}
        .feed-link.secondary {{
            background: #e56228;
        }}
        .feed-link.secondary:hover {{ background: #d04f1a; }}
        code {{
            background: #f4f4f4;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 0.9em;
        }}
        .updated {{ color: #999; font-size: 0.85rem; margin-top: 2rem; }}
        .feed-section {{
            border: 1px solid #e5e7eb;
            border-radius: 12px;
            padding: 1.5rem;
            margin-top: 1.5rem;
        }}
    </style>
</head>
<body>
    <h1>Tech News Daily — RSS Feeds</h1>
    <p class="subtitle">Subscribe to our feeds for the latest in tech.</p>

    <div class="feed-section">
        <h2>Weekly Digest</h2>
        <p>A curated weekly summary of top app movers and key tech news organized by product area. Published every Friday.</p>
        <a class="feed-link" href="weekly-digest.xml">Subscribe to Weekly Digest</a>
        <p style="margin-top: 1rem; font-size: 0.9em; color: #666;">
            Feed URL: <code>{PAGES_BASE_URL}/weekly-digest.xml</code>
        </p>
    </div>

    <div class="feed-section">
        <h2>Product Hunt — Top Products Today</h2>
        <p>Daily top products from Product Hunt, updated automatically.</p>
        <a class="feed-link secondary" href="feed.xml">Subscribe to Product Hunt Feed</a>
        <p style="margin-top: 1rem; font-size: 0.9em; color: #666;">
            Feed URL: <code>{PAGES_BASE_URL}/feed.xml</code>
        </p>
    </div>

    <p class="updated">Last updated: {now}</p>
</body>
</html>"""

    os.makedirs("public", exist_ok=True)
    with open("public/index.html", "w", encoding="utf-8") as f:
        f.write(html_content)

    print("  Index page updated with weekly digest link")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Weekly Digest Generator")
    print("=" * 60)

    # Validate env vars
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
        sys.exit(1)
    if not GEMINI_API_KEY:
        print("ERROR: Missing GEMINI_API_KEY")
        sys.exit(1)

    # Determine week range (last 7 days)
    week_start, week_end = get_week_range()
    range_label = format_week_range(week_start, week_end)
    print(f"\n  Week: {range_label} ({week_start} to {week_end})")

    # Check if digest already exists
    existing = check_existing_digest(week_start)
    if existing:
        print(f"  Digest already exists for week {week_start} (id={existing.get('id')})")
        print("  Skipping generation, will regenerate RSS from all digests...")
    else:
        # Fetch data
        data = fetch_weekly_data(week_start, week_end)

        total_items = sum(len(v) for v in data.values())
        if total_items == 0:
            print("  WARNING: No data found for this week. Skipping digest generation.")
        else:
            # Generate digest via Gemini
            prompt = build_prompt(data, week_start, week_end)
            content_html, excerpt = generate_digest_via_gemini(prompt)

            title = f"Tech News Weekly — {range_label}"

            # Save to Supabase
            digest_data = {
                "title": title,
                "content_html": content_html,
                "excerpt": excerpt,
                "week_start": week_start,
                "week_end": week_end,
                "published_at": datetime.now(timezone.utc).isoformat(),
            }
            save_digest_to_supabase(digest_data)

    # Fetch all digests and generate RSS
    print("\n  Generating RSS feed from all digests...")
    all_digests = fetch_all_digests()
    print(f"  Found {len(all_digests)} total digests")

    generate_digest_rss(all_digests, "public/weekly-digest.xml")
    update_index_html()

    print("\n" + "=" * 60)
    print("Weekly digest generation complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
