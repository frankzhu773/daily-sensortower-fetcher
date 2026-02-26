"""
Fetch top 15 Product Hunt products of the day and store in Supabase.
Overwrites all previous data on each run.
"""

import os
import sys
import requests
import json
from datetime import datetime, timezone

# ── Configuration ──────────────────────────────────────────────
PH_API_KEY = os.environ.get("PH_API_KEY", "")
PH_API_SECRET = os.environ.get("PH_API_SECRET", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

TABLE_NAME = "product_hunt_top_product"


def get_ph_token():
    """Get OAuth token from Product Hunt API."""
    print("Authenticating with Product Hunt API...")
    resp = requests.post(
        "https://api.producthunt.com/v2/oauth/token",
        json={
            "client_id": PH_API_KEY,
            "client_secret": PH_API_SECRET,
            "grant_type": "client_credentials",
        },
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]
    print("  ✓ Token obtained")
    return token


def fetch_top_products(token, count=15):
    """Fetch top products sorted by ranking from Product Hunt API."""
    print(f"Fetching top {count} products...")

    query = """
    {
      posts(order: RANKING, first: %d) {
        edges {
          node {
            id
            name
            tagline
            description
            slug
            url
            website
            votesCount
            commentsCount
            createdAt
            featuredAt
            thumbnail {
              url
            }
            topics {
              edges {
                node {
                  name
                }
              }
            }
          }
        }
      }
    }
    """ % count

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    resp = requests.post(
        "https://api.producthunt.com/v2/api/graphql",
        json={"query": query},
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    if "errors" in data:
        print(f"  ✗ GraphQL errors: {json.dumps(data['errors'])}")
        sys.exit(1)

    posts = data["data"]["posts"]["edges"]
    print(f"  ✓ Fetched {len(posts)} products")

    results = []
    for i, edge in enumerate(posts, 1):
        p = edge["node"]
        topics = ", ".join(t["node"]["name"] for t in p["topics"]["edges"])
        thumb = p["thumbnail"]["url"] if p.get("thumbnail") else None

        # Product Hunt detail page URL
        slug = p.get("slug", "")
        ph_url = f"https://www.producthunt.com/posts/{slug}" if slug else p["url"]
        # External website URL
        website_url = p.get("website") or p["url"]

        results.append({
            "rank": i,
            "name": p["name"],
            "tagline": p["tagline"],
            "description": p.get("description", ""),
            "url": ph_url,
            "website_url": website_url,
            "thumbnail_url": thumb,
            "votes_count": p["votesCount"],
            "comments_count": p["commentsCount"],
            "topics": topics,
            "featured_at": p.get("featuredAt"),
            "fetch_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        })
        print(f"  {i}. {p['name']} — {p['votesCount']} votes, {p['commentsCount']} comments")

    return results


def delete_all_rows():
    """Delete all existing rows from the table."""
    print(f"Deleting all existing rows from {TABLE_NAME}...")
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }
    resp = requests.delete(
        f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}?id=gt.0",
        headers=headers,
        timeout=30,
    )
    if resp.status_code in (200, 204):
        print("  ✓ All existing rows deleted")
    else:
        print(f"  ✗ Delete failed: {resp.status_code} {resp.text}")
        sys.exit(1)


def insert_rows(rows):
    """Insert new rows into the table."""
    print(f"Inserting {len(rows)} rows into {TABLE_NAME}...")
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}",
        headers=headers,
        json=rows,
        timeout=30,
    )
    if resp.status_code in (200, 201):
        print(f"  ✓ {len(rows)} rows inserted successfully")
    else:
        print(f"  ✗ Insert failed: {resp.status_code} {resp.text}")
        sys.exit(1)


def main():
    print("=" * 60)
    print("Product Hunt Top Products Fetcher")
    print("=" * 60)

    # Validate env vars
    missing = []
    if not PH_API_KEY:
        missing.append("PH_API_KEY")
    if not PH_API_SECRET:
        missing.append("PH_API_SECRET")
    if not SUPABASE_URL:
        missing.append("SUPABASE_URL")
    if not SUPABASE_SERVICE_ROLE_KEY:
        missing.append("SUPABASE_SERVICE_ROLE_KEY")
    if missing:
        print(f"ERROR: Missing environment variables: {', '.join(missing)}")
        sys.exit(1)

    # Step 1: Authenticate
    token = get_ph_token()

    # Step 2: Fetch top 15 products
    products = fetch_top_products(token, count=15)

    # Step 3: Delete all existing rows (overwrite)
    delete_all_rows()

    # Step 4: Insert new rows
    insert_rows(products)

    print("\n" + "=" * 60)
    print("✓ Done! Top 15 Product Hunt products stored in Supabase.")
    print("=" * 60)


if __name__ == "__main__":
    main()
