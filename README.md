# Sensor Tower Data Fetcher

Automated daily fetcher for Sensor Tower app analytics data. Fetches top apps by downloads, download growth, and top advertisers, then stores the data in Supabase.

## Data Collected

| Table | Description | Key Metrics |
|-------|-------------|-------------|
| `download_rank_30d` | Top 15 apps by absolute downloads (last 30 days) | Downloads, delta, % change |
| `download_percent_rank_30d` | Top 15 apps by download % increase (last 30 days) | % increase, downloads |
| `advertiser_rank_30d` | Top 15 advertisers by Share of Voice (US) | SoV score |

## Setup

### Required Secrets (GitHub Actions)

| Secret | Description |
|--------|-------------|
| `SENSORTOWER_API_KEY` | Sensor Tower API authentication key |
| `SUPABASE_URL` | Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Supabase service role key for write access |

### Schedule

Runs daily at **00:00 UTC** (8:00 AM GMT+8) via GitHub Actions cron.

Can also be triggered manually from the Actions tab.

## Local Development

```bash
export SENSORTOWER_API_KEY="your_key"
export SUPABASE_URL="https://your-project.supabase.co"
export SUPABASE_SERVICE_ROLE_KEY="your_service_role_key"
pip install -r requirements.txt
python fetch_sensortower.py
```
