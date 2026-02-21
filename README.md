# lumen-scout

`lumen-scout` is a Python 3.12 CLI for discovering private K–12 schools, enriching public lead data, and generating outreach artifacts.

Phase 2 updates this repo to use `scout-core` + SQLite as the canonical store. `./data/leads.csv` is now an exported legacy view for compatibility.
## Features

- Discover private K–12 schools by city (default: `Downey, CA`)
- Provider support:
  - SerpAPI (`SERPAPI_API_KEY`) **implemented and recommended default**
  - Brave Search (`BRAVE_SEARCH_API_KEY`) implemented (optional)
- Canonical lead storage in SQLite (through lumen-scout's scout-core integration layer)
- Legacy CSV export compatibility at `./data/leads.csv`
- Enrich each lead from public pages:
  - Homepage
  - `/contact`
  - `/about`
- Extract contact email when publicly visible
- Generate:
  - First outreach draft markdown
  - Follow-up draft markdown
  - Custom call brief markdown
- Manual delivery mode for v1
- `--dry-run` for `enrich`, `draft`, `followup`, and `brief`

## Installation

### Windows PowerShell

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ..\scout-core
pip install -e .
```

### macOS/Linux

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ../scout-core
pip install -e .
```

## Environment variables

```bash
# Required for discover with SerpAPI provider
export SERPAPI_API_KEY="your_serpapi_key"

# Optional alternative provider
# export BRAVE_SEARCH_API_KEY="your_brave_search_key"

# Required for non-dry-run enrich/draft/followup/brief content generation
export OPENAI_API_KEY="your_openai_api_key"

# Optional: change OpenAI model
# export OPENAI_MODEL="gpt-4o-mini"

# Optional: override SQLite path used by scout-core integration
# Default: C:\Users\danie\dev\scout-data\scout.db
# export SCOUT_DB_PATH="C:\\path\\to\\scout.db"
```

## Commands

### 1) Discover

```bash
lumen-scout discover --city "Downey, CA" --max 25 --provider serpapi
# or
lumen-scout discover --city "Downey, CA" --max 25 --provider brave
```

Discovers schools, upserts canonical leads, then exports `./data/leads.csv`.

### 2) Enrich

```bash
lumen-scout enrich --input ./data/leads.csv
# dry-run
lumen-scout enrich --input ./data/leads.csv --dry-run
```

Loads leads from canonical storage, enriches them, upserts updates, then re-exports `./data/leads.csv`.

### 3) Draft initial outreach

```bash
lumen-scout draft --input ./data/leads.csv --limit 10 --delivery-mode manual
# dry-run
lumen-scout draft --input ./data/leads.csv --limit 10 --delivery-mode manual --dry-run
```

Outputs markdown files in `./outreach_drafts` and persists `email1_path` to canonical storage.

### 4) Follow-up drafts

```bash
lumen-scout followup --input ./data/leads.csv --days 5
```

Outputs markdown files in `./outreach_drafts` and persists `followup_path`.

### 5) Call brief

```bash
lumen-scout brief --input ./data/leads.csv --lead-id <uuid>
# dry-run
lumen-scout brief --input ./data/leads.csv --lead-id <uuid> --dry-run
```

Outputs `./call_briefs/<lead_id>.md` and persists `brief_path`.

## Smoke test

### Windows PowerShell

```powershell
# 1) Discover writes canonical DB + exports CSV
lumen-scout discover --city "Downey, CA" --max 5 --provider serpapi

# 2) Confirm CSV is present/updated
Get-Item .\data\leads.csv
Get-Content .\data\leads.csv -TotalCount 5

# 3) Enrich dry-run still works
lumen-scout enrich --input .\data\leads.csv --dry-run

# 4) Empty DB safety check (optional)
$env:SCOUT_DB_PATH = "$PWD\tmp\empty.db"
lumen-scout enrich --input .\data\leads.csv --dry-run
```

## Data contract compatibility

Legacy CSV columns are preserved:

- `school_name` maps from canonical `name`
- `city` maps from `extras_json["city"]`
- Universal fields (`website`, `domain`, `contact_email`, etc.) map from canonical columns
- School-specific values not represented canonically are retained in `extras_json` and restored during export when available

## Notes

- No login-protected scraping is attempted.
- Gmail API is intentionally not used in v1.
- For production use, add retries, stronger robots handling, and richer ranking/validation for discovered entities.
