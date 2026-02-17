# lumen-scout

`lumen-scout` is a standalone Python 3.12 CLI tool for discovering private K–12 schools, enriching public lead data, and producing lightweight outreach artifacts for manual delivery.

## Features

- Discover private K–12 schools by city (default: `Downey, CA`)
- Provider support:
  - SerpAPI (`SERPAPI_API_KEY`) **implemented and recommended default**
  - Brave Search (`BRAVE_SEARCH_API_KEY`) implemented (optional)
- De-duplicate leads by:
  - Website domain
  - Fuzzy school name match
- Enrich each lead from public pages:
  - Homepage
  - `/contact`
  - `/about`
- Extract contact email when publicly visible
- Generate:
  - First outreach draft markdown
  - Follow-up draft markdown
  - Custom call brief markdown
- Manual delivery mode for v1, with a delivery interface that includes a Gmail draft stub for future implementation
- `--dry-run` for `enrich`, `draft`, and `brief` commands (no API calls, uses placeholders)

## Installation

```bash
python3.12 -m venv .venv
source .venv/bin/activate
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
```

## Commands

### 1) Discover

```bash
lumen-scout discover --city "Downey, CA" --max 25 --provider serpapi
# or
lumen-scout discover --city "Downey, CA" --max 25 --provider brave
```

Creates `./data/leads.csv` if missing and appends deduped leads.

### 2) Enrich

```bash
lumen-scout enrich --input ./data/leads.csv
# dry-run
lumen-scout enrich --input ./data/leads.csv --dry-run
```

Fetches public pages (respecting robots where feasible), extracts email when available, and generates `personalization_hook`.

### 3) Draft initial outreach

```bash
lumen-scout draft --input ./data/leads.csv --limit 10 --delivery-mode manual
# dry-run
lumen-scout draft --input ./data/leads.csv --limit 10 --delivery-mode manual --dry-run
```

Outputs markdown files:
- `./outreach_drafts/<lead_id>_email1.md`

### 4) Follow-up drafts

```bash
lumen-scout followup --input ./data/leads.csv --days 5
```

Outputs markdown files:
- `./outreach_drafts/<lead_id>_followup_day5.md`

### 5) Call brief

```bash
lumen-scout brief --input ./data/leads.csv --lead-id <uuid>
# dry-run
lumen-scout brief --input ./data/leads.csv --lead-id <uuid> --dry-run
```

Outputs:
- `./call_briefs/<lead_id>.md`

## Data contract

`./data/leads.csv` is the source of truth.

Columns:

- `lead_id`
- `school_name`
- `city`
- `website`
- `domain`
- `provider`
- `contact_email`
- `contact_role`
- `all_emails`
- `primary_contact`
- `linkedin_url`
- `contact_method`
- `contact_score`
- `contact_priority_label`
- `contact_form_url`
- `contact_page`
- `about_page`
- `personalization_hook`
- `enriched_at`
- `email1_path`
- `followup_path`
- `brief_path`
- `notes`

## Delivery architecture

- `Delivery` interface
- `ManualDelivery` writes markdown files
- `GmailDraftDelivery` exists as a stub (not implemented in v1)

## Notes

- No login-protected scraping is attempted.
- Gmail API is intentionally not used in v1.
- For production use, add retries, stronger robots handling, and richer ranking/validation for discovered entities.
