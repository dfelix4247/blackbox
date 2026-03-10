from __future__ import annotations

# ---------------------------------------------------------------------------
# Exit code registry (documented here for downstream consumers)
# ---------------------------------------------------------------------------
# 0  = success (summary.status == "ok")
# 2  = invalid input  (missing query, max <= 0, bad args)
# 3  = configuration error (missing/invalid API key)
# 4  = provider / network error (timeout, HTTP error)
# 5  = provider rate-limited / quota exceeded
# ---------------------------------------------------------------------------

import hashlib
import json
import logging
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urldefrag, urlparse, urlunparse

import typer

from .core_store import (
    UpsertResult,
    create_working_set,
    ensure_seeded_from_csv,
    export_legacy_schools_csv,
    get_active_working_set,
    get_active_working_set_lead_ids,
    get_all_leads,
    get_leads_by_ids,
    get_leads_by_type,
    has_active_working_set,
    upsert_lead,
    upsert_school_lead,  # kept for callers that still use the legacy name
)
from .delivery import ManualDelivery
from .enrichment import enrich_lead
from .llm import LLMService
from .models import Lead
from .providers import get_provider


# ---------------------------------------------------------------------------
# Logging — always goes to stderr, never stdout
# ---------------------------------------------------------------------------

logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="%(levelname)s %(message)s",
)
log = logging.getLogger("lumen-scout")

# ---------------------------------------------------------------------------
# Third-party HTTP library loggers that emit INFO lines like:
#   "INFO HTTP Request: GET https://..."
# These must be capped at WARNING so they never appear on stdout or stderr
# during discover-web (where stdout must be pure JSONL).
# ---------------------------------------------------------------------------
_NOISY_LOGGERS = ("httpx", "httpcore", "httpcore.http11", "urllib3", "urllib3.connectionpool")


def _scrub_stdout_handlers() -> None:
    """
    Walk every logger (root + all named) and remove any handler whose
    stream points to stdout in any form.  Safe to call multiple times.
    """
    stdout_streams = {sys.stdout, sys.__stdout__} if sys.__stdout__ is not None else {sys.stdout}
    all_loggers: list[logging.Logger] = [logging.getLogger()]
    for name in list(logging.Logger.manager.loggerDict):
        obj = logging.Logger.manager.loggerDict[name]
        if isinstance(obj, logging.Logger):
            all_loggers.append(obj)
    for logger in all_loggers:
        for handler in list(logger.handlers):
            if getattr(handler, "stream", None) in stdout_streams:
                logger.removeHandler(handler)
                handler.close()


def _reconfigure_logging_to_stderr() -> None:
    """
    Ensure ALL loggers write only to stderr, and silence noisy libraries.

    Strategy:
      1. Scrub every stdout handler from every logger (root + named).
      2. Silence known HTTP-client loggers AND the provider's own logger
         (which emits INFO [DISCOVER] lines) to WARNING level so their
         INFO lines never reach stdout or stderr during discover-web.
      3. Guarantee root logger has exactly one stderr handler.

    The scrub is intentionally idempotent — call it as many times as
    needed (e.g. both before and after provider calls).
    """
    # --- step 1: scrub all stdout handlers ---
    _scrub_stdout_handlers()

    # --- step 2: silence noisy loggers ---
    # _NOISY_LOGGERS covers HTTP clients; "lumen-scout.providers" and the
    # root "lumen_scout" / module-level loggers cover the provider's own
    # INFO [DISCOVER] messages.  We widen the net to catch any logger whose
    # name contains known noisy prefixes.
    for name in _NOISY_LOGGERS:
        _silence_logger(name)

    # --- step 3: ensure root has a stderr handler ---
    root = logging.getLogger()
    if not any(
        getattr(h, "stream", None) in (sys.stderr, sys.__stderr__)
        for h in root.handlers
    ):
        h = logging.StreamHandler(sys.stderr)
        h.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        root.addHandler(h)


def _silence_logger(name: str) -> None:
    """Cap a named logger at WARNING and remove any stdout handlers it has."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.WARNING)
    stdout_streams = {sys.stdout, sys.__stdout__} if sys.__stdout__ is not None else {sys.stdout}
    for handler in list(logger.handlers):
        if getattr(handler, "stream", None) in stdout_streams:
            logger.removeHandler(handler)
            handler.close()


# ---------------------------------------------------------------------------
# App definition
# ---------------------------------------------------------------------------

APP_HELP = """\
lumen-scout: discover, enrich, and draft school outreach — plus generic web discovery.

COMMANDS (modes)
  discover        School pipeline: find private K-12 schools and upsert leads.
  discover-web    Generic web discovery: run any free-text query; outputs strict JSONL.
  enrich          Enrich school leads from their websites and generate personalisation hooks.
  draft           Generate initial outreach draft markdown files for school leads.
  followup        Generate follow-up markdown drafts for school leads.
  brief           Generate a custom call-brief markdown for a single school lead.
  capabilities    Print a short capabilities summary with example invocations.

QUICK EXAMPLES
  lumen-scout discover --city "Downey, CA" --max 25
  lumen-scout discover-web "real estate investing in Downey" --max 5
  lumen-scout capabilities

EXIT CODES (discover-web)
  0  success
  2  invalid input
  3  configuration error (bad/missing API key)
  4  provider / network error
  5  rate-limited / quota exceeded
"""

app = typer.Typer(help=APP_HELP, no_args_is_help=True)


# ---------------------------------------------------------------------------
# JSONL output helpers — ALL structured output goes to stdout via these only
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Stdout writer — always UTF-8, always to fd-1, never affected by console
# encoding or PowerShell's redirection encoding.
# ---------------------------------------------------------------------------
# On Windows, sys.stdout (and sys.__stdout__) use the console code page
# (often CP1252 or UTF-16-LE when redirected with 1>), which corrupts JSON.
# We bypass that entirely by writing raw UTF-8 bytes directly to fd-1 via
# os.fdopen / the underlying buffer, then flushing.
# ---------------------------------------------------------------------------
import io as _io
import os as _os

def _make_utf8_stdout() -> _io.TextIOWrapper:
    """Return a UTF-8 text writer bound directly to fd 1 (stdout)."""
    try:
        # Duplicate fd 1 so our wrapper owns its own fd and won't interfere
        # with sys.stdout if something else holds it open.
        fd1_dup = _os.dup(1)
        raw = _os.fdopen(fd1_dup, "wb", buffering=0)
        return _io.TextIOWrapper(raw, encoding="utf-8", line_buffering=False)
    except Exception:
        # Absolute fallback: reconfigure sys.stdout if possible, else use as-is
        try:
            sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
        except Exception:
            pass
        return sys.stdout


_UTF8_STDOUT: _io.TextIOWrapper = _make_utf8_stdout()


def _emit(record: dict) -> None:
    """
    Write one JSON object as a single line to stdout (fd-1) in UTF-8 and
    flush immediately.

    Uses _UTF8_STDOUT — a TextIOWrapper bound directly to a dup of fd-1 with
    explicit UTF-8 encoding — so output is never corrupted by the Windows
    console code page or PowerShell's UTF-16-LE redirection encoding.
    """
    line = json.dumps(record, ensure_ascii=False) + "\n"
    _UTF8_STDOUT.write(line)
    _UTF8_STDOUT.flush()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# String / URL sanitisation helpers
# ---------------------------------------------------------------------------

_CTRL_RE = re.compile(r"[\x00-\x1f\x7f]")


def _sanitise_str(value: Any) -> str:
    """Return a clean, UTF-8-safe, control-char-free string."""
    if not isinstance(value, str):
        value = str(value) if value is not None else ""
    value = value.encode("utf-8", errors="replace").decode("utf-8", errors="replace")
    value = _CTRL_RE.sub(" ", value)
    value = unicodedata.normalize("NFC", value)
    return value.strip()


def _normalise_url(raw: str) -> str:
    """
    Normalise a URL for stable ID generation and deduplication:
      - lower-case scheme + host
      - strip fragment
      - strip trailing slash from path
      - returns "" if not a valid absolute http/https URL
    """
    raw = _sanitise_str(raw)
    if not raw:
        return ""
    raw, _ = urldefrag(raw)
    try:
        parsed = urlparse(raw)
    except Exception:
        return ""
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return ""
    return urlunparse((
        parsed.scheme.lower(),
        parsed.netloc.lower(),
        parsed.path.rstrip("/") or "/",
        parsed.params,
        parsed.query,
        "",
    ))


def _make_id(url: str, title: str, snippet: str) -> str:
    """
    Stable deterministic 12-char hex ID.
      Primary key:  sha1(normalised_url)
      Fallback:     sha1(title + snippet)  — used when URL is empty/invalid
    """
    norm = _normalise_url(url)
    seed = norm if norm else (_sanitise_str(title) + _sanitise_str(snippet))
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Generic result → Lead mapping
# ---------------------------------------------------------------------------

def _result_to_lead(result: dict) -> Lead:
    """
    Map one discover-web result dict into a generic Lead suitable for upsert.

    Mapping rules:
      - title          → name  (primary display field; no school_name needed)
      - url            → website
      - domain         → extracted from url netloc (stripped of www.)
      - snippet        → notes  (preserves discovery context)
      - query          → source_query
      - lead_type      → "generic"  (never masquerades as a school)

    The Lead model validator will ensure name is set and leave school_name
    None (appropriate for non-school entities).
    """
    url     = result.get("url", "") or ""
    title   = result.get("title", "") or ""
    snippet = result.get("snippet", "") or ""
    query   = result.get("query", "") or ""

    # Extract bare domain from the URL for reliable deduplication.
    domain: str | None = None
    try:
        parsed = urlparse(url)
        if parsed.netloc:
            domain = parsed.netloc.lower().removeprefix("www.") or None
    except Exception:
        pass

    return Lead(
        name=title or url or "unknown",
        lead_type="generic",
        website=url or None,
        domain=domain,
        source_query=query or None,
        notes=snippet or None,
        provider=result.get("source"),
    )


# ---------------------------------------------------------------------------
# Error-code classification
# ---------------------------------------------------------------------------

class DiscoveryError(Exception):
    """Raised by _run_web_discovery to signal a structured, classified failure."""

    def __init__(
        self,
        code: str,
        message: str,
        exit_code: int,
        details: Optional[dict] = None,
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.exit_code = exit_code
        self.details: dict = details or {}


def _classify_exception(exc: Exception) -> DiscoveryError:
    """Map a raw provider exception to a DiscoveryError with a stable code."""
    msg = str(exc).lower()
    if any(k in msg for k in ("api key", "apikey", "api_key", "unauthorized", "401", "forbidden", "403")):
        return DiscoveryError("API_KEY_MISSING", str(exc), exit_code=3)
    if any(k in msg for k in ("rate limit", "rate_limit", "quota", "429", "too many")):
        return DiscoveryError("RATE_LIMITED", str(exc), exit_code=5)
    if any(k in msg for k in ("timeout", "timed out", "connection", "network", "socket")):
        return DiscoveryError(
            "HTTP_ERROR", str(exc), exit_code=4, details={"hint": "network/timeout"}
        )
    return DiscoveryError("HTTP_ERROR", str(exc), exit_code=4)


# ---------------------------------------------------------------------------
# Core web-discovery logic (no I/O; raises DiscoveryError on failure)
# ---------------------------------------------------------------------------

_PROVIDER_TIMEOUT_S = 25
_MAX_RETRIES = 1


def _run_web_discovery(
    query: str,
    max_results: int,
    engine: str,
    location: Optional[str],
) -> list[dict]:
    """
    Execute a free-text web query through the named engine.

    Returns a deduplicated list of result dicts (type="result").
    Zero results is NOT an error — returns an empty list.
    Raises DiscoveryError on any fatal failure.

    IMPORTANT: this function must never write to stdout.  All diagnostic
    output uses log.* (wired to stderr) or raises DiscoveryError.
    """
    try:
        svc = get_provider(engine)
    except Exception as exc:
        raise _classify_exception(exc) from exc

    # Re-scrub after provider initialisation: the provider module may have
    # called logging.basicConfig() or attached its own handlers during import.
    # Also silence its logger so INFO [DISCOVER] lines never reach stdout.
    _scrub_stdout_handlers()
    for _noisy in _NOISY_LOGGERS:
        _silence_logger(_noisy)
    # Silence any logger the provider registered under its own name by
    # suppressing all loggers that are not "lumen-scout" and not root.
    for _lname in list(logging.Logger.manager.loggerDict):
        _obj = logging.Logger.manager.loggerDict[_lname]
        if isinstance(_obj, logging.Logger) and not _lname.startswith("lumen-scout"):
            _obj.setLevel(logging.WARNING)

    effective_query = query if location is None else f"{query} {location}"
    fetched_at = _now_iso()

    raw_leads = None
    for attempt in range(_MAX_RETRIES + 1):
        try:
            raw_leads = svc.search(city=effective_query, max_results=max_results)
            # Re-scrub after search in case the provider attached handlers mid-call
            _scrub_stdout_handlers()
            break
        except Exception as exc:
            classified = _classify_exception(exc)
            if classified.exit_code in (2, 3):
                raise classified from exc
            if attempt < _MAX_RETRIES:
                sleep_s = 2 if classified.exit_code == 5 else 1
                log.warning(
                    "Provider attempt %d/%d failed (%s); retrying in %ds…",
                    attempt + 1, _MAX_RETRIES + 1, classified.code, sleep_s,
                )
                time.sleep(sleep_s)
            else:
                raise classified from exc

    assert raw_leads is not None

    seen_urls: dict[str, int] = {}
    results: list[dict] = []
    raw_count = len(raw_leads)  # total from provider before URL-level dedupe

    for rank, lead in enumerate(raw_leads, start=1):
        # Generic-first name extraction: prefer name/title over school_name.
        # school_name is a legacy provider field; generic providers may use
        # 'name' or 'title' instead.  Use the first non-empty value.
        title = _sanitise_str(
            getattr(lead, "name",        None) or
            getattr(lead, "title",       None) or
            getattr(lead, "school_name", None) or
            ""
        )
        raw_url = _sanitise_str(getattr(lead, "website", "") or "")
        snippet = _sanitise_str(
            getattr(lead, "snippet", "")
            or getattr(lead, "source_query", "")
            or ""
        )
        norm_url = _normalise_url(raw_url)
        url      = norm_url if norm_url else raw_url

        if norm_url:
            if norm_url in seen_urls:
                log.debug(
                    "Deduplicating rank=%d url=%s (already seen at rank=%d)",
                    rank, norm_url, seen_urls[norm_url],
                )
                continue
            seen_urls[norm_url] = rank

        results.append({
            "type":       "result",
            "query":      query,
            "rank":       rank,
            "id":         _make_id(url, title, snippet),
            "title":      title,
            "url":        url,
            "snippet":    snippet,
            "source":     engine,
            "fetched_at": fetched_at,
        })

    return results, raw_count


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Follow-up intent resolution (v1 — rule-based, no NLP dependency)
# ---------------------------------------------------------------------------
# These helpers are used by the `run` command to interpret natural-language
# continuation phrases typed after a discovery run.
# ---------------------------------------------------------------------------

# Keyword sets per intent bucket.  Order matters: first match wins in
# resolve_followup_intent().
_INTENT_ENRICH: tuple[str, ...] = (
    "enrich", "look deeper", "get more details", "scrape their sites",
    "scrape sites", "find contact info", "learn more", "research these",
)
_INTENT_DRAFT: tuple[str, ...] = (
    "write outreach", "generate cold emails", "draft messages",
    "prepare emails", "write emails", "draft outreach", "draft",
)
_INTENT_EXPORT: tuple[str, ...] = (
    "export", "save this list", "download this", "give me csv",
    "export these leads", "export leads",
)
_INTENT_RANK: tuple[str, ...] = (
    "rank", "score", "prioritize", "prioritise", "rank these",
    "score these", "rank this list", "score this list",
)

# Phrases that imply "the current working set".
_WS_REFERENCES: tuple[str, ...] = (
    "these", "this list", "those", "these leads", "these companies",
    "the results", "the last search",
)


def resolve_followup_intent(text: str) -> Optional[str]:
    """
    Map a natural-language string to one of: "enrich", "draft", "export",
    "rank", or None if no intent is recognised.

    Matching is case-insensitive substring search.  First bucket to match wins.
    """
    lowered = text.lower()
    for phrase in _INTENT_ENRICH:
        if phrase in lowered:
            return "enrich"
    for phrase in _INTENT_DRAFT:
        if phrase in lowered:
            return "draft"
    for phrase in _INTENT_EXPORT:
        if phrase in lowered:
            return "export"
    for phrase in _INTENT_RANK:
        if phrase in lowered:
            return "rank"
    return None


def references_active_working_set(text: str) -> bool:
    """
    Return True if the text contains a phrase that references the active
    working set (e.g. "these", "this list", "the results").
    """
    lowered = text.lower()
    return any(ref in lowered for ref in _WS_REFERENCES)


# ---------------------------------------------------------------------------
# Scoped workflow helpers — operate on a specific set of lead IDs only.
# These are intentionally separate from the global enrich/draft commands so
# the working-set continuation path doesn't alter broad command behavior.
# ---------------------------------------------------------------------------

def _enrich_leads(leads: list[Lead], dry_run: bool = False) -> int:
    """Enrich a specific list of leads and persist each result. Returns count."""
    llm = LLMService(dry_run=dry_run)
    count = 0
    for lead in leads:
        enriched = enrich_lead(lead, llm)
        upsert_lead(enriched)
        count += 1
    return count


def _draft_leads(leads: list[Lead], limit: int, dry_run: bool = False) -> int:
    """Generate outreach drafts for a specific list of leads. Returns count."""
    llm = LLMService(dry_run=dry_run)
    delivery = ManualDelivery()
    sorted_leads = sorted(leads, key=lambda l: l.contact_score, reverse=True)
    count = 0
    for lead in sorted_leads:
        if count >= limit:
            break
        method = lead.contact_method or "none"
        tier   = lead.contact_priority_label or "Tier 5"
        if tier in {"Tier 1", "Tier 3"}:
            content     = llm.email_draft(lead)
            output_path = Path("outreach_drafts") / f"{lead.lead_id}_email1.md"
            delivery.deliver(lead, content, output_path)
            lead.email1_path = str(output_path)
        elif tier == "Tier 4":
            content     = llm.contact_form_draft(lead)
            output_path = Path("outreach_drafts") / f"{lead.lead_id}_contact_form.md"
            delivery.deliver(lead, content, output_path)
            lead.email1_path = str(output_path)
        elif tier == "Tier 5" and method == "phone_only":
            continue
        if tier in {"Tier 1", "Tier 2"} and lead.linkedin_url:
            linkedin_content = llm.linkedin_draft(lead)
            lp = Path("outreach_drafts") / f"{lead.lead_id}_linkedin.md"
            delivery.deliver(lead, linkedin_content, lp)
        if tier in {"Tier 1", "Tier 2"} and lead.contact_email and not lead.email1_path:
            email_content = llm.email_draft(lead)
            ep = Path("outreach_drafts") / f"{lead.lead_id}_email1.md"
            delivery.deliver(lead, email_content, ep)
            lead.email1_path = str(ep)
        upsert_lead(lead)
        count += 1
    return count


def _export_leads(leads: list[Lead], path: Path) -> Path:
    """Export a specific list of leads to CSV. Returns the written path."""
    from .models import CSV_FIELDS
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        import csv as _csv
        writer = _csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for lead in leads:
            writer.writerow(lead.model_dump())
    return path


def _score_lead(lead: Lead) -> tuple[int, str, str]:
    """
    Score a single lead using additive, rule-based field checks.

    Returns (score, label, reasons) where:
      score   — int, capped at 100
      label   — "high" / "medium" / "low"
      reasons — semicolon-separated human-readable explanation

    Scoring rules (v1):
      +20  has contact_email
      +10  has contact_role
      +15  has personalization_hook
      +10  has website
      +10  has domain
      +10  has contact_priority_label
      +10  has phone
      + 5  has city
      +10  is a school lead with a name (school_name or name)
    """
    score   = 0
    reasons: list[str] = []

    if lead.contact_email:
        score += 20
        reasons.append("has contact email")

    if lead.contact_role:
        score += 10
        reasons.append("has contact role")

    if lead.personalization_hook:
        score += 15
        reasons.append("has personalization hook")

    if lead.website:
        score += 10
        reasons.append("has website")

    if lead.domain:
        score += 10
        reasons.append("has domain")

    if lead.contact_priority_label:
        score += 10
        reasons.append("has contact priority label")

    if lead.phone:
        score += 10
        reasons.append("has phone")

    if lead.city:
        score += 5
        reasons.append("has city")

    if lead.lead_type == "school" and (lead.school_name or lead.name):
        score += 10
        reasons.append("is named school lead")

    score = min(score, 100)

    if score >= 60:
        label = "high"
    elif score >= 30:
        label = "medium"
    else:
        label = "low"

    return score, label, "; ".join(reasons) if reasons else "no qualifying fields"


def _rank_leads(leads: list[Lead], version: str = "v1") -> list[Lead]:
    """
    Score, label, and persist a list of leads using the v1 rule-based scorer.

    For each lead:
      1. Calls _score_lead() to get (score, label, reasons).
      2. Applies mark_ranked() so all ranking fields update atomically.
      3. Persists the updated lead via upsert_lead().

    Returns the leads sorted by lead_score DESC, then by display name ASC as
    a deterministic tie-breaker.
    """
    for lead in leads:
        score, label, reasons = _score_lead(lead)
        lead.mark_ranked(score=score, label=label, reasons=reasons, version=version)
        upsert_lead(lead)

    return sorted(leads, key=lambda l: (-l.lead_score, l.display_name.lower()))


def _print_ranking_summary(ranked: list[Lead], version: str, top: int = 5) -> None:
    """
    Print a concise, human-readable ranking summary to stdout.

    Each displayed lead shows its score, label, and the reasons that
    produced that score.  Leads with no reasons string are shown without
    the reasons line rather than printing a blank or None.
    """
    typer.echo(f"\nRanked {len(ranked)} leads using ranking version {version}.\n")
    typer.echo("Top ranked leads:")
    for i, lead in enumerate(ranked[:top], start=1):
        typer.echo(
            f"  {i}. {lead.display_name} — {lead.lead_score} ({lead.lead_score_label})"
        )
        reasons = (lead.lead_score_reasons or "").strip()
        if reasons:
            typer.echo(f"     reasons: {reasons}")
    if len(ranked) > top:
        typer.echo(f"  … and {len(ranked) - top} more.")

@app.command(
    name="discover",
    help=(
        "SCHOOL PIPELINE — discover private K-12 schools and upsert leads into "
        "scout-core storage.\n\n"
        "Example: lumen-scout discover --city 'Downey, CA' --max 25"
    ),
)
def discover(
    city: str = typer.Option("Downey, CA", "--city", help="City (and state) to search."),
    max_results: int = typer.Option(25, "--max", min=1, help="Maximum number of schools to return."),
    provider: str = typer.Option("serpapi", "--provider", help="Search provider to use."),
) -> None:
    """Discover private K-12 schools and upsert leads into scout-core storage."""
    _reconfigure_logging_to_stderr()
    logging.getLogger().setLevel(logging.INFO)
    for handler in logging.getLogger().handlers:
        handler.setFormatter(logging.Formatter("%(message)s"))

    # Count only school leads before/after so the summary is not polluted by
    # generic leads that may already exist in the same store.
    before_school_count = len(get_leads_by_type("school"))
    svc = get_provider(provider)
    found = svc.search(city=city, max_results=max_results)

    inserted = 0
    updated  = 0
    persisted_lead_ids: list[str] = []
    for lead in found:
        typer.echo(
            f"[DISCOVER] accepted lead='{lead.school_name}' query='{lead.source_query}' "
            f"website='{lead.website or 'n/a'}'"
        )
        # Persist each found school lead through the canonical store.
        res = upsert_lead(lead)
        persisted_lead_ids.append(lead.lead_id)
        if res.status == "inserted":
            inserted += 1
        else:
            updated += 1

    # Create (or replace) the active working set with this run's lead IDs.
    # This allows `lumen-scout run "enrich these"` to operate on exactly
    # the leads discovered in this command without re-querying the DB.
    if persisted_lead_ids:
        create_working_set(
            lead_ids=persisted_lead_ids,
            query=city,
            source_command="discover",
            lead_type="school",
        )

    csv_path = export_legacy_schools_csv()
    typer.echo(
        f"Discovered {len(found)} school results; "
        f"{inserted} new, {updated} updated — "
        f"{before_school_count + inserted} total school leads -> {csv_path}"
    )
    if persisted_lead_ids:
        typer.echo(
            f"Active working set updated ({len(persisted_lead_ids)} leads). "
            "Follow up with: lumen-scout run \"enrich these\""
        )


@app.command(
    name="discover-web",
    help=(
        "GENERIC WEB DISCOVERY — run any free-text QUERY and receive strictly\n"
        "machine-parseable JSONL on stdout. Logs and progress always go to stderr.\n\n"
        "Output format (stdout only, always valid JSONL):\n"
        "  result  — one per found item (type='result')\n"
        "            fields: id, query, rank, title, url, snippet, source, fetched_at\n"
        "  error   — emitted on failure before summary (type='error')\n"
        "            fields: code, message, details\n"
        "  summary — always the final line (type='summary')\n"
        "            fields: query, source, requested_max, returned_count,\n"
        "                    deduped_count, duration_ms, fetched_at, status\n\n"
        "Exit codes: 0=ok  2=bad input  3=config error  4=network/provider  5=rate-limited\n\n"
        'Pipe-safe: discover-web ... | python -c "import sys,json; [json.loads(l) for l in sys.stdin]"\n\n'
        "Example: lumen-scout discover-web 'real estate investing in Downey' --max 5"
    ),
)
def discover_web(
    query: str = typer.Argument(
        ..., help="Free-text search query (wrap in quotes if it contains spaces)."
    ),
    max_results: int = typer.Option(
        5, "--max", min=1, help="Maximum number of results to return."
    ),
    engine: str = typer.Option(
        "serpapi", "--engine", help="Search engine / provider to use."
    ),
    location: Optional[str] = typer.Option(
        None, "--location", help="Optional location hint passed to the engine."
    ),
    save: Optional[Path] = typer.Option(
        None,
        "--save",
        help="Also write the full JSON array of results to this file path (stderr logs save status).",
        writable=True,
        resolve_path=True,
    ),
) -> None:
    """
    GENERIC WEB DISCOVERY

    stdout contract: ONLY _emit() calls may write to stdout in this command.
    stdout = strict JSONL (result records, optional error record, summary record).
    stderr = everything else (logs, progress, warnings, diagnostics).
    """
    # Step 1: wire all logging to stderr before anything else runs.
    _reconfigure_logging_to_stderr()

    # Step 2: validate input — emit error+summary then exit 2 on bad input.
    query = (query or "").strip()
    if not query:
        _emit({
            "type": "error", "query": query, "source": engine,
            "code": "INVALID_INPUT", "message": "query must not be empty",
            "details": {},
        })
        _emit({
            "type": "summary", "query": query, "source": engine,
            "requested_max": max_results, "returned_count": 0, "deduped_count": 0,
            "duration_ms": 0, "fetched_at": _now_iso(), "status": "error",
        })
        raise typer.Exit(code=2)

    t_start = time.monotonic()
    fetched_at = _now_iso()
    exit_code = 0
    results: list[dict] = []
    raw_count = 0  # items returned by provider before URL-level dedupe
    summary_emitted = False
    # Persistence counters — reported in summary, never affect JSONL result records.
    _leads_inserted = 0
    _leads_updated  = 0
    # Ordered lead IDs for working set creation (populated during persistence loop).
    _persisted_lead_ids: list[str] = []

    # Step 3: outer try/finally guarantees summary is emitted even on
    # unexpected BaseException (e.g. SystemExit raised by typer internals,
    # KeyboardInterrupt, or an unanticipated crash in provider code).
    try:
        try:
            results, raw_count = _run_web_discovery(
                query=query,
                max_results=max_results,
                engine=engine,
                location=location,
            )
            # Emit each result to stdout first (preserves JSONL contract),
            # then persist to the store — store failures are logged to stderr
            # and never corrupt stdout.
            for record in results:
                _emit(record)

            for record in results:
                try:
                    lead = _result_to_lead(record)
                    upsert_result = upsert_lead(lead)
                    # Capture the lead_id regardless of insert/update so the
                    # working set preserves original result order.
                    _persisted_lead_ids.append(lead.lead_id)
                    if upsert_result.status == "inserted":
                        _leads_inserted += 1
                    else:
                        _leads_updated += 1
                except Exception as _persist_exc:
                    log.warning("Failed to persist result '%s': %s", record.get("title"), _persist_exc)

            # Create (or replace) the active working set — stderr only, never stdout.
            if _persisted_lead_ids:
                try:
                    create_working_set(
                        lead_ids=_persisted_lead_ids,
                        query=query,
                        source_command="discover-web",
                        lead_type="generic",
                    )
                    log.info(
                        "Active working set updated (%d leads). "
                        "Follow up with: lumen-scout run \"enrich these\"",
                        len(_persisted_lead_ids),
                    )
                except Exception as _ws_exc:
                    log.warning("Failed to create working set: %s", _ws_exc)

        except DiscoveryError as de:
            exit_code = de.exit_code
            log.error("%s: %s", de.code, de.message)
            _emit({
                "type":    "error",
                "query":   query,
                "source":  engine,
                "code":    de.code,
                "message": de.message,
                "details": de.details,
            })

        except Exception as exc:
            exit_code = 4
            log.exception("Unexpected error in discover-web")
            _emit({
                "type":    "error",
                "query":   query,
                "source":  engine,
                "code":    "UNEXPECTED_ERROR",
                "message": _sanitise_str(str(exc)),
                "details": {},
            })

        # Summary — always the final stdout line.
        # Includes persistence counters so OpenClaw / callers can track store state.
        # Counter semantics:
        #   returned_count    — results emitted this run (after URL-level dedupe)
        #   url_dupes_dropped — items the provider returned that were dropped as
        #                       duplicate URLs within this single run
        #   leads_inserted    — net-new records written to the store
        #   leads_updated     — existing store records merged / seen-counter bumped
        #   working_set_created — True when an active working set was written
        # Note: deduped_count is preserved as an alias for returned_count so
        # existing downstream parsers that read it are not broken.
        duration_ms = int((time.monotonic() - t_start) * 1000)
        _emit({
            "type":                "summary",
            "query":               query,
            "source":              engine,
            "requested_max":       max_results,
            "returned_count":      len(results),
            "url_dupes_dropped":   max(raw_count - len(results), 0),
            "leads_inserted":      _leads_inserted,
            "leads_updated":       _leads_updated,
            "working_set_created": bool(_persisted_lead_ids),
            "deduped_count":       len(results),   # kept for backwards compat
            "duration_ms":         duration_ms,
            "fetched_at":          fetched_at,
            "status":              "ok" if exit_code == 0 else "error",
        })
        summary_emitted = True

        # Optional file save — stderr only, never pollutes stdout.
        if save is not None and results:
            try:
                save.parent.mkdir(parents=True, exist_ok=True)
                save.write_text(
                    json.dumps(results, indent=2, ensure_ascii=False) + "\n",
                    encoding="utf-8",
                )
                log.info("Results saved to %s", save)
            except OSError as exc:
                log.error("Could not write results to %s: %s", save, exc)
                if exit_code == 0:
                    exit_code = 4

    except BaseException as _fatal:
        # Last-resort safety net: emit a minimal summary so stdout is never
        # empty and is always parseable as JSONL, then re-raise.
        if not summary_emitted:
            duration_ms = int((time.monotonic() - t_start) * 1000)
            _emit({
                "type":           "summary",
                "query":          query,
                "source":         engine,
                "requested_max":  max_results,
                "returned_count": 0,
                "deduped_count":  0,
                "duration_ms":    duration_ms,
                "fetched_at":     fetched_at,
                "status":         "error",
            })
        raise

    if exit_code != 0:
        raise typer.Exit(code=exit_code)


@app.command(
    help=(
        "ENRICHMENT — enrich school leads from website pages and generate a "
        "personalisation hook.\n\n"
        "Example: lumen-scout enrich --input data/leads.csv"
    ),
)
def enrich(
    input: Path = typer.Option(Path("data/leads.csv"), "--input", help="Path to the leads CSV."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Skip LLM calls; use placeholder text."),
) -> None:
    """Enrich leads from website pages and generate personalisation hook."""
    ensure_seeded_from_csv(input)
    leads = get_all_leads()

    llm = LLMService(dry_run=dry_run)
    updated: list[Lead] = []
    for lead in leads:
        enriched = enrich_lead(lead, llm)
        upsert_school_lead(enriched)
        updated.append(enriched)

    csv_path = export_legacy_schools_csv(input)
    typer.echo(f"Enriched {len(updated)} leads -> {csv_path}")


@app.command(
    help=(
        "DRAFT — generate initial outreach draft markdown files for school leads.\n\n"
        "Example: lumen-scout draft --input data/leads.csv --limit 10"
    ),
)
def draft(
    input: Path = typer.Option(Path("data/leads.csv"), "--input", help="Path to the leads CSV."),
    limit: int = typer.Option(10, "--limit", min=1, help="Maximum number of drafts to create."),
    delivery_mode: str = typer.Option(
        "manual", "--delivery-mode", help="Delivery mode (only 'manual' supported in v1)."
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Skip LLM calls; use placeholder text."),
) -> None:
    """Generate initial outreach draft markdown files."""
    if delivery_mode != "manual":
        raise typer.BadParameter("Only manual delivery-mode is supported in v1")

    ensure_seeded_from_csv(input)
    leads = get_all_leads()
    llm = LLMService(dry_run=dry_run)
    delivery = ManualDelivery()

    sorted_leads = sorted(leads, key=lambda lead: lead.contact_score, reverse=True)
    count = 0
    for lead in sorted_leads:
        if count >= limit:
            break
        method = lead.contact_method or "none"
        tier = lead.contact_priority_label or "Tier 5"
        if tier in {"Tier 1", "Tier 3"}:
            content = llm.email_draft(lead)
            output_path = Path("outreach_drafts") / f"{lead.lead_id}_email1.md"
            delivery.deliver(lead, content, output_path)
            lead.email1_path = str(output_path)
        elif tier == "Tier 4":
            content = llm.contact_form_draft(lead)
            output_path = Path("outreach_drafts") / f"{lead.lead_id}_contact_form.md"
            delivery.deliver(lead, content, output_path)
            lead.email1_path = str(output_path)
        elif tier == "Tier 5" and method == "phone_only":
            continue

        if tier in {"Tier 1", "Tier 2"} and lead.linkedin_url:
            linkedin_content = llm.linkedin_draft(lead)
            linkedin_output_path = Path("outreach_drafts") / f"{lead.lead_id}_linkedin.md"
            delivery.deliver(lead, linkedin_content, linkedin_output_path)

        if tier in {"Tier 1", "Tier 2"} and lead.contact_email and not lead.email1_path:
            email_content = llm.email_draft(lead)
            email_output_path = Path("outreach_drafts") / f"{lead.lead_id}_email1.md"
            delivery.deliver(lead, email_content, email_output_path)
            lead.email1_path = str(email_output_path)

        upsert_school_lead(lead)
        count += 1

    export_legacy_schools_csv(input)
    typer.echo(f"Created {count} outreach drafts in ./outreach_drafts")
    typer.echo("Next steps: review markdown drafts, personalise as needed, and send manually.")


@app.command(
    help=(
        "FOLLOW-UP — generate follow-up markdown drafts for school leads.\n\n"
        "Example: lumen-scout followup --days 5"
    ),
)
def followup(
    input: Path = typer.Option(Path("data/leads.csv"), "--input", help="Path to the leads CSV."),
    days: int = typer.Option(5, "--days", min=1, help="Days since the initial outreach was sent."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Skip LLM calls; use placeholder text."),
) -> None:
    """Generate follow-up markdown drafts."""
    ensure_seeded_from_csv(input)
    leads = get_all_leads()
    llm = LLMService(dry_run=dry_run)
    delivery = ManualDelivery()

    count = 0
    for lead in leads:
        content = llm.followup_draft(lead, days)
        output_path = Path("outreach_drafts") / f"{lead.lead_id}_followup_day{days}.md"
        delivery.deliver(lead, content, output_path)
        lead.followup_path = str(output_path)
        upsert_school_lead(lead)
        count += 1

    export_legacy_schools_csv(input)
    typer.echo(f"Created {count} follow-up drafts in ./outreach_drafts")
    typer.echo("Next steps: review follow-up markdown drafts and send manually.")


@app.command(
    help=(
        "CALL BRIEF — generate a custom call-brief markdown for a single school lead.\n\n"
        "Example: lumen-scout brief --lead-id abc123"
    ),
)
def brief(
    input: Path = typer.Option(Path("data/leads.csv"), "--input", help="Path to the leads CSV."),
    lead_id: str = typer.Option(..., "--lead-id", help="The lead_id to generate a brief for."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Skip LLM calls; use placeholder text."),
) -> None:
    """Generate a custom call brief markdown for one lead."""
    ensure_seeded_from_csv(input)
    leads = get_all_leads()
    target: Optional[Lead] = next((lead for lead in leads if lead.lead_id == lead_id), None)
    if not target:
        raise typer.BadParameter(f"Lead id not found: {lead_id}")

    llm = LLMService(dry_run=dry_run)
    content = llm.call_brief(target)
    output_path = Path("call_briefs") / f"{target.lead_id}.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content.strip() + "\n", encoding="utf-8")

    target.brief_path = str(output_path)
    upsert_school_lead(target)
    export_legacy_schools_csv(input)
    typer.echo(f"Created call brief: {output_path}")


@app.command(
    name="run",
    help=(
        "RUN — natural-language continuation for the active working set.\n\n"
        "Interprets a free-text phrase and dispatches to enrich, draft, or export\n"
        "using only the leads from the most recent discovery run.\n\n"
        "Examples:\n"
        '  lumen-scout run "enrich these"\n'
        '  lumen-scout run "write outreach for these"\n'
        '  lumen-scout run "export this list"\n'
    ),
)
def run(
    text: str = typer.Argument(
        ..., help="Natural-language instruction referencing the active working set."
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Skip LLM calls; use placeholder text."),
    limit: int = typer.Option(10, "--limit", min=1, help="Max drafts when intent is 'draft'."),
    export_path: Path = typer.Option(
        Path("data/working_set_export.csv"),
        "--export-path",
        help="Output CSV path when intent is 'export'.",
    ),
) -> None:
    """Dispatch a follow-up action against the active working set leads."""

    # --- resolve intent --------------------------------------------------
    intent = resolve_followup_intent(text)
    if intent is None:
        typer.echo(
            f"Could not recognise an intent in: \"{text}\"\n"
            "Supported intents: enrich, draft, export.\n"
            "Example: lumen-scout run \"enrich these\""
        )
        raise typer.Exit(code=2)

    # --- check for working set reference --------------------------------
    if not references_active_working_set(text):
        typer.echo(
            f"Phrase does not reference the active working set: \"{text}\"\n"
            "Include a reference like 'these', 'this list', or 'the results'.\n"
            "Example: lumen-scout run \"enrich these\""
        )
        raise typer.Exit(code=2)

    # --- confirm active working set exists --------------------------------
    if not has_active_working_set():
        typer.echo(
            "No active working set found. "
            "Run a discovery search first, then follow with enrich, draft, or export."
        )
        raise typer.Exit(code=2)

    ws = get_active_working_set()
    lead_ids = get_active_working_set_lead_ids()
    if not lead_ids:
        typer.echo(
            "Active working set exists but contains no leads. "
            "Run discovery again to populate it."
        )
        raise typer.Exit(code=2)

    typer.echo(
        f"Working set: {len(lead_ids)} leads "
        f"(query: \"{ws.get('query') or 'n/a'}\", "
        f"source: {ws.get('source_command', 'unknown')})"
    )

    leads = get_leads_by_ids(lead_ids)
    if not leads:
        typer.echo(
            f"Working set references {len(lead_ids)} lead ID(s) "
            "but none were found in the store. The store may have been reset."
        )
        raise typer.Exit(code=2)

    # --- dispatch ---------------------------------------------------------
    if intent == "enrich":
        typer.echo(f"Enriching {len(leads)} leads from working set…")
        count = _enrich_leads(leads, dry_run=dry_run)
        typer.echo(f"Enriched {count} leads.")

    elif intent == "draft":
        typer.echo(f"Drafting outreach for up to {limit} leads from working set…")
        count = _draft_leads(leads, limit=limit, dry_run=dry_run)
        typer.echo(f"Created {count} outreach draft(s) in ./outreach_drafts")

    elif intent == "export":
        typer.echo(f"Exporting {len(leads)} leads from working set to {export_path}…")
        written = _export_leads(leads, path=export_path)
        typer.echo(f"Exported {len(leads)} leads -> {written}")

    elif intent == "rank":
        typer.echo(f"Ranking {len(leads)} leads from working set…")
        ranked = _rank_leads(leads)
        _print_ranking_summary(ranked, version="v1")


@app.command(
    name="rank",
    help=(
        "RANK — score and prioritize the active working set leads.\n\n"
        "Applies a deterministic, rule-based scorer (v1) to each lead in the\n"
        "active working set, persists the ranking, and prints a summary.\n\n"
        "Example: lumen-scout rank"
    ),
)
def rank(
    version: str = typer.Option("v1", "--version", help="Scoring version tag written to ranked_at metadata."),
    top: int = typer.Option(5, "--top", min=1, help="Number of top leads to show in the summary."),
) -> None:
    """Score and persist rankings for the active working set."""
    _reconfigure_logging_to_stderr()

    if not has_active_working_set():
        typer.echo(
            "No active working set found. "
            "Run a discovery search first, then follow with enrich, draft, or export."
        )
        raise typer.Exit(code=2)

    lead_ids = get_active_working_set_lead_ids()
    if not lead_ids:
        typer.echo(
            "Active working set exists but contains no leads. "
            "Run discovery again to populate it."
        )
        raise typer.Exit(code=2)

    leads = get_leads_by_ids(lead_ids)
    if not leads:
        typer.echo(
            f"Working set references {len(lead_ids)} lead ID(s) "
            "but none were found in the store. The store may have been reset."
        )
        raise typer.Exit(code=2)

    ranked = _rank_leads(leads, version=version)
    _print_ranking_summary(ranked, version=version, top=top)


@app.command(
    name="capabilities",
    help="Print a short summary of lumen-scout capabilities with example invocations.",
)
def capabilities() -> None:
    """Print a short capabilities summary with example invocations."""
    typer.echo(
        """
\u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510
\u2502                         lumen-scout capabilities                          \u2502
\u251c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u252c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2524
\u2502 Mode             \u2502 Description                                            \u2502
\u251c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u253c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2524
\u2502 discover         \u2502 School pipeline \u2014 find private K-12 schools,           \u2502
\u2502                  \u2502 upsert leads, export CSV.                              \u2502
\u2502                  \u2502                                                        \u2502
\u2502                  \u2502 Example:                                               \u2502
\u2502                  \u2502   lumen-scout discover --city "Downey, CA" --max 25    \u2502
\u251c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u253c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2524
\u2502 discover-web     \u2502 Generic web discovery \u2014 any free-text query.           \u2502
\u2502                  \u2502 stdout = strict JSONL (result + summary records).      \u2502
\u2502                  \u2502 stderr = logs / progress (never mixed into stdout).    \u2502
\u2502                  \u2502                                                        \u2502
\u2502                  \u2502 Record types on stdout:                                \u2502
\u2502                  \u2502   result  \u2014 id, query, rank, title, url, snippet,      \u2502
\u2502                  \u2502             source, fetched_at                         \u2502
\u2502                  \u2502   error   \u2014 code, message, details (on failure)        \u2502
\u2502                  \u2502   summary \u2014 always last; status="ok"|"error"           \u2502
\u2502                  \u2502                                                        \u2502
\u2502                  \u2502 Examples:                                              \u2502
\u2502                  \u2502   lumen-scout discover-web                             \u2502
\u2502                  \u2502     "real estate investing in Downey" --max 5          \u2502
\u2502                  \u2502   lumen-scout discover-web "tutoring centers"          \u2502
\u2502                  \u2502     --max 10 --location "Los Angeles"                  \u2502
\u2502                  \u2502     --save results.json                                \u2502
\u2502                  \u2502                                                        \u2502
\u2502                  \u2502 Exit codes:                                            \u2502
\u2502                  \u2502   0=ok  2=bad input  3=config  4=network  5=quota      \u2502
\u251c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u253c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2524
\u2502 enrich           \u2502 Scrape school websites; generate outreach hooks.       \u2502
\u2502 draft            \u2502 Create email / LinkedIn / contact-form drafts.         \u2502
\u2502 followup         \u2502 Create follow-up drafts (N days after outreach).       \u2502
\u2502 brief            \u2502 Create a call-brief for one specific lead.             \u2502
\u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2534\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518

Run `lumen-scout <command> --help` for full flag documentation.
"""
    )


if __name__ == "__main__":
    app()