from __future__ import annotations

import csv
import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, NamedTuple, Optional
from uuid import uuid4

from .models import CSV_FIELDS, Lead


class UpsertResult(NamedTuple):
    """
    Returned by upsert_lead() so callers can track insert/update counts
    without touching private store internals.

    status values:
      "inserted" — row did not exist; new record created.
      "updated"  — row already existed; fields merged / counters incremented.
    """
    status: Literal["inserted", "updated"]

DEFAULT_DB_PATH = Path(r"C:\Users\danie\dev\scout-data\scout.db")
DATA_PATH = Path("data/leads.csv")


# Fields stored as top-level columns in the leads table.
# Everything else that needs preserving spills into extras_json.
SCALAR_FIELDS = {
    "lead_id",
    "website",
    "domain",
    "provider",
    "source_query",
    "address",
    "phone",
    "contact_email",
    "contact_role",
    "all_emails",
    "primary_contact",
    "linkedin_url",
    "contact_form_url",
    "contact_page",
    "contact_method",
    "contact_score",
    "contact_priority_label",
    "about_page",
    "about_page_url",
    "staff_page_url",
    "personalization_hook",
    "enriched_at",
    "email1_path",
    "followup_path",
    "brief_path",
    "notes",
}


CANONICAL_COLUMNS = [
    "lead_id",
    "entity_key",
    "lead_type",
    "name",
    "website",
    "domain",
    "provider",
    "source_query",
    "address",
    "phone",
    "contact_email",
    "contact_role",
    "all_emails",
    "primary_contact",
    "linkedin_url",
    "contact_form_url",
    "contact_page",
    "contact_method",
    "contact_score",
    "contact_priority_label",
    "about_page",
    "about_page_url",
    "staff_page_url",
    "personalization_hook",
    "enriched_at",
    "email1_path",
    "followup_path",
    "brief_path",
    "notes",
    "extras_json",
    # Persistence metadata columns (new)
    "first_seen_at",
    "last_seen_at",
    "times_seen",
    # Ranking metadata columns
    "lead_score",
    "lead_score_label",
    "lead_score_reasons",
    "ranking_version",
    "ranked_at",
    "updated_at",
]


def get_db_path() -> Path:
    configured = os.getenv("SCOUT_DB_PATH")
    return Path(configured) if configured else DEFAULT_DB_PATH


def _connect() -> sqlite3.Connection:
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_store() -> None:
    """
    Create (or migrate) the canonical leads table plus working set tables.

    Migration strategy: ALTER TABLE ADD COLUMN is safe to re-run because each
    statement is wrapped in a try/except that ignores "duplicate column" errors.
    New tables use CREATE TABLE IF NOT EXISTS, so they are also safe to re-run.
    New columns are appended; nothing existing is touched.
    """
    with _connect() as conn:
        # Base table — identical to the original schema.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS leads (
                lead_id TEXT PRIMARY KEY,
                entity_key TEXT,
                name TEXT,
                website TEXT,
                domain TEXT,
                provider TEXT,
                source_query TEXT,
                address TEXT,
                phone TEXT,
                contact_email TEXT,
                contact_role TEXT,
                all_emails TEXT,
                primary_contact TEXT,
                linkedin_url TEXT,
                contact_form_url TEXT,
                contact_page TEXT,
                contact_method TEXT,
                contact_score INTEGER DEFAULT 0,
                contact_priority_label TEXT,
                about_page TEXT,
                about_page_url TEXT,
                staff_page_url TEXT,
                personalization_hook TEXT,
                enriched_at TEXT,
                email1_path TEXT,
                followup_path TEXT,
                brief_path TEXT,
                notes TEXT,
                extras_json TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_leads_entity_key ON leads(entity_key)"
        )

        # Additive migrations — safe to run against an already-populated DB.
        _add_column_if_missing(conn, "leads", "lead_type",     "TEXT DEFAULT 'school'")
        _add_column_if_missing(conn, "leads", "first_seen_at", "TEXT")
        _add_column_if_missing(conn, "leads", "last_seen_at",  "TEXT")
        _add_column_if_missing(conn, "leads", "times_seen",    "INTEGER DEFAULT 0")

        # Ranking metadata columns — additive, safe to re-run.
        _add_column_if_missing(conn, "leads", "lead_score",         "INTEGER NOT NULL DEFAULT 0")
        _add_column_if_missing(conn, "leads", "lead_score_label",   "TEXT")
        _add_column_if_missing(conn, "leads", "lead_score_reasons", "TEXT")
        _add_column_if_missing(conn, "leads", "ranking_version",    "TEXT")
        _add_column_if_missing(conn, "leads", "ranked_at",          "TEXT")

        # ------------------------------------------------------------------ #
        # Working set tables                                                   #
        # ------------------------------------------------------------------ #

        # working_sets: one row per discovery run that created a named result set.
        # is_active = 1 means "current"; only one should be active at a time.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS working_sets (
                id             TEXT PRIMARY KEY,
                query          TEXT,
                source_command TEXT NOT NULL,
                lead_type      TEXT,
                created_at     TEXT NOT NULL,
                is_active      INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        # Fast lookup of the current active set (the common query path).
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_working_sets_active_created
            ON working_sets (is_active, created_at)
            """
        )

        # working_set_items: ordered lead IDs belonging to a working set.
        # Stores only lead_id references — no lead payload duplication.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS working_set_items (
                working_set_id TEXT    NOT NULL,
                lead_id        TEXT    NOT NULL,
                position       INTEGER,
                PRIMARY KEY (working_set_id, lead_id)
            )
            """
        )
        # Ordered retrieval of items for a given set.
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_working_set_items_set_position
            ON working_set_items (working_set_id, position)
            """
        )

        conn.commit()


def _add_column_if_missing(
    conn: sqlite3.Connection, table: str, column: str, definition: str
) -> None:
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
    except sqlite3.OperationalError:
        pass  # Column already exists — nothing to do.


# ---------------------------------------------------------------------------
# Entity-key / dedupe logic
# ---------------------------------------------------------------------------

def _entity_key(lead: Lead) -> str:
    """
    Stable dedupe identity for a lead. Priority order:

    1. Normalised domain  (most reliable; domain:example.com)
    2. Normalised website (fallback when domain not extracted yet; web:example.com/path)
    3. name + city        (school-style legacy fallback; name_city:acme::portland)
    4. name only          (generic leads that have no location; name:acme inc)

    The prefix keeps buckets non-overlapping so a domain and a name that happen
    to stringify identically are never conflated.
    """
    if lead.domain:
        return f"domain:{lead.domain.strip().lower()}"

    if lead.website:
        # Strip protocol and trailing slash for stability.
        raw = lead.website.strip().lower()
        raw = raw.removeprefix("https://").removeprefix("http://").rstrip("/")
        return f"web:{raw}"

    display = (lead.name or lead.school_name or "").strip().lower()

    if lead.city:
        return f"name_city:{display}::{lead.city.strip().lower()}"

    return f"name:{display}"


# ---------------------------------------------------------------------------
# Row mapping helpers
# ---------------------------------------------------------------------------

def _lead_to_record(lead: Lead) -> dict[str, Any]:
    """
    Map a Lead onto the flat column dict that goes into the DB.

    Fields that don't have a dedicated column are preserved in extras_json so
    round-trips through _row_to_lead() are lossless.
    """
    # Spill non-column fields + school compat fields into extras.
    extras: dict[str, Any] = {}

    # Always persist city and school_name in extras for backwards compat.
    if lead.city:
        extras["city"] = lead.city
    if lead.school_name:
        extras["school_name"] = lead.school_name

    # Any CSV field that isn't a dedicated scalar column lands in extras.
    column_set = set(CANONICAL_COLUMNS) | SCALAR_FIELDS
    for field in CSV_FIELDS:
        if field in column_set or field in {"city", "school_name"}:
            continue
        value = getattr(lead, field, None)
        if value not in (None, ""):
            extras[field] = value

    return {
        "lead_id":               lead.lead_id,
        "entity_key":            _entity_key(lead),
        "lead_type":             lead.lead_type,
        # Use the generic .name property (validator guarantees it is set).
        "name":                  lead.name,
        "website":               lead.website,
        "domain":                lead.domain,
        "provider":              lead.provider,
        "source_query":          lead.source_query,
        "address":               lead.address,
        "phone":                 lead.phone,
        "contact_email":         lead.contact_email,
        "contact_role":          lead.contact_role,
        "all_emails":            lead.all_emails,
        "primary_contact":       lead.primary_contact,
        "linkedin_url":          lead.linkedin_url,
        "contact_form_url":      lead.contact_form_url,
        "contact_page":          lead.contact_page,
        "contact_method":        lead.contact_method,
        "contact_score":         lead.contact_score,
        "contact_priority_label": lead.contact_priority_label,
        "about_page":            lead.about_page,
        "about_page_url":        lead.about_page_url,
        "staff_page_url":        lead.staff_page_url,
        "personalization_hook":  lead.personalization_hook,
        "enriched_at":           lead.enriched_at,
        "email1_path":           lead.email1_path,
        "followup_path":         lead.followup_path,
        "brief_path":            lead.brief_path,
        "notes":                 lead.notes,
        "extras_json":           json.dumps(extras, ensure_ascii=False),
        # Persistence metadata — upsert logic fills these in at write time.
        "first_seen_at":         lead.first_seen_at,
        "last_seen_at":          lead.last_seen_at,
        "times_seen":            lead.times_seen,
        # Ranking metadata — written as-is; scorer sets these via mark_ranked().
        "lead_score":            lead.lead_score,
        "lead_score_label":      lead.lead_score_label,
        "lead_score_reasons":    lead.lead_score_reasons,
        "ranking_version":       lead.ranking_version,
        "ranked_at":             lead.ranked_at,
    }


def _row_to_lead(row: sqlite3.Row) -> Lead:
    """Reconstruct a Lead from a DB row, merging extras_json for lossless round-trip."""
    extras = json.loads(row["extras_json"] or "{}")

    # Recover school compat fields from extras (written by _lead_to_record).
    city        = extras.get("city") or ""
    school_name = extras.get("school_name") or row["name"] or ""

    raw: dict[str, Any] = {field: None for field in CSV_FIELDS}
    raw["lead_id"]     = row["lead_id"]
    raw["school_name"] = school_name
    raw["city"]        = city
    raw["name"]        = row["name"]

    # Pull lead_type, persistence metadata, and ranking metadata if the
    # columns exist in this row (older DBs may not have them yet).
    for col in (
        "lead_type",
        "first_seen_at", "last_seen_at", "times_seen",
        "lead_score", "lead_score_label", "lead_score_reasons",
        "ranking_version", "ranked_at",
    ):
        try:
            raw[col] = row[col]
        except IndexError:
            pass  # Column not yet present in older DB — leave as model default.

    for field in SCALAR_FIELDS:
        if field in row.keys():
            raw[field] = row[field]

    # extras_json fills any gap not covered by top-level columns.
    for field in CSV_FIELDS:
        if raw.get(field) in (None, "") and field in extras:
            raw[field] = extras[field]

    if raw.get("contact_score") in (None, ""):
        raw["contact_score"] = 0
    if raw.get("lead_score") in (None, ""):
        raw["lead_score"] = 0

    return Lead.model_validate(raw)


# ---------------------------------------------------------------------------
# Core upsert
# ---------------------------------------------------------------------------

def upsert_lead(lead: Lead) -> UpsertResult:
    """
    Generic upsert for any lead type.

    INSERT behaviour  (first discovery):
      • Writes all fields.
      • Sets first_seen_at = last_seen_at = now, times_seen = 1.

    UPDATE behaviour  (rediscovery / enrichment):
      • Increments times_seen, refreshes last_seen_at.
      • Uses COALESCE so existing non-null values are never overwritten by
        weaker/null incoming values — enrichment always wins over blanks.
      • first_seen_at is never overwritten (preserved from first insert).

    Returns an UpsertResult indicating whether the lead was "inserted" or
    "updated".  Callers should use this instead of inspecting store internals.
    """
    ensure_store()
    now     = datetime.utcnow().isoformat()
    payload = _lead_to_record(lead)

    # Inject timestamps into the insert payload.
    payload.setdefault("first_seen_at", now)
    payload["last_seen_at"] = now
    payload.setdefault("times_seen", 1)

    insert_cols = [col for col in CANONICAL_COLUMNS if col != "updated_at"]

    # On conflict: merge non-null incoming values, never blank out enriched data.
    assignments = []
    for col in insert_cols:
        if col in {"lead_id", "entity_key", "first_seen_at"}:
            # lead_id / entity_key are immutable; first_seen_at preserved forever.
            continue
        if col == "times_seen":
            assignments.append("times_seen = leads.times_seen + 1")
        else:
            assignments.append(f"{col} = COALESCE(excluded.{col}, leads.{col})")

    with _connect() as conn:
        # Check pre-existence using the stable entity_key before we write.
        # This is the single correct place to do this check — callers must not
        # replicate it using private store helpers.
        key = _entity_key(lead)
        already_exists = conn.execute(
            "SELECT 1 FROM leads WHERE entity_key = ?", (key,)
        ).fetchone() is not None

        conn.execute(
            f"""
            INSERT INTO leads ({", ".join(insert_cols)})
            VALUES ({", ".join('?' for _ in insert_cols)})
            ON CONFLICT(entity_key) DO UPDATE SET
                {", ".join(assignments)},
                last_seen_at = excluded.last_seen_at,
                updated_at   = CURRENT_TIMESTAMP
            """,
            [payload.get(col) for col in insert_cols],
        )
        conn.commit()

    return UpsertResult(status="updated" if already_exists else "inserted")


def upsert_school_lead(lead: Lead) -> UpsertResult:
    """
    Compatibility wrapper — existing callers keep working unchanged.
    Internally delegates to the generic upsert_lead() and propagates its result.
    """
    return upsert_lead(lead)


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

def get_all_leads() -> list[Lead]:
    ensure_store()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM leads ORDER BY updated_at DESC, name ASC"
        ).fetchall()
    return [_row_to_lead(row) for row in rows]


def get_leads_by_type(lead_type: str) -> list[Lead]:
    """Return only leads of a specific type (e.g. 'school' or 'generic')."""
    ensure_store()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM leads WHERE lead_type = ? ORDER BY updated_at DESC, name ASC",
            (lead_type,),
        ).fetchall()
    return [_row_to_lead(row) for row in rows]


def get_leads_by_ids(lead_ids: list[str]) -> list[Lead]:
    """
    Return Lead objects for the given lead_ids, preserving input order.

    IDs that do not exist in the store are silently omitted.
    Returns [] immediately if lead_ids is empty (avoids a no-op DB round-trip).
    """
    if not lead_ids:
        return []

    ensure_store()

    # Parameterised IN (...) query — one placeholder per ID.
    placeholders = ", ".join("?" for _ in lead_ids)
    with _connect() as conn:
        rows = conn.execute(
            f"SELECT * FROM leads WHERE lead_id IN ({placeholders})",
            lead_ids,
        ).fetchall()

    # Build a map so we can restore caller-specified order regardless of
    # whatever order SQLite returns the rows in.
    row_map = {row["lead_id"]: _row_to_lead(row) for row in rows}
    return [row_map[lid] for lid in lead_ids if lid in row_map]


def get_top_ranked_leads(limit: int = 10) -> list[Lead]:
    """
    Return the highest-scored leads ordered by ranking quality.

    Ordering:
      1. lead_score DESC          — primary: highest score first
      2. ranked_at DESC           — secondary: most recently ranked (NULLs last,
                                    emulated by CASE since SQLite lacks NULLS LAST)
      3. last_seen_at DESC        — tertiary: tie-break by recency

    Unranked leads (lead_score = 0, ranked_at IS NULL) naturally sink to the
    bottom of the list. limit defaults to 10 but any positive integer is valid.
    """
    ensure_store()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM   leads
            ORDER  BY
                lead_score DESC,
                CASE WHEN ranked_at IS NULL THEN 0 ELSE 1 END DESC,
                ranked_at DESC,
                last_seen_at DESC,
                name ASC
            LIMIT  ?
            """,
            (limit,),
        ).fetchall()
    return [_row_to_lead(row) for row in rows]


# ---------------------------------------------------------------------------
# Legacy CSV export / import
# ---------------------------------------------------------------------------

def export_legacy_schools_csv(path: Path = DATA_PATH) -> Path:
    """
    Export school leads to CSV in the original format.

    Only rows where lead_type = 'school' are included, so generic leads
    don't silently pollute the school pipeline's CSV. This is deliberate:
    if you want generic leads exported, call a separate export function.
    """
    # Filter to school leads only for backwards compat.
    leads = get_leads_by_type("school")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for lead in leads:
            writer.writerow(lead.model_dump())
    return path


def import_legacy_csv(path: Path = DATA_PATH) -> int:
    """
    Import a legacy school CSV. Rows are validated as Lead objects and upserted.
    school_name / city columns in the CSV are handled by the model validator.
    """
    if not path.exists():
        return 0
    with path.open("r", newline="", encoding="utf-8") as f:
        rows = [Lead.model_validate(row) for row in csv.DictReader(f)]

    for lead in rows:
        upsert_lead(lead)
    return len(rows)


def ensure_seeded_from_csv(path: Path = DATA_PATH) -> None:
    if get_all_leads():
        return
    import_legacy_csv(path)


# ---------------------------------------------------------------------------
# Working set API
# ---------------------------------------------------------------------------
# A "working set" is the ordered list of lead IDs produced by the most recent
# discovery run.  Downstream operations (enrich, draft, followup, brief) can
# resolve the active working set to operate on exactly those leads without
# re-querying or re-serialising full Lead objects.
#
# Design decisions:
#   • Only lead_id references are stored — no payload duplication.
#   • At most one working set is active at a time.  create_working_set()
#     atomically deactivates any prior active sets before writing the new one.
#   • Works for both school and generic leads (no lead_type assumption).
#   • All functions call ensure_store() so callers never need to pre-initialise.
# ---------------------------------------------------------------------------

def clear_active_working_sets() -> None:
    """
    Mark every currently-active working set as inactive.

    Idempotent — safe to call when no active set exists.
    """
    ensure_store()
    with _connect() as conn:
        conn.execute(
            "UPDATE working_sets SET is_active = 0 WHERE is_active = 1"
        )
        conn.commit()


def create_working_set(
    lead_ids: list[str],
    query: Optional[str],
    source_command: str,
    lead_type: Optional[str],
) -> str:
    """
    Persist a new active working set and return its ID.

    Steps (all within a single transaction):
      1. Deactivate any currently-active working sets.
      2. Insert one row into working_sets (is_active = 1).
      3. Insert one row per lead_id into working_set_items, preserving order
         via the position column (0-based).

    Args:
        lead_ids:       Ordered list of lead_id values from the discovery run.
        query:          The search query that produced these leads (may be None).
        source_command: CLI command name that created this set, e.g. "discover"
                        or "discover-web".
        lead_type:      Category hint ("school", "generic", or None for mixed).

    Returns:
        The new working set ID (UUID string).
    """
    ensure_store()
    ws_id      = str(uuid4())
    created_at = datetime.utcnow().isoformat()

    with _connect() as conn:
        # Deactivate prior sets atomically.
        conn.execute(
            "UPDATE working_sets SET is_active = 0 WHERE is_active = 1"
        )

        conn.execute(
            """
            INSERT INTO working_sets (id, query, source_command, lead_type, created_at, is_active)
            VALUES (?, ?, ?, ?, ?, 1)
            """,
            (ws_id, query, source_command, lead_type, created_at),
        )

        # Insert items with explicit position so ORDER BY position ASC is stable.
        conn.executemany(
            """
            INSERT INTO working_set_items (working_set_id, lead_id, position)
            VALUES (?, ?, ?)
            """,
            [(ws_id, lead_id, pos) for pos, lead_id in enumerate(lead_ids)],
        )

        conn.commit()

    return ws_id


def get_active_working_set() -> Optional[dict[str, Any]]:
    """
    Return the most recently created active working set as a plain dict,
    or None if no active set exists.

    The returned dict contains: id, query, source_command, lead_type,
    created_at, is_active.  It does not include the item list — use
    get_working_set_lead_ids() for that.
    """
    ensure_store()
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT id, query, source_command, lead_type, created_at, is_active
            FROM   working_sets
            WHERE  is_active = 1
            ORDER  BY created_at DESC
            LIMIT  1
            """
        ).fetchone()

    if row is None:
        return None

    return {
        "id":             row["id"],
        "query":          row["query"],
        "source_command": row["source_command"],
        "lead_type":      row["lead_type"],
        "created_at":     row["created_at"],
        "is_active":      bool(row["is_active"]),
    }


def get_working_set_lead_ids(working_set_id: str) -> list[str]:
    """
    Return the ordered lead IDs for a specific working set.

    Ordering is by position ASC, matching the original discovery order.
    Returns [] if the working set ID does not exist or has no items.
    """
    ensure_store()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT lead_id
            FROM   working_set_items
            WHERE  working_set_id = ?
            ORDER  BY position ASC
            """,
            (working_set_id,),
        ).fetchall()

    return [row["lead_id"] for row in rows]


def get_active_working_set_lead_ids() -> list[str]:
    """
    Resolve the active working set and return its lead IDs in discovery order.

    Returns [] if no active working set exists.
    Equivalent to:
        ws = get_active_working_set()
        return get_working_set_lead_ids(ws["id"]) if ws else []
    """
    ws = get_active_working_set()
    if ws is None:
        return []
    return get_working_set_lead_ids(ws["id"])


def has_active_working_set() -> bool:
    """
    Return True if at least one active working set exists, False otherwise.

    Cheaper than get_active_working_set() when the caller only needs to
    branch on existence without reading the set metadata.
    """
    ensure_store()
    with _connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM working_sets WHERE is_active = 1 LIMIT 1"
        ).fetchone()
    return row is not None