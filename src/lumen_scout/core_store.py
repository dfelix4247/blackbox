from __future__ import annotations

import csv
import json
import os
import sqlite3
from pathlib import Path
from typing import Any

from .models import CSV_FIELDS, Lead

DEFAULT_DB_PATH = Path(r"C:\Users\danie\dev\scout-data\scout.db")
DATA_PATH = Path("data/leads.csv")


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
    with _connect() as conn:
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
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_leads_entity_key ON leads(entity_key)")
        conn.commit()


def _entity_key(lead: Lead) -> str:
    if lead.domain:
        return f"domain:{lead.domain.lower()}"
    return f"name_city:{lead.school_name.strip().lower()}::{lead.city.strip().lower()}"


def _lead_to_record(lead: Lead) -> dict[str, Any]:
    extras: dict[str, Any] = {"city": lead.city, "school_name": lead.school_name}
    for field in CSV_FIELDS:
        if field in SCALAR_FIELDS or field in {"school_name", "city"}:
            continue
        value = getattr(lead, field)
        if value not in (None, ""):
            extras[field] = value

    return {
        "lead_id": lead.lead_id,
        "entity_key": _entity_key(lead),
        "name": lead.school_name,
        "website": lead.website,
        "domain": lead.domain,
        "provider": lead.provider,
        "source_query": lead.source_query,
        "address": lead.address,
        "phone": lead.phone,
        "contact_email": lead.contact_email,
        "contact_role": lead.contact_role,
        "all_emails": lead.all_emails,
        "primary_contact": lead.primary_contact,
        "linkedin_url": lead.linkedin_url,
        "contact_form_url": lead.contact_form_url,
        "contact_page": lead.contact_page,
        "contact_method": lead.contact_method,
        "contact_score": lead.contact_score,
        "contact_priority_label": lead.contact_priority_label,
        "about_page": lead.about_page,
        "about_page_url": lead.about_page_url,
        "staff_page_url": lead.staff_page_url,
        "personalization_hook": lead.personalization_hook,
        "enriched_at": lead.enriched_at,
        "email1_path": lead.email1_path,
        "followup_path": lead.followup_path,
        "brief_path": lead.brief_path,
        "notes": lead.notes,
        "extras_json": json.dumps(extras, ensure_ascii=False),
    }


def upsert_school_lead(lead: Lead) -> None:
    ensure_store()
    payload = _lead_to_record(lead)
    columns = [col for col in CANONICAL_COLUMNS if col != "updated_at"]

    assignments = []
    for col in columns:
        if col in {"lead_id", "entity_key"}:
            continue
        assignments.append(f"{col}=COALESCE(excluded.{col}, leads.{col})")

    with _connect() as conn:
        conn.execute(
            f"""
            INSERT INTO leads ({", ".join(columns)})
            VALUES ({", ".join('?' for _ in columns)})
            ON CONFLICT(entity_key) DO UPDATE SET
                {", ".join(assignments)},
                updated_at=CURRENT_TIMESTAMP
            """,
            [payload.get(col) for col in columns],
        )
        conn.commit()


def _row_to_lead(row: sqlite3.Row) -> Lead:
    extras = json.loads(row["extras_json"] or "{}")
    city = extras.get("city") or ""
    school_name = row["name"] or extras.get("school_name") or ""

    raw: dict[str, Any] = {field: None for field in CSV_FIELDS}
    raw["lead_id"] = row["lead_id"]
    raw["school_name"] = school_name
    raw["city"] = city

    for field in SCALAR_FIELDS:
        if field in row.keys():
            raw[field] = row[field]

    for field in CSV_FIELDS:
        if raw.get(field) in (None, "") and field in extras:
            raw[field] = extras[field]

    if raw.get("contact_score") in (None, ""):
        raw["contact_score"] = 0

    return Lead.model_validate(raw)


def get_all_leads() -> list[Lead]:
    ensure_store()
    with _connect() as conn:
        rows = conn.execute("SELECT * FROM leads ORDER BY updated_at DESC, name ASC").fetchall()
    return [_row_to_lead(row) for row in rows]


def export_legacy_schools_csv(path: Path = DATA_PATH) -> Path:
    leads = get_all_leads()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for lead in leads:
            writer.writerow(lead.model_dump())
    return path


def import_legacy_csv(path: Path = DATA_PATH) -> int:
    if not path.exists():
        return 0
    with path.open("r", newline="", encoding="utf-8") as f:
        rows = [Lead.model_validate(row) for row in csv.DictReader(f)]

    for lead in rows:
        upsert_school_lead(lead)
    return len(rows)


def ensure_seeded_from_csv(path: Path = DATA_PATH) -> None:
    if get_all_leads():
        return
    import_legacy_csv(path)
