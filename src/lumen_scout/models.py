from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator


# Supported lead categories. "school" preserves the existing pipeline;
# "generic" covers future discovery results for any business / org.
LeadType = Literal["school", "generic"]


class Lead(BaseModel):
    # ------------------------------------------------------------------ #
    # Identity                                                             #
    # ------------------------------------------------------------------ #
    lead_id: str = Field(default_factory=lambda: str(uuid4()))

    # Generic display name — the canonical name field going forward.
    # May be left None when loading legacy school rows (school_name fills in).
    name: Optional[str] = None

    # Legacy school field — kept for backwards compatibility.
    # Still accepted on ingest; do not remove until school pipeline migrates.
    school_name: Optional[str] = None

    # Lead category. Defaults to "school" so existing records keep their
    # semantics without requiring a migration.
    lead_type: LeadType = "school"

    # ------------------------------------------------------------------ #
    # Location (no longer universally required)                            #
    # ------------------------------------------------------------------ #
    # city was previously required; now Optional so generic leads validate.
    city: Optional[str] = None

    # ------------------------------------------------------------------ #
    # Discovery / contact fields (unchanged)                               #
    # ------------------------------------------------------------------ #
    website: Optional[str] = None
    domain: Optional[str] = None
    provider: Optional[str] = None
    source_query: Optional[str] = None
    address: Optional[str] = None
    phone: Optional[str] = None
    contact_email: Optional[str] = None
    contact_role: Optional[str] = None
    all_emails: Optional[str] = None
    primary_contact: Optional[str] = None
    linkedin_url: Optional[str] = None
    contact_form_url: Optional[str] = None
    contact_page: Optional[str] = None
    contact_method: Optional[str] = None
    contact_score: int = 0
    contact_priority_label: Optional[str] = None
    about_page: Optional[str] = None
    about_page_url: Optional[str] = None
    staff_page_url: Optional[str] = None
    personalization_hook: Optional[str] = None
    enriched_at: Optional[str] = None
    email1_path: Optional[str] = None
    followup_path: Optional[str] = None
    brief_path: Optional[str] = None
    notes: Optional[str] = None

    # ------------------------------------------------------------------ #
    # Persistence metadata (new — supports deduplication across runs)      #
    # ------------------------------------------------------------------ #
    first_seen_at: Optional[str] = None
    last_seen_at: Optional[str] = None
    times_seen: int = 0

    # ------------------------------------------------------------------ #
    # Ranking metadata                                                     #
    # ------------------------------------------------------------------ #
    # Numeric priority score — higher is better. Populated by the ranking
    # step; defaults to 0 so unranked leads sort consistently.
    lead_score: int = 0

    # Human-readable bucket derived from lead_score, e.g. "high", "medium",
    # "low". Not enforced by the model in v1 — the scorer sets this freely.
    lead_score_label: Optional[str] = None

    # Plain-text explanation of why the lead received its score.
    # Example: "has contact email; has personalization hook; has domain"
    lead_score_reasons: Optional[str] = None

    # Identifies which scoring logic version produced the current score,
    # e.g. "v1". Useful for invalidating stale scores after logic changes.
    ranking_version: Optional[str] = None

    # ISO timestamp of the most recent scoring run for this lead.
    ranked_at: Optional[str] = None

    # ------------------------------------------------------------------ #
    # Cross-field compatibility                                            #
    # ------------------------------------------------------------------ #
    @model_validator(mode="after")
    def _sync_name_and_school_name(self) -> "Lead":
        """
        Bidirectional fallback between `name` and `school_name`:

        1. Legacy school row arrives with school_name but no name
           → populate name from school_name so downstream code can always
             read .name regardless of lead_type.

        2. Generic school lead arrives with name but no school_name
           → back-fill school_name so any existing school-pipeline code
             that reads .school_name still works.

        3. Neither field is set → raise, because every lead needs a display name.
        """
        if not self.name and self.school_name:
            # Case 1: legacy row
            object.__setattr__(self, "name", self.school_name)
        elif self.name and not self.school_name and self.lead_type == "school":
            # Case 2: new-style school lead
            object.__setattr__(self, "school_name", self.name)
        elif not self.name and not self.school_name:
            raise ValueError(
                "A Lead must have at least one of 'name' or 'school_name'."
            )
        return self

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #
    def mark_enriched(self) -> None:
        self.enriched_at = datetime.utcnow().isoformat()

    def mark_seen(self) -> None:
        """Call each time this lead is encountered in a discovery run."""
        now = datetime.utcnow().isoformat()
        if not self.first_seen_at:
            self.first_seen_at = now
        self.last_seen_at = now
        self.times_seen += 1

    def mark_ranked(
        self,
        score: int,
        label: Optional[str] = None,
        reasons: Optional[str] = None,
        version: Optional[str] = None,
    ) -> None:
        """Record a ranking result on the lead.

        Mirrors the pattern of mark_enriched() — a single call keeps all
        ranking fields consistent and stamps ranked_at automatically.
        """
        self.lead_score = score
        self.lead_score_label = label
        self.lead_score_reasons = reasons
        self.ranking_version = version
        self.ranked_at = datetime.utcnow().isoformat()

    @property
    def display_name(self) -> str:
        """Always returns a human-readable name regardless of lead_type."""
        return self.name or self.school_name or ""


# CSV_FIELDS intentionally preserves the original field order so existing
# CSV writers/readers keep working. New fields are appended at the end.
_LEGACY_FIELDS = [
    "lead_id", "school_name", "city", "website", "domain", "provider",
    "source_query", "address", "phone", "contact_email", "contact_role",
    "all_emails", "primary_contact", "linkedin_url", "contact_form_url",
    "contact_page", "contact_method", "contact_score", "contact_priority_label",
    "about_page", "about_page_url", "staff_page_url", "personalization_hook",
    "enriched_at", "email1_path", "followup_path", "brief_path", "notes",
]
_NEW_FIELDS = [
    "name", "lead_type", "first_seen_at", "last_seen_at", "times_seen",
    "lead_score", "lead_score_label", "lead_score_reasons", "ranking_version", "ranked_at",
]
CSV_FIELDS = _LEGACY_FIELDS + _NEW_FIELDS