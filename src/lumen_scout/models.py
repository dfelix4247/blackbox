from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import uuid4

from pydantic import BaseModel, Field


class Lead(BaseModel):
    lead_id: str = Field(default_factory=lambda: str(uuid4()))
    school_name: str
    city: str
    website: Optional[str] = None
    domain: Optional[str] = None
    provider: Optional[str] = None
    source_query: Optional[str] = None
    address: Optional[str] = None
    phone: Optional[str] = None
    contact_email: Optional[str] = None
    contact_role: Optional[str] = None
    contact_form_url: Optional[str] = None
    contact_page: Optional[str] = None
    about_page: Optional[str] = None
    about_page_url: Optional[str] = None
    staff_page_url: Optional[str] = None
    personalization_hook: Optional[str] = None
    enriched_at: Optional[str] = None
    email1_path: Optional[str] = None
    followup_path: Optional[str] = None
    brief_path: Optional[str] = None
    notes: Optional[str] = None

    def mark_enriched(self) -> None:
        self.enriched_at = datetime.utcnow().isoformat()


CSV_FIELDS = list(Lead.model_fields.keys())
