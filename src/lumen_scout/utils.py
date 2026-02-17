from __future__ import annotations

import re
from difflib import SequenceMatcher
from urllib.parse import urlparse

EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")


def domain_from_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    domain = parsed.netloc.lower().strip()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain or None


def dedupe_leads(existing: list, incoming: list, name_threshold: float = 0.9) -> list:
    kept = list(existing)
    for lead in incoming:
        if any(e.domain and lead.domain and e.domain == lead.domain for e in kept):
            continue
        if any(SequenceMatcher(None, e.school_name.lower(), lead.school_name.lower()).ratio() >= name_threshold for e in kept):
            continue
        kept.append(lead)
    return kept


def find_email(text: str) -> str | None:
    match = EMAIL_RE.search(text)
    return match.group(0) if match else None
