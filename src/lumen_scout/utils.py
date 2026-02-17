from __future__ import annotations

import re
from difflib import SequenceMatcher
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
PHONE_RE = re.compile(r"(?:\+1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")


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


def extract_emails_with_context(text: str, window: int = 120) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    for match in EMAIL_RE.finditer(text):
        start = max(0, match.start() - window)
        end = min(len(text), match.end() + window)
        context = text[start:end]
        found.append((match.group(0), context))
    return found


def find_contact_form_url(html: str, base_url: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    for form in soup.find_all("form"):
        action = form.get("action")
        if action:
            return urljoin(base_url, action)
    for link in soup.find_all("a"):
        href = link.get("href")
        text = (link.get_text(" ", strip=True) or "").lower()
        if not href:
            continue
        if "contact" in href.lower() or "contact" in text:
            return urljoin(base_url, href)
    return None


def find_phone(text: str) -> str | None:
    match = PHONE_RE.search(text)
    return match.group(0) if match else None