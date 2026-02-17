from __future__ import annotations

from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import httpx
from bs4 import BeautifulSoup

from .llm import LLMService
from .models import Lead
from .utils import extract_emails_with_context, find_contact_form_url, find_phone

ROLE_PRIORITY = {
    "principal/head": 0,
    "admissions": 1,
    "office/general": 2,
    "unknown": 3,
}

TARGET_PATHS = {
    "/contact": "contact",
    "/about": "about",
    "/staff": "staff",
    "/directory": "directory",
    "/administration": "administration",
    "/leadership": "leadership",
}

@dataclass
class PageBundle:
    homepage: str = ""
    pages: dict[str, str] = field(default_factory=dict)
    urls: dict[str, str] = field(default_factory=dict)


def _fetch_html(url: str, client: httpx.Client) -> str:
    resp = client.get(url, follow_redirects=True, timeout=20.0)
    resp.raise_for_status()
    return resp.text


def _extract_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    return " ".join(soup.get_text(" ", strip=True).split())


def _allowed_by_robots(url: str, user_agent: str = "lumen-scout") -> bool:
    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    parser = RobotFileParser()
    try:
        parser.set_url(robots_url)
        parser.read()
        return parser.can_fetch(user_agent, url)
    except Exception:
        return True

def _classify_role(context: str) -> str:
    lower = context.lower()
    if any(token in lower for token in ["principal", "head of school", "headmaster", "head school"]):
        return "principal/head"
    if "admission" in lower or "enrollment" in lower:
        return "admissions"
    if any(token in lower for token in ["office", "info@", "contact", "front desk", "general"]):
        return "office/general"
    return "unknown"


def _select_best_email(candidates: list[tuple[str, str]]) -> tuple[str | None, str | None]:
    if not candidates:
        return None, None
    scored = []
    for email, context in candidates:
        role = _classify_role(context)
        scored.append((ROLE_PRIORITY.get(role, 99), email, role))
    scored.sort(key=lambda item: item[0])
    _, best_email, best_role = scored[0]
    return best_email, best_role


def fetch_school_pages(lead: Lead) -> PageBundle:
    if not lead.website:
        return PageBundle()
    if not _allowed_by_robots(lead.website):
        return PageBundle()

    pages = PageBundle()
    with httpx.Client(headers={"User-Agent": "lumen-scout/0.1"}) as client:
        try:
            homepage_html = _fetch_html(lead.website, client)
            pages.homepage = _extract_text(homepage_html)
            form_url = find_contact_form_url(homepage_html, lead.website)
            if form_url:
                pages.urls["contact_form"] = form_url
        except Exception:
            return pages

        for path, key in TARGET_PATHS.items():
            url = urljoin(lead.website, path)
            if not _allowed_by_robots(url):
                continue
            try:
                html = _fetch_html(url, client)
                pages.pages[key] = _extract_text(html)
                pages.urls[key] = url
                if "contact_form" not in pages.urls:
                    form_url = find_contact_form_url(html, url)
                    if form_url:
                        pages.urls["contact_form"] = form_url
            except Exception:
                continue
    return pages


def enrich_lead(lead: Lead, llm: LLMService) -> Lead:
    pages = fetch_school_pages(lead)
    aggregate_parts = [pages.homepage]
    aggregate_parts.extend(pages.pages.values())
    aggregate = "\n".join(part for part in aggregate_parts if part).strip()
    if aggregate:
        email_candidates = extract_emails_with_context(aggregate)
        best_email, best_role = _select_best_email(email_candidates)
        if best_email:
            lead.contact_email = best_email
            lead.contact_role = best_role
        lead.contact_page = pages.urls.get("contact")
        lead.about_page = pages.urls.get("about")
        lead.about_page_url = pages.urls.get("about")
        lead.staff_page_url = pages.urls.get("staff") or pages.urls.get("directory") or pages.urls.get("administration") or pages.urls.get("leadership")
        if not lead.contact_email:
            lead.contact_form_url = pages.urls.get("contact_form")
            if not lead.phone:
                lead.phone = find_phone(aggregate)
        lead.personalization_hook = llm.personalization_hook(lead, aggregate)
    else:
        lead.personalization_hook = llm.personalization_hook(lead, "")

    lead.mark_enriched()
    return lead
