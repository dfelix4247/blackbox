from __future__ import annotations

from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import httpx
from bs4 import BeautifulSoup

from .llm import LLMService
from .models import Lead
from .utils import extract_emails_with_context, find_contact_form_url, find_phone

EMAIL_ROLE_PRIORITY = {
    "principal/head": 0,
    "admissions": 1,
    "office/general": 2,
    "unknown": 3,
    "director_ops": 1,
    "admissions": 2,
    "office/info": 3,
    "generic": 4,
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
    html_by_page: dict[str, str] = field(default_factory=dict)


@dataclass
class ContactAssessment:
    method: str
    score: int
    priority_label: str


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

def _classify_email_role(email: str, context: str) -> str:
    lower = f"{email} {context}".lower()
    if any(token in lower for token in ["principal", "head of school", "headmaster", "head school"]):
        return "principal/head"
    if any(token in lower for token in ["director of operations", "director operations", "operations director"]):
        return "director_ops"
    if "admission" in lower or "enrollment" in lower:
        return "admissions"
    if any(token in lower for token in ["office", "main office", "info@", "contact@"]):
        return "office/info"
    return "generic"


def _select_best_email(candidates: list[tuple[str, str]]) -> tuple[str | None, str | None, str]:
    if not candidates:
        return None, None, ""

    scored: list[tuple[int, str, str]] = []
    ordered_emails: list[str] = []
    for email, context in candidates:
        role = _classify_email_role(email, context)
        scored.append((EMAIL_ROLE_PRIORITY.get(role, 99), email, role))
        if email not in ordered_emails:
            ordered_emails.append(email)
    scored.sort(key=lambda item: item[0])
    _, best_email, best_role = scored[0]
    return best_email, best_role, ";".join(ordered_emails)


def _classify_linkedin_role(anchor_text: str, href: str) -> str:
    lower = f"{anchor_text} {href}".lower()
    if any(token in lower for token in ["principal", "head of school", "headmaster", "head school"]):
        return "principal/head"
    if any(token in lower for token in ["director of operations", "director operations", "operations director"]):
        return "director_ops"
    return "school"


def _find_best_linkedin_url(html_chunks: list[str]) -> tuple[str | None, str | None]:
    ranked: list[tuple[int, str, str]] = []
    seen: set[str] = set()
    role_rank = {"principal/head": 0, "director_ops": 1, "school": 2}

    for html in html_chunks:
        soup = BeautifulSoup(html, "html.parser")
        for anchor in soup.find_all("a"):
            href = anchor.get("href")
            if not href or "linkedin.com" not in href.lower() or href in seen:
                continue
            seen.add(href)
            role = _classify_linkedin_role(anchor.get_text(" ", strip=True), href)
            ranked.append((role_rank.get(role, 9), href, role))

    if not ranked:
        return None, None

    ranked.sort(key=lambda item: item[0])
    _, url, role = ranked[0]
    return url, role


def _score_contactability(
    best_email: str | None,
    best_email_role: str | None,
    linkedin_role: str | None,
    has_contact_form: bool,
    has_phone: bool,
    city_matches_linkedin: bool,
) -> ContactAssessment:
    if best_email and best_email_role == "principal/head":
        score = 100
        method = "principal_email"
        tier = "Tier 1"
    elif best_email and best_email_role == "director_ops":
        score = 95
        method = "director_email"
        tier = "Tier 1"
    elif linkedin_role == "principal/head":
        score = 85
        method = "linkedin"
        tier = "Tier 2"
    elif linkedin_role == "director_ops":
        score = 80
        method = "linkedin"
        tier = "Tier 2"
    elif linkedin_role == "school":
        score = 75
        method = "linkedin"
        tier = "Tier 2"
    elif best_email and best_email_role == "admissions":
        score = 70 if "admissions@" in best_email.lower() else 90
        method = "admissions_email"
        tier = "Tier 3"
    elif best_email and "info@" in best_email.lower():
        score = 65
        method = "general_email"
        tier = "Tier 3"
    elif best_email and "office@" in best_email.lower():
        score = 60
        method = "general_email"
        tier = "Tier 3"
    elif best_email:
        score = 50
        method = "general_email"
        tier = "Tier 3"
    elif has_contact_form:
        score = 40
        method = "contact_form"
        tier = "Tier 4"
    elif has_phone:
        score = 20
        method = "phone_only"
        tier = "Tier 5"
    else:
        score = 0
        method = "none"
        tier = "Tier 5"

    if best_email and "@" in best_email and all(token not in best_email.lower() for token in ["info@", "office@", "admissions@", "contact@"]) and score < 100:
        score += 5
    if city_matches_linkedin:
        score += 5

    return ContactAssessment(method=method, score=min(score, 100), priority_label=tier)


def fetch_school_pages(lead: Lead) -> PageBundle:
    if not lead.website:
        return PageBundle()
    if not _allowed_by_robots(lead.website):
        return PageBundle()

    pages = PageBundle()
    with httpx.Client(headers={"User-Agent": "lumen-scout/0.1"}) as client:
        try:
            homepage_html = _fetch_html(lead.website, client)
            pages.html_by_page["homepage"] = homepage_html
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
                pages.html_by_page[key] = html
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
        best_email, best_role, all_emails = _select_best_email(email_candidates)
        linkedin_url, linkedin_role = _find_best_linkedin_url(list(pages.html_by_page.values()))

        lead.contact_email = best_email
        lead.contact_role = best_role
        lead.all_emails = all_emails or None
        lead.primary_contact = best_email
        lead.linkedin_url = linkedin_url
        lead.contact_page = pages.urls.get("contact")
        lead.about_page = pages.urls.get("about")
        lead.about_page_url = pages.urls.get("about")
        lead.staff_page_url = pages.urls.get("staff") or pages.urls.get("directory") or pages.urls.get("administration") or pages.urls.get("leadership")
        lead.contact_form_url = pages.urls.get("contact_form")

        if not lead.phone:
            lead.phone = find_phone(aggregate)

        city_match = bool(linkedin_url and lead.city and lead.city.lower() in aggregate.lower())
        assessment = _score_contactability(
            best_email=best_email,
            best_email_role=best_role,
            linkedin_role=linkedin_role,
            has_contact_form=bool(lead.contact_form_url),
            has_phone=bool(lead.phone),
            city_matches_linkedin=city_match,
        )
        lead.contact_method = assessment.method
        lead.contact_score = assessment.score
        lead.contact_priority_label = assessment.priority_label
        lead.personalization_hook = llm.personalization_hook(lead, aggregate)
    else:
        lead.personalization_hook = llm.personalization_hook(lead, "")
        assessment = _score_contactability(
            best_email=lead.contact_email,
            best_email_role=lead.contact_role,
            linkedin_role=None,
            has_contact_form=bool(lead.contact_form_url),
            has_phone=bool(lead.phone),
            city_matches_linkedin=False,
        )
        lead.contact_method = assessment.method
        lead.contact_score = assessment.score
        lead.contact_priority_label = assessment.priority_label

    lead.mark_enriched()
    return lead
