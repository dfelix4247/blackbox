from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import httpx
from bs4 import BeautifulSoup

from .llm import LLMService
from .models import Lead
from .utils import find_email


@dataclass
class PageBundle:
    homepage: str = ""
    contact: str = ""
    about: str = ""
    contact_url: str | None = None
    about_url: str | None = None


def _fetch_text(url: str, client: httpx.Client) -> str:
    resp = client.get(url, follow_redirects=True, timeout=20.0)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
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


def fetch_school_pages(lead: Lead) -> PageBundle:
    if not lead.website:
        return PageBundle()
    if not _allowed_by_robots(lead.website):
        return PageBundle()

    pages = PageBundle()
    with httpx.Client(headers={"User-Agent": "lumen-scout/0.1"}) as client:
        try:
            pages.homepage = _fetch_text(lead.website, client)
        except Exception:
            return pages

        for path, attr_text, attr_url in [
            ("/contact", "contact", "contact_url"),
            ("/about", "about", "about_url"),
        ]:
            url = urljoin(lead.website, path)
            if not _allowed_by_robots(url):
                continue
            try:
                text = _fetch_text(url, client)
                setattr(pages, attr_text, text)
                setattr(pages, attr_url, url)
            except Exception:
                continue
    return pages


def enrich_lead(lead: Lead, llm: LLMService) -> Lead:
    pages = fetch_school_pages(lead)
    aggregate = "\n".join([pages.homepage, pages.contact, pages.about]).strip()
    if aggregate:
        email = find_email(aggregate)
        if email:
            lead.contact_email = email
        lead.contact_page = pages.contact_url
        lead.about_page = pages.about_url
        lead.personalization_hook = llm.personalization_hook(lead, aggregate)
    else:
        lead.personalization_hook = llm.personalization_hook(lead, "")
    lead.mark_enriched()
    return lead
