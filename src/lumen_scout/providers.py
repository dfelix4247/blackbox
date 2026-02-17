from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod

import httpx

from .models import Lead
from .utils import domain_from_url

from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)

BLOCKED_DIRECTORY_DOMAINS = {
    "niche.com",
    "yelp.com",
    "greatschools.org",
    "privateschoolreview.com",
    "expertise.com",
    "mapquest.com",
    "facebook.com",
    "instagram.com",
}


class SearchProvider(ABC):
    @abstractmethod
    def search(self, city: str, max_results: int) -> list[Lead]:
        raise NotImplementedError


class SerpApiProvider(SearchProvider):
    endpoint = "https://serpapi.com/search.json"

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or os.getenv("SERPAPI_API_KEY")
        if not self.api_key:
            raise ValueError("SERPAPI_API_KEY is required for SerpAPI provider")

    def search(self, city: str, max_results: int) -> list[Lead]:
        normalized_city = city.replace(",", "").strip()
        queries = [
            f"Private school {normalized_city}",
            f"Catholic school {normalized_city}",
            f"Christian school {normalized_city}",
            f"Montessori {normalized_city}",
            f"College prep {normalized_city}",
        ]

        leads: list[Lead] = []
        seen_domains: set[str] = set()
        seen_names: set[str] = set()

        for query in queries:
            params = {
                "engine": "google_maps",
                "q": query,
                "api_key": self.api_key,
            }
            resp = httpx.get(self.endpoint, params=params, timeout=30.0)
            resp.raise_for_status()
            data = resp.json()

            for item in data.get("local_results", []):
                school_name = item.get("title") or item.get("name")
                website = item.get("website") or item.get("links", {}).get("website")
                address = item.get("address")
                phone = item.get("phone")

                if not school_name:
                    logger.info("[DISCOVER] query='%s' rejected: missing school name", query)
                    continue

                domain = domain_from_url(website)
                blocked = False
                if domain:
                    blocked = any(domain == blocked_domain or domain.endswith(f".{blocked_domain}") for blocked_domain in BLOCKED_DIRECTORY_DOMAINS)
                if blocked:
                    logger.info(
                        "[DISCOVER] query='%s' rejected: %s (%s) blocked directory domain",
                        query,
                        school_name,
                        domain,
                    )
                    continue

                normalized_name = school_name.lower().strip()
                if domain and domain in seen_domains:
                    logger.info("[DISCOVER] query='%s' rejected: %s duplicate domain %s", query, school_name, domain)
                    continue
                if normalized_name in seen_names:
                    logger.info("[DISCOVER] query='%s' rejected: %s duplicate school name", query, school_name)
                    continue

                lead = Lead(
                    school_name=school_name,
                    city=city,
                    website=website,
                    domain=domain,
                    provider="serpapi",
                    source_query=query,
                    address=address,
                    phone=phone,
                )
            
        
                leads.append(lead)
                if domain:
                    seen_domains.add(domain)
                seen_names.add(normalized_name)
                logger.info(
                    "[DISCOVER] query='%s' accepted: %s (domain=%s, website=%s)",
                    query,
                    school_name,
                    domain or "none",
                    website or "none",
                )
                if len(leads) >= max_results:
                    return leads

        return leads


class BraveProvider(SearchProvider):
    endpoint = "https://api.search.brave.com/res/v1/web/search"

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or os.getenv("BRAVE_SEARCH_API_KEY")
        if not self.api_key:
            raise ValueError("BRAVE_SEARCH_API_KEY is required for Brave provider")

    def search(self, city: str, max_results: int) -> list[Lead]:
        q = f"private K-12 schools in {city}"
        headers = {"Accept": "application/json", "X-Subscription-Token": self.api_key}
        params = {"q": q, "count": min(max_results, 20)}
        resp = httpx.get(self.endpoint, headers=headers, params=params, timeout=30.0)
        resp.raise_for_status()
        data = resp.json()
        results = []
        for item in data.get("web", {}).get("results", [])[:max_results]:
            url = item.get("url")
            title = item.get("title")
            if not title:
                continue
            results.append(
                Lead(
                    school_name=title,
                    city=city,
                    website=url,
                    domain=domain_from_url(url),
                    provider="brave",
                )
            )
        return results


def get_provider(name: str) -> SearchProvider:
    normalized = name.strip().lower()
    if normalized == "serpapi":
        return SerpApiProvider()
    if normalized == "brave":
        return BraveProvider()
    raise ValueError(f"Unsupported provider: {name}")
