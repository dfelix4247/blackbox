from __future__ import annotations

import os
from abc import ABC, abstractmethod

import httpx

from .models import Lead
from .utils import domain_from_url

from dotenv import load_dotenv
load_dotenv()


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
        q = f"private K-12 schools in {city}"
        params = {
            "engine": "google",
            "q": q,
            "num": min(max_results, 100),
            "api_key": self.api_key,
        }
        resp = httpx.get(self.endpoint, params=params, timeout=30.0)
        resp.raise_for_status()
        data = resp.json()
        results = []
        for item in data.get("organic_results", [])[:max_results]:
            url = item.get("link")
            title = item.get("title")
            if not title:
                continue
            results.append(
                Lead(
                    school_name=title,
                    city=city,
                    website=url,
                    domain=domain_from_url(url),
                    provider="serpapi",
                )
            )
        return results


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
