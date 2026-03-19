"""
scrapers/base.py — Abstract base class for all platform scrapers.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class PlatformScraper(ABC):
    """
    Contract every platform scraper must implement.

    scrape() returns a list of dicts, each matching the JobPosting
    event schema consumed by the aggregator service:

        company    str
        role       str
        source     str
        url        str
        stack      list[str]   (serialised to JSON by the emitter)
        product    str | None
        location   str | None
        posted_at  str | None  (ISO-8601 string or None)
    """

    # Scrapers set this to identify themselves in stream events
    source_name: str = "unknown"

    @abstractmethod
    async def scrape(self, role: str, stack: list[str]) -> list[dict[str, Any]]:
        """
        Scrape jobs for *role* on *stack* and return a list of raw job dicts.
        Each dict must have at minimum 'company', 'role', and 'url' set.
        """
        ...
