"""
scrapers/internshala.py — Internshala job scraper using httpx + BeautifulSoup.

Target URL pattern:
    https://internshala.com/jobs/keywords-{role-slug}/

Selector notes (last verified March 2026):
──────────────────────────────────────────────────────────────────────────────
  Job cards            div.individual_internship   (each posting)
  Title                h3.job-internship-name  a
                        OR  div.profile  h3 a
  Company              h4.company-name  a
                        OR  div.company_name  a
  Location             div.internship_other_details_container
                          span[title*="City"]  a
                        OR  a.location_link
  Skills               div.round_tabs_container  span.round_tabs
                        OR  div#skills_section  span.skill
  Stipend / Salary     div.stipend_container  span.stipend
  Job URL              h3.job-internship-name  a[href]  → prepend base URL
  Posted date          div.posted_by_container  div.status-inactive  span
                        OR  div.posted_recently
──────────────────────────────────────────────────────────────────────────────

Pagination:
  Internshala lists 12–15 jobs per page.  We fetch up to 2 pages
  (there is no explicit page-number URL param — Internshala uses an
  AJAX-based infinite scroll for logged-in users, but the first two
  pages are accessible via:
    page 1: /jobs/keywords-{slug}/
    page 2: /jobs/keywords-{slug}/?page=2

Rate-limiting notes:
  - Internshala does not aggressively block scrapers but does impose
    CAPTCHA on excessive traffic.  Keep SCRAPER_DELAY_SECONDS ≥ 2.
  - Do NOT run more than 2 concurrent requests against Internshala.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import re
from typing import Any

import httpx
from bs4 import BeautifulSoup

from .base import PlatformScraper

logger = logging.getLogger(__name__)

BASE_URL  = "https://internshala.com"
MAX_PAGES = 2

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": "https://internshala.com/",
}


def _role_to_slug(role: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", role.lower()).strip("-")


def _parse_cards(soup: BeautifulSoup, role: str) -> list[dict[str, Any]]:
    """
    Extract job postings from one results page.

    SELECTOR MAP — update these strings when Internshala changes its HTML:
      card_sel     : outermost container for one job posting
      title_sel    : anchor inside the job title heading
      company_sel  : anchor or element with company name
      loc_sel      : location element (city)
      skills_sel   : individual skill/technology badge elements
      stipend_sel  : salary / stipend string element
      url_attr     : attribute on the title anchor holding the relative URL
    """
    card_sel    = "div.individual_internship"
    title_sel   = "h3.job-internship-name a, div.profile h3 a"
    company_sel = "h4.company-name a, div.company_name a, p.company-name"
    loc_sel     = "a.location_link, div.internship_other_details_container span a"
    skills_sel  = "div.round_tabs_container span.round_tabs, div#skills_section span.skill"
    stipend_sel = "div.stipend_container span.stipend, span.stipend"
    url_attr    = "href"

    jobs: list[dict[str, Any]] = []

    for card in soup.select(card_sel):
        try:
            title_el = card.select_one(title_sel)
            if not title_el:
                continue

            title   = title_el.get_text(strip=True)
            raw_url = title_el.get(url_attr, "")
            job_url = raw_url if raw_url.startswith("http") else BASE_URL + raw_url

            company_el = card.select_one(company_sel)
            company    = company_el.get_text(strip=True) if company_el else ""

            loc_el   = card.select_one(loc_sel)
            location = loc_el.get_text(strip=True) if loc_el else ""

            skills = [el.get_text(strip=True) for el in card.select(skills_sel)]

            stipend_el = card.select_one(stipend_sel)
            product    = stipend_el.get_text(strip=True) if stipend_el else None

            if not company:
                continue

            jobs.append(
                {
                    "company":   company,
                    "role":      title or role,
                    "source":    "internshala",
                    "url":       job_url,
                    "stack":     skills,
                    "product":   product,      # reuse product field for stipend info
                    "location":  location,
                    "posted_at": None,
                }
            )
        except Exception as exc:
            logger.debug("[Internshala] Error parsing card: %s", exc)
            continue

    return jobs


class IntershalaScraper(PlatformScraper):
    source_name = "internshala"

    def __init__(self) -> None:
        self._delay: float = float(os.environ.get("SCRAPER_DELAY_SECONDS", 3))

    async def scrape(self, role: str, stack: list[str]) -> list[dict[str, Any]]:
        slug = _role_to_slug(role)
        all_jobs: list[dict[str, Any]] = []

        async with httpx.AsyncClient(
            headers=_HEADERS,
            follow_redirects=True,
            timeout=20,
        ) as client:
            for page in range(1, MAX_PAGES + 1):
                if page == 1:
                    url = f"{BASE_URL}/jobs/keywords-{slug}/"
                else:
                    url = f"{BASE_URL}/jobs/keywords-{slug}/?page={page}"

                logger.info("[Internshala] Fetching page %d: %s", page, url)
                try:
                    resp = await client.get(url)
                    resp.raise_for_status()
                except httpx.HTTPStatusError as exc:
                    logger.warning(
                        "[Internshala] HTTP %s on page %d — stopping.",
                        exc.response.status_code, page,
                    )
                    break
                except httpx.RequestError as exc:
                    logger.error("[Internshala] Request error page %d: %s", page, exc)
                    break

                soup  = BeautifulSoup(resp.text, "html.parser")
                cards = _parse_cards(soup, role)
                logger.info("[Internshala] Page %d → %d jobs", page, len(cards))
                all_jobs.extend(cards)

                if not cards:
                    break

                if page < MAX_PAGES:
                    delay = self._delay + random.uniform(0, 1)
                    await asyncio.sleep(max(delay, 2.0))

        logger.info(
            "[Internshala] Total scraped: %d jobs for role '%s'", len(all_jobs), role
        )
        return all_jobs
