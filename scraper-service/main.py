"""
main.py — FastAPI entrypoint for the Platform Scraper Service.

Endpoints
─────────
  POST /scrape    Trigger all scrapers concurrently
  GET  /health    Liveness probe
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel

import emit as emitter
from scrapers.naukri import NaukriScraper
from scrapers.linkedin import LinkedInScraper
from scrapers.internshala import IntershalaScraper
from scrapers.unstop import UnstopScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

PLATFORMS = ["naukri", "linkedin", "internshala", "unstop"]


# ---------------------------------------------------------------------------
# Lifespan — warm up Redis connection on startup
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Eagerly open the Redis connection so the first /scrape is fast
    await emitter.get_redis()
    logger.info("Scraper service ready.")
    yield
    await emitter.close_redis()
    logger.info("Scraper service shut down.")


app = FastAPI(
    title="Platform Scraper Service",
    description=(
        "Scrapes Naukri, LinkedIn (public), and Internshala for job listings "
        "and emits events to the jobs:raw Redis Stream."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class ScrapeRequest(BaseModel):
    role: str = "Backend Engineer"
    stack: list[str] = []
    platforms: list[str] = []   # empty → scrape all platforms


class ScrapeResponse(BaseModel):
    triggered: bool
    platforms: list[str]


# ---------------------------------------------------------------------------
# Background scrape runner
# ---------------------------------------------------------------------------

async def _run_scrapers(role: str, stack: list[str], platforms: list[str]) -> None:
    """
    Run selected (or all) scrapers concurrently then emit results to Redis.
    Designed to be launched as a BackgroundTask so /scrape returns immediately.

    ``platforms`` is a list of lowercase platform names; an empty list means
    "scrape everything" (the legacy / default behaviour).
    """
    all_scrapers = [
        NaukriScraper(),
        LinkedInScraper(),
        IntershalaScraper(),
        UnstopScraper(),
    ]

    # Filter to requested platforms; default to all when nothing is specified.
    if platforms:
        requested = {p.lower() for p in platforms}
        scrapers = [s for s in all_scrapers if s.source_name.lower() in requested]
        if not scrapers:
            logger.warning(
                "No scrapers matched requested platforms %s — falling back to all.",
                platforms,
            )
            scrapers = all_scrapers
    else:
        scrapers = all_scrapers

    # Run selected scrapers in parallel; capture per-scraper exceptions.
    results = await asyncio.gather(
        *[s.scrape(role, stack) for s in scrapers],
        return_exceptions=True,
    )

    for scraper, result in zip(scrapers, results):
        if isinstance(result, Exception):
            logger.error(
                "[%s] Scraper raised an exception: %s",
                scraper.source_name, result,
            )
            continue

        logger.info(
            "[%s] Emitting %d jobs.", scraper.source_name, len(result)
        )
        emitted = await emitter.emit_jobs(result)
        logger.info("[%s] Emitted %d / %d jobs.", scraper.source_name, emitted, len(result))


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health", tags=["ops"])
async def health():
    return {"status": "ok"}


@app.post("/scrape", response_model=ScrapeResponse, tags=["scraping"])
async def trigger_scrape(body: ScrapeRequest, background_tasks: BackgroundTasks):
    """
    Trigger platform scrapers concurrently in the background.

    If ``body.platforms`` is non-empty, only those platforms are scraped.
    An empty (or omitted) ``platforms`` list scrapes all platforms (legacy behaviour).
    Returns immediately while scraping happens asynchronously.
    """
    active_platforms = (
        [p.lower() for p in body.platforms]
        if body.platforms
        else PLATFORMS
    )
    background_tasks.add_task(_run_scrapers, body.role, body.stack, body.platforms)
    return ScrapeResponse(triggered=True, platforms=active_platforms)
