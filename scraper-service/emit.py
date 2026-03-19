"""
emit.py — Shared Redis Stream emitter for the Platform Scraper Service.

Emits job-event dicts onto the same 'jobs:raw' stream consumed by the
Job Aggregator Service, using the canonical JobPosting schema.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

STREAM_NAME = "jobs:raw"
STREAM_MAXLEN = 50_000          # cap stream length (approximate)

_redis: Optional[aioredis.Redis] = None


async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        host = os.environ.get("REDIS_HOST", "localhost")
        port = int(os.environ.get("REDIS_PORT", 6379))
        _redis = aioredis.Redis(host=host, port=port, decode_responses=True)
    return _redis


async def close_redis() -> None:
    global _redis
    if _redis:
        await _redis.aclose()
        _redis = None


async def emit_job(job: Dict[str, Any]) -> None:
    """
    Publish a single job-event dict to the Redis Stream.

    Mandatory fields that must be present in *job*:
        company, role

    All other fields are optional and will default to empty strings or
    JSON-encoded empty arrays when absent.
    """
    redis = await get_redis()

    # Normalise / sanitise fields
    event: Dict[str, str] = {
        "company":   str(job.get("company", "")).strip(),
        "role":      str(job.get("role",    "")).strip(),
        "source":    str(job.get("source",  "")),
        "url":       str(job.get("url",     "")),
        "stack":     json.dumps(job.get("stack") or []),
        "product":   str(job.get("product",   "") or ""),
        "location":  str(job.get("location",  "") or ""),
        "posted_at": str(job.get("posted_at", "") or ""),
    }

    if not event["company"] or not event["role"]:
        logger.debug("Skipping emit — missing company or role: %s", job)
        return

    await redis.xadd(STREAM_NAME, event, maxlen=STREAM_MAXLEN, approximate=True)
    logger.debug("Emitted: %s @ %s", event["role"], event["company"])


async def emit_jobs(jobs: list[Dict[str, Any]]) -> int:
    """Emit a list of jobs; returns the number successfully emitted."""
    count = 0
    for job in jobs:
        try:
            await emit_job(job)
            count += 1
        except Exception as exc:
            logger.exception("Failed to emit job %s: %s", job.get("url"), exc)
    return count
