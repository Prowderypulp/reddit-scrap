"""
Arctic Shift keyword scraper for Reddit comments.

Uses https://arctic-shift.photon-reddit.com (no auth, no API key).
Searches comments (via `body`) for keywords provided in a text file, 
with optional subreddit restriction and time window.
Streams results to JSONL, resumable.

Usage:
    python main.py \
        --keywords-file keywords.txt \
        --subreddit popgen \
        --after 2023-01-01 --before 2025-01-01 \
        --out out/
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator

import httpx

log = logging.getLogger("arctic_shift")

BASE_URL = "https://arctic-shift.photon-reddit.com"
PAGE_SIZE = 100         # API max per request


# ---------- helpers ----------

def to_epoch(s: str | None) -> int | None:
    if not s:
        return None
    if s.isdigit():
        return int(s)
    dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


class JsonlWriter:
    """Append-only JSONL with dedup on `id`. Loads existing ids on open."""

    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self.seen: set[str] = set()
        if path.exists():
            with path.open() as f:
                for line in f:
                    try:
                        self.seen.add(json.loads(line)["id"])
                    except Exception:
                        pass
            log.info("%s: resumed with %d ids", path.name, len(self.seen))
        self._fh = path.open("a", buffering=1)
        self._lock = asyncio.Lock()

    async def write(self, obj: dict) -> bool:
        rid = obj.get("id")
        if not rid or rid in self.seen:
            return False
        async with self._lock:
            self._fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
            self.seen.add(rid)
        return True

    def close(self):
        self._fh.close()


# ---------- Arctic Shift client ----------

@dataclass
class ArcticShift:
    base_url: str = BASE_URL
    _client: httpx.AsyncClient = field(init=False)

    def __post_init__(self):
        self._client = httpx.AsyncClient(
            timeout=60.0,
            headers={"User-Agent": "arctic-shift-scraper/0.1 (+python httpx)"},
        )

    async def _get(self, path: str, **params) -> dict:
        # drop None/empty values
        params = {k: v for k, v in params.items() if v is not None and v != ""}
        for attempt in range(6):
            try:
                r = await self._client.get(f"{self.base_url}{path}", params=params)
            except httpx.HTTPError as e:
                log.warning("network error %s, retry %d: %s", path, attempt, e)
                await asyncio.sleep(2 ** attempt)
                continue

            if r.status_code == 429:
                wait = float(r.headers.get("X-RateLimit-Reset", 2 ** attempt))
                log.warning("429 Too Many Requests, server enforced sleep for %.1fs", wait)
                await asyncio.sleep(wait)
                continue
            
            if r.status_code >= 500:
                await asyncio.sleep(2 ** attempt)
                continue

            # "Query timed out" can come back 200 with a body message
            if r.headers.get("content-type", "").startswith("application/json"):
                d = r.json()
                if isinstance(d, dict) and d.get("error") == "Query timed out":
                    log.warning("query timed out for %s params=%s; backing off", path, params)
                    await asyncio.sleep(2 ** attempt)
                    continue
                if r.status_code < 400:
                    return d
            r.raise_for_status()
        raise RuntimeError(f"giving up on {path} params={params}")

    async def search_comments(
        self,
        keyword: str,
        subreddit: str | None,
        after: int | None,
        before: int | None,
        limit_per_page: int = PAGE_SIZE,
    ) -> AsyncIterator[dict]:
        """
        Ascending-time pagination for comments. 
        """
        path = "/api/comments/search"
        cursor_after = after
        total = 0
        
        while True:
            params: dict[str, Any] = {
                "body": keyword,
                "limit": limit_per_page,
                "sort": "asc",
                "after": cursor_after,
                "before": before,
            }
            if subreddit:
                params["subreddit"] = subreddit

            d = await self._get(path, **params)
            batch = d.get("data", []) if isinstance(d, dict) else []
            if not batch:
                return

            for item in batch:
                yield item
            total += len(batch)

            if len(batch) < limit_per_page:
                return  # drained

            newest = max(int(it["created_utc"]) for it in batch)
            next_cursor = newest + 1
            if cursor_after is not None and next_cursor <= cursor_after:
                log.warning("cursor did not advance (%s), stopping", cursor_after)
                return
            cursor_after = next_cursor

            if total % 500 == 0:
                log.info("  ... %d comments so far (cursor=%s)", total, cursor_after)

    async def aclose(self):
        await self._client.aclose()


# ---------- orchestration ----------

async def scrape_comments(
    keywords: list[str],
    subreddit: str | None,
    after: int | None,
    before: int | None,
    api: ArcticShift,
    writer: JsonlWriter,
):
    for kw in keywords:
        log.info("[comments] keyword=%r subreddit=%s", kw, subreddit or "*")
        n_new = n_seen = 0
        async for item in api.search_comments(kw, subreddit, after, before):
            item["_keyword"] = kw
            if await writer.write(item):
                n_new += 1
            else:
                n_seen += 1
        log.info("[comments] keyword=%r done: %d new, %d dedup", kw, n_new, n_seen)


async def main_async(args):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s | %(message)s",
    )
    
    # Read keywords from the provided text file
    with open(args.keywords_file, 'r', encoding='utf-8') as f:
        keywords = [line.strip() for line in f if line.strip()]
        
    if not keywords:
        log.error("Keywords file is empty.")
        sys.exit(1)

    after = to_epoch(args.after)
    before = to_epoch(args.before)
    out = Path(args.out)

    api = ArcticShift()
    comments_writer = JsonlWriter(out / "comments.jsonl")

    try:
        await scrape_comments(keywords, args.subreddit, after, before, api, comments_writer)
    finally:
        comments_writer.close()
        await api.aclose()


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--keywords-file", required=True, help="Path to text file with keywords (one per line)")
    p.add_argument("--subreddit", default=None, help="restrict to a subreddit (no r/)")
    p.add_argument("--after", default=None, help="YYYY-MM-DD or epoch seconds")
    p.add_argument("--before", default=None, help="YYYY-MM-DD or epoch seconds")
    p.add_argument("--out", default="arctic_out")
    return p.parse_args(argv)


def main():
    try:
        asyncio.run(main_async(parse_args()))
    except KeyboardInterrupt:
        log.warning("interrupted; rerun to resume")
        sys.exit(130)


if __name__ == "__main__":
    main()
