"""
Arctic Shift keyword scraper for Reddit posts and comments.

Uses https://arctic-shift.photon-reddit.com (no auth, no API key).
Searches posts (via `query` = title+selftext) and comments (via `body`) for one
or more keywords, with optional subreddit restriction and time window.
Streams results to JSONL, resumable, rate-aware.

Docs: https://github.com/ArthurHeitmann/arctic_shift/blob/master/api/README.md

Usage:
    python reddit_scraper.py \
        --keywords "qpadm" "f-statistics" \
        --subreddit popgen \
        --after 2023-01-01 --before 2025-01-01 \
        --out out/

    # Also fetch full comment trees for every matched post:
    python reddit_scraper.py --keywords "qpadm" --trees --out out/
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator

import httpx

log = logging.getLogger("arctic_shift")

BASE_URL = "https://arctic-shift.photon-reddit.com"
PAGE_SIZE = 100         # API max per request
DEFAULT_RPS = 2.0       # be considerate; it's a free service


# ---------- helpers ----------

class RateLimiter:
    def __init__(self, rps: float):
        self.min_interval = 1.0 / rps
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def wait(self):
        async with self._lock:
            now = time.monotonic()
            d = now - self._last
            if d < self.min_interval:
                await asyncio.sleep(self.min_interval - d)
            self._last = time.monotonic()


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
    rps: float = DEFAULT_RPS
    _client: httpx.AsyncClient = field(init=False)
    _rl: RateLimiter = field(init=False)

    def __post_init__(self):
        self._client = httpx.AsyncClient(
            timeout=60.0,
            headers={"User-Agent": "arctic-shift-scraper/0.1 (+python httpx)"},
        )
        self._rl = RateLimiter(self.rps)

    async def _get(self, path: str, **params) -> dict:
        # drop None/empty values
        params = {k: v for k, v in params.items() if v is not None and v != ""}
        for attempt in range(6):
            await self._rl.wait()
            try:
                r = await self._client.get(f"{self.base_url}{path}", params=params)
            except httpx.HTTPError as e:
                log.warning("network error %s, retry %d: %s", path, attempt, e)
                await asyncio.sleep(2 ** attempt)
                continue

            # respect rate-limit headers proactively
            remaining = r.headers.get("X-RateLimit-Remaining")
            if remaining is not None:
                try:
                    if int(remaining) <= 1:
                        reset = float(r.headers.get("X-RateLimit-Reset", "1"))
                        log.info("rate-limit low (%s left), sleeping %.1fs", remaining, reset)
                        await asyncio.sleep(max(reset, 1.0))
                except ValueError:
                    pass

            if r.status_code == 429:
                wait = float(r.headers.get("X-RateLimit-Reset", 2 ** attempt))
                log.warning("429, sleeping %.1fs", wait)
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

    async def search(
        self,
        kind: str,                     # "posts" or "comments"
        keyword: str,
        subreddit: str | None,
        after: int | None,
        before: int | None,
        limit_per_page: int = PAGE_SIZE,
    ) -> AsyncIterator[dict]:
        """
        Ascending-time pagination. API returns <=100 per call; we advance
        the `after` cursor to (max created_utc) + 1 until we drain the window.
        """
        path = f"/api/{kind}/search"
        # "query" = title + selftext (posts); "body" = comment body text
        kw_param = "query" if kind == "posts" else "body"

        cursor_after = after
        total = 0
        while True:
            params: dict[str, Any] = {
                kw_param: keyword,
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
                log.info("  ... %d %s so far (cursor=%s)", total, kind, cursor_after)

    async def comments_tree(self, post_id: str, limit: int = 9999) -> list[dict]:
        """Full comment tree for a post, flattened (strips 'more' placeholders)."""
        pid = post_id if post_id.startswith("t3_") else f"t3_{post_id}"
        d = await self._get("/api/comments/tree", link_id=pid, limit=limit)
        tree = d.get("data", []) if isinstance(d, dict) else []
        return list(_walk_tree(tree))

    async def aclose(self):
        await self._client.aclose()


def _walk_tree(nodes: list[dict]):
    for n in nodes:
        if n.get("kind") == "more":
            continue
        yield {k: v for k, v in n.items() if k not in ("replies", "children")}
        for key in ("replies", "children"):
            sub = n.get(key)
            if isinstance(sub, list):
                yield from _walk_tree(sub)


# ---------- orchestration ----------

async def scrape(
    kind: str,
    keywords: list[str],
    subreddit: str | None,
    after: int | None,
    before: int | None,
    api: ArcticShift,
    writer: JsonlWriter,
):
    for kw in keywords:
        log.info("[%s] keyword=%r subreddit=%s", kind, kw, subreddit or "*")
        n_new = n_seen = 0
        async for item in api.search(kind, kw, subreddit, after, before):
            item["_keyword"] = kw
            if await writer.write(item):
                n_new += 1
            else:
                n_seen += 1
        log.info("[%s] keyword=%r done: %d new, %d dedup", kind, kw, n_new, n_seen)


async def fetch_trees(api: ArcticShift, posts_path: Path, out: Path):
    tree_writer = JsonlWriter(out / "comment_trees.jsonl")
    try:
        with posts_path.open() as f:
            post_ids = [json.loads(line)["id"] for line in f if line.strip()]
        log.info("fetching trees for %d posts", len(post_ids))
        for i, pid in enumerate(post_ids, 1):
            try:
                for c in await api.comments_tree(pid):
                    c["_post_id"] = pid
                    await tree_writer.write(c)
            except Exception as e:
                log.warning("tree fail %s: %s", pid, e)
            if i % 50 == 0:
                log.info("  trees: %d/%d", i, len(post_ids))
    finally:
        tree_writer.close()


async def main_async(args):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s | %(message)s",
    )
    after = to_epoch(args.after)
    before = to_epoch(args.before)
    out = Path(args.out)

    api = ArcticShift(rps=args.rps)
    posts_writer = JsonlWriter(out / "posts.jsonl")
    comments_writer = JsonlWriter(out / "comments.jsonl")

    try:
        tasks = []
        if "posts" in args.kinds:
            tasks.append(scrape("posts", args.keywords, args.subreddit,
                                after, before, api, posts_writer))
        if "comments" in args.kinds:
            tasks.append(scrape("comments", args.keywords, args.subreddit,
                                after, before, api, comments_writer))
        await asyncio.gather(*tasks)

        if args.trees and "posts" in args.kinds:
            await fetch_trees(api, out / "posts.jsonl", out)
    finally:
        posts_writer.close()
        comments_writer.close()
        await api.aclose()


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--keywords", nargs="+", required=True)
    p.add_argument("--subreddit", default=None, help="restrict to a subreddit (no r/)")
    p.add_argument("--after", default=None, help="YYYY-MM-DD or epoch seconds")
    p.add_argument("--before", default=None, help="YYYY-MM-DD or epoch seconds")
    p.add_argument("--kinds", nargs="+", choices=["posts", "comments"],
                   default=["posts", "comments"])
    p.add_argument("--trees", action="store_true",
                   help="after scraping posts, fetch full comment trees via /api/comments/tree")
    p.add_argument("--rps", type=float, default=DEFAULT_RPS)
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
