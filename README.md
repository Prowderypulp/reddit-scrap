# Reddit Scraper (Arctic Shift)

Async keyword scraper for Reddit posts and comments, backed by the
[Arctic Shift API](https://github.com/ArthurHeitmann/arctic_shift/blob/master/api/README.md).
No auth, no API keys.

## Install

```bash
pip install httpx
```

Requires Python 3.10+.

## Usage

Keyword search on Arctic Shift requires scoping by `--subreddit` (or `--author`),
otherwise the API returns 400.

```bash
python main.py --keywords qpAdm --subreddit popgen --out out/
```

Multiple keywords, time-bounded, with full comment trees:

```bash
python main.py \
  --keywords "qpAdm" "f4-statistic" "ADMIXTOOLS" \
  --subreddit popgen \
  --after 2023-01-01 --before 2025-01-01 \
  --trees \
  --out out/
```

### Arguments

| Flag | Description |
|---|---|
| `--keywords` | One or more search terms (required) |
| `--subreddit` | Restrict to a subreddit (no `r/` prefix) |
| `--after` | `YYYY-MM-DD` or epoch seconds |
| `--before` | `YYYY-MM-DD` or epoch seconds |
| `--kinds` | `posts`, `comments`, or both (default: both) |
| `--trees` | Fetch full comment tree for each matched post |
| `--rps` | Requests per second (default: 2.0) |
| `--out` | Output directory (default: `arctic_out`) |

## Output

JSONL files, one record per line:

- `out/posts.jsonl` — matching posts (search hits on title + selftext)
- `out/comments.jsonl` — matching comments (search hits on body)
- `out/comment_trees.jsonl` — full threads for matched posts (with `--trees`)

Each record carries a `_keyword` field naming the search term that matched it.

## Resuming

Re-running the same command resumes where it left off. The script loads
existing IDs from each output file on startup and skips them. Safe to
Ctrl-C and restart.

## Notes

- Comment body search uses Postgres FTS and can time out on broad queries.
  The client backs off and retries automatically; if it persists, narrow
  the time window or pick a smaller subreddit.
- Default rate is 2 rps to stay polite. The script also reads
  `X-RateLimit-Remaining` / `X-RateLimit-Reset` headers and sleeps when
  the budget runs low.
