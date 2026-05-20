# Scrapling Runtime Notes

## Current Production Use

The v2 worker uses Scrapling as the default single-article fetch tier:

`Scrapling -> Oxylabs -> archive.org`

Google News RSS discovery also uses Scrapling before any paid SERP fallback.

## Concurrent Crawling Options

Scrapling has two concurrency models that may be useful later:

- `AsyncFetcher`: useful for bounded batches of independent URLs where the worker already owns the queue and database writes.
- `Spider`: useful for source-specific crawls where one seed page fans out into many pages from the same site.

The spider framework supports:

- global concurrency via `concurrent_requests`
- per-domain concurrency via `concurrent_requests_per_domain`
- politeness delays via `download_delay`
- optional robots.txt obey mode
- pause/resume checkpoints via `crawldir`
- multiple sessions, such as fast HTTP for listings and stealth/browser sessions only for protected detail pages

## Recommendation For EduThreat

Do not replace the v2 `fetch_article` worker with a Scrapling spider yet. The existing Postgres task queue already gives us safer cross-domain concurrency, retries, leases, and per-stage backpressure. A spider inside each worker could make one task fetch many URLs at once, but it would also bypass the queue's memory/backpressure controls and make OOM debugging harder.

Good near-term uses:

- Source-specific crawlers where we own the frontier, such as a future dedicated DataBreaches RSS/detail crawler.
- A bounded `AsyncFetcher` batch helper for `fetch_article` only if we cap concurrency very low, for example 2-4 URLs per worker, and preserve one `article_fetch_attempts` row per URL.

Keep the current approach for this deploy:

- Scale via v2 worker counts and task-type worker allocation.
- Keep fetch tasks one source incident at a time.
- Use Scrapling as the low-cost fetch primitive, not as an unbounded crawler.
