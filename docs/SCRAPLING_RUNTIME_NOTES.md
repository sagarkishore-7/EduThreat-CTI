# Scrapling Runtime Notes

## Current Production Use

The v2 worker uses Scrapling as the default single-article fetch tier, with
newspaper3k retained as a cheap rescue parser for sites where Scrapling returns
a gate page, empty body, or transient source error:

`Scrapling -> newspaper3k -> Oxylabs -> archive.org`

There is also optional browser-backed Scrapling rescue:

`Scrapling -> newspaper3k -> Scrapling Dynamic -> Oxylabs -> archive.org -> Scrapling Stealthy`

Keep this tier off unless static Scrapling/newspaper are missing useful articles
that need JavaScript rendering or stronger anti-bot handling. It launches
Chromium, so the safe production defaults are:

- `EDU_CTI_FETCH_ENABLE_SCRAPLING_BROWSER=0`
- `EDU_CTI_SCRAPLING_BROWSER_MODE=dynamic`
- `EDU_CTI_SCRAPLING_BROWSER_TRIGGER_REASONS=403,empty_content,soft_404`
- `EDU_CTI_SCRAPLING_BROWSER_MAX_CONCURRENCY=1`

Stealth mode uses Scrapling's `StealthyFetcher`, which depends on Patchright.
The v2 worker Docker image runs both `playwright install chromium --with-deps`
and `patchright install chromium`. When `EDU_CTI_SCRAPLING_BROWSER_MODE=stealthy`,
the pipeline still lets archive.org run before StealthyFetcher; stealth is the
last resort after the free archive fallback fails.

Proxy support is env-provided only through `EDU_CTI_SCRAPLING_PROXY_URL` or
`EDU_CTI_SCRAPLING_PROXY_POOL`. Do not scrape public free proxy lists into the
worker; they create noisy failures, security risk, and domain reputation drift.

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
