# Security Policy

## Posture

EduThreat-CTI is an **open, read-only research platform**. The public API serves
the enriched education-sector incident corpus derived entirely from public OSINT
disclosures. The API endpoint being publicly known is expected and intended —
researchers and tools are meant to consume it.

What this means for security:

- **Read endpoints are public.** They expose only already-public, aggregated
  incident intelligence. No personal data beyond what the cited sources already
  disclosed (see the dataset datasheet for the data-handling posture).
- **All write/admin endpoints require authentication.** Every mutating or
  administrative route (`/api/admin/v2/*`) is gated behind a session token; only
  `/api/admin/v2/login` is unauthenticated (it issues the token). Read endpoints
  perform no mutations.
- **Rate limiting** protects the public API from abuse and runaway cost
  (default 60 requests/minute per client IP; `/health` is exempt). Configurable
  via `API_RATE_LIMIT`.
- **CORS** is restricted to the dashboard origin(s) by default; configurable via
  `CORS_ALLOW_ORIGINS`.
- **No secrets in the client.** The dashboard ships only `NEXT_PUBLIC_API_URL`
  (the public API base) to the browser — this is by design and exposes nothing
  sensitive. Credentials, database URLs, and provider keys live only in
  server-side environment variables and are never committed.

## Configuration (operators)

| Variable | Purpose | Default |
|---|---|---|
| `API_RATE_LIMIT` | Per-IP rate limit for public read endpoints | `60/minute` |
| `CORS_ALLOW_ORIGINS` | Comma-separated allowed browser origins (`*` to allow any) | dashboard + localhost |
| `CORS_ALLOW_ORIGIN_REGEX` | Regex for *additional* allowed origins — admits Vercel preview deployments whose subdomain carries a rotating build hash (set empty to disable) | matches this project's `*.vercel.app` production + preview/branch URLs |

We recommend fronting the API with a CDN/proxy (e.g. Cloudflare) for caching,
edge rate-limiting, and DDoS protection, and pointing a custom domain at it so
the deployment host is not the public face.

## Reporting a vulnerability

Please report security issues privately rather than opening a public issue.
Email **sagarkishore.7@gmail.com** with details and reproduction steps. Because
the platform only mirrors already-public disclosures, it raises no responsible-
disclosure obligation beyond honouring the original sources' terms; the corpus
contains no exploit code, operational tooling, or undisclosed vulnerabilities.
