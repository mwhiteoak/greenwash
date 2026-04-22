# Greenwashing Auditor

Crawls a website and evaluates environmental/sustainability claims against
Australian greenwashing law:

- Australian Consumer Law ss 18 & 29 (Sch 2, *Competition and Consumer Act 2010*)
- ACCC *Making environmental claims: A guide for business* (Dec 2023) — 8 principles
- ASIC Information Sheet 271 (financial products)

Users bring their own Anthropic API key, so hosting costs you nothing beyond
Railway's compute.

## Local development

```bash
cd app
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

Open http://localhost:8000

## Deploy to Railway

1. Push this directory to a new GitHub repo (see `app/` contents).
2. railway.com → New Project → Deploy from GitHub repo → pick the repo.
3. Railway auto-detects Python via Nixpacks and uses `railway.json` for the
   start command. Health check is `/healthz`.
4. In the service **Settings → Networking**, click **Generate Domain** to get
   a public URL.
5. No environment variables are required — the API key is supplied per-request
   by the user.

## How it works

- Frontend posts `{url, api_key, max_pages, delay, include_subdomains}` to
  `/api/start`, receives a `job_id`, then subscribes to
  `/api/stream/{job_id}` (Server-Sent Events).
- Backend crawls sitemap + internal links, respects `robots.txt`, and sends
  each page's text (plus image `alt` attributes) to Claude Sonnet 4.6.
- Live events (`status`, `fetch`, `analyse`, `result`, `done`) render in the
  UI as they happen.
- On completion, the user downloads an Excel report from
  `/api/download/{job_id}`. Job state is kept in memory for 1 hour then swept.

## Security notes

- API keys are held only for the lifetime of the request and not logged or
  persisted.
- In-memory job state means a single Railway instance; don't scale replicas
  above 1 without moving state into Redis.
- Consider adding rate limiting (e.g. `slowapi`) if you expose this publicly.
