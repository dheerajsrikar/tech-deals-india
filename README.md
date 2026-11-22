# Indian Tech & Mobile Deals — Autonomous Affiliate Agent

100% free, autonomous tech-deals agent. Every 2 hours it reads the
DesiDime deals RSS, lets Gemini (`gemini-3.5-flash-lite`, free tier)
approve only real tech deals, posts them to Telegram (+ Pinterest when
configured), and publishes them on a GitHub Pages site.

## Architecture

| Piece | Service | Free tier |
|---|---|---|
| Back-end | Google Cloud Run (`asia-east1`, 256Mi, max 1 instance) | 2M requests/month free |
| Store | Firestore Native Mode (`asia-east1`), `posted_deals` + `deals` | 1 GiB + 50k reads/day free |
| Trigger | Cloud Scheduler, `0 */2 * * *` → `GET /run?key=CRON_SECRET` | 3 jobs free |
| Front-end | GitHub Pages (`main` branch) | Free |

## Repo layout

```
index.html            # GitHub Pages site (fetches /deals from Cloud Run)
backend/main.py       # Flask app: /run, /deals, /
backend/requirements.txt
backend/Dockerfile
deploy.sh             # one-command deploy (needs gcloud login)
```

## Endpoints

- `GET /` — health check.
- `GET /run?key=CRON_SECRET` — run one agent cycle. Returns
  `{"checked": n, "approved": n, "posted": n, "skipped": n}`.
- `GET /deals` — public, CORS-enabled. Latest 24 approved deals,
  newest first: `{"deals": [...], "count": n}`.

## Env vars (set on Cloud Run — never commit secrets)

| Var | Purpose |
|---|---|
| `CRON_SECRET` | Shared secret for `/run` + the Scheduler job |
| `GEMINI_API_KEY` | Google AI Studio free key |
| `TELEGRAM_BOT_TOKEN` | From @BotFather |
| `TELEGRAM_CHAT_ID` | Channel/chat ID the bot posts to (bot must be admin) |
| `AMAZON_TAG` | Amazon Associates tracking ID (e.g. `yourtag-21`) |
| `CUELINKS_ID` | Cuelinks publisher ID (non-Amazon stores) |
| `PINTEREST_TOKEN` | Optional — Pinterest API v5 token |
| `PINTEREST_BOARD_ID` | Optional — Pinterest board to pin to |

## Deploy

```bash
# 1. Log in once
gcloud auth login
# 2. Fill in .env (copy .env.example) — it is git-ignored
cp .env.example .env   # then edit the values
# 3. Deploy + wire the scheduler + point the site at the service
PROJECT_ID=tech-deals-india ./deploy.sh
```

`deploy.sh` creates the project APIs, the Firestore database in
`asia-east1`, deploys Cloud Run, creates the 2-hour Scheduler job, and
rewrites `__API_URL__` in `index.html` to the live service URL.

## Notes

- Deduplication: an MD5 of each deal URL is stored in `posted_deals`,
  so a deal is never processed twice — even across restarts.
- DesiDime blocks bare `curl` / default Python user-agents (403), so the
  feed is fetched with browser-like headers.
- Gemini is the only filter: if the key is missing or a call fails, the
  deal is skipped, never posted unfiltered.
- Pinterest is optional — when `PINTEREST_TOKEN` / `PINTEREST_BOARD_ID`
  are unset, pinning is skipped silently.
- Affiliate disclosure is in the site footer.
