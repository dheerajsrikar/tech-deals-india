#!/usr/bin/env bash
# Deploy the Tech Deals agent to Google Cloud Run (Always Free Tier).
# Usage:  PROJECT_ID=tech-deals-india ./deploy.sh
# Requires: gcloud login done, .env filled in (see .env.example).
# .env is git-ignored and never leaves this machine except as Cloud Run
# env vars set by this script.
set -euo pipefail

PROJECT_ID="${PROJECT_ID:?set PROJECT_ID, e.g. PROJECT_ID=tech-deals-india}"
REGION="asia-east1"
SERVICE="tech-deals-agent"
GCLOUD="$HOME/workspace/google-cloud-sdk/bin/gcloud"
[ -x "$GCLOUD" ] || GCLOUD="gcloud"

if [ ! -f .env ]; then echo "Missing .env — copy .env.example to .env and fill it in."; exit 1; fi
set -a; # shellcheck disable=SC1091
. ./.env; set +a
: "${CRON_SECRET:?CRON_SECRET missing in .env}"
: "${GEMINI_API_KEY:?GEMINI_API_KEY missing in .env}"
: "${TELEGRAM_BOT_TOKEN:?TELEGRAM_BOT_TOKEN missing in .env}"
: "${TELEGRAM_CHAT_ID:?TELEGRAM_CHAT_ID missing in .env}"

"$GCLOUD" config set project "$PROJECT_ID"
"$GCLOUD" services enable run.googleapis.com firestore.googleapis.com \
  cloudscheduler.googleapis.com artifactregistry.googleapis.com \
  cloudbuild.googleapis.com --project="$PROJECT_ID"

# Firestore Native Mode in asia-east1 (one database per project; if the
# project already has one in another region this step fails — use a fresh
# project, e.g. tech-deals-india).
"$GCLOUD" firestore databases create --location="$REGION" \
  --type=firestore-native --project="$PROJECT_ID" 2>/dev/null \
  || echo "Firestore database already exists — continuing."

# Build + deploy (Cloud Build builds the image; no local Docker needed).
"$GCLOUD" run deploy "$SERVICE" \
  --source ./backend \
  --region "$REGION" \
  --memory 256Mi --cpu 1 --max-instances 1 --min-instances 0 \
  --allow-unauthenticated \
  --set-env-vars "CRON_SECRET=$CRON_SECRET,GEMINI_API_KEY=$GEMINI_API_KEY,TELEGRAM_BOT_TOKEN=$TELEGRAM_BOT_TOKEN,TELEGRAM_CHAT_ID=$TELEGRAM_CHAT_ID,AMAZON_TAG=${AMAZON_TAG:-},CUELINKS_ID=${CUELINKS_ID:-},PINTEREST_TOKEN=${PINTEREST_TOKEN:-},PINTEREST_BOARD_ID=${PINTEREST_BOARD_ID:-}" \
  --project="$PROJECT_ID"

URL=$("$GCLOUD" run services describe "$SERVICE" --region "$REGION" \
  --format='value(status.url)' --project="$PROJECT_ID")
echo "Service URL: $URL"

# Scheduler: every 2 hours, GET /run?key=CRON_SECRET
"$GCLOUD" scheduler jobs delete tech-deals-run --location="$REGION" \
  --project="$PROJECT_ID" --quiet 2>/dev/null || true
"$GCLOUD" scheduler jobs create http tech-deals-run \
  --schedule="0 */2 * * *" --uri="$URL/run?key=$CRON_SECRET" \
  --http-method=GET --location="$REGION" --project="$PROJECT_ID"

# Point the GitHub Pages site at the live backend, then commit + push.
sed -i "s|__API_URL__|$URL/deals|g" index.html
echo "index.html now points at $URL/deals"
echo "Next: git add -A && git commit -m 'Point site at live backend' && git push"
echo "Then enable GitHub Pages from the main branch and test: curl '$URL/run?key=***'"
