#!/usr/bin/env bash
# Deploys Marquee to Cloud Run, storing sensitive values in Secret
# Manager rather than passing them as plain env vars.
#
# Usage:
#   ./scripts/deploy.sh
#
# Requires: gcloud CLI authenticated (`gcloud auth login`), a GCP project
# set (`gcloud config set project <id>`), and a local `.env` file filled in
# (see .env.example).
set -euo pipefail

SERVICE_NAME="marquee"
REGION="${REGION:-us-central1}"

if [ ! -f .env ]; then
  echo "✗ No .env file found. Copy .env.example to .env and fill it in first."
  exit 1
fi

# shellcheck disable=SC1091
source .env

PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
if [ -z "$PROJECT_ID" ]; then
  echo "✗ No GCP project set. Run: gcloud config set project <your-project-id>"
  exit 1
fi

echo "→ Project: $PROJECT_ID  |  Region: $REGION"

echo "→ Enabling required APIs (safe to re-run)..."
gcloud services enable run.googleapis.com secretmanager.googleapis.com \
  aiplatform.googleapis.com --quiet

create_or_update_secret() {
  local name="$1"
  local value="$2"
  if gcloud secrets describe "$name" >/dev/null 2>&1; then
    echo -n "$value" | gcloud secrets versions add "$name" --data-file=- --quiet
  else
    echo -n "$value" | gcloud secrets create "$name" --data-file=- --replication-policy=automatic --quiet
  fi
}

echo "→ Writing secrets to Secret Manager..."
create_or_update_secret "marquee-clickhouse-host" "$CLICKHOUSE_HOST"
create_or_update_secret "marquee-clickhouse-password" "$CLICKHOUSE_PASSWORD"
create_or_update_secret "marquee-google-api-key" "${GOOGLE_API_KEY:-}"

# MARQUEE_API_KEY is optional — only wire it up if you've actually set one
# locally. Without this step, setting it in your local .env would silently
# have no effect on the deployed instance, which would stay wide open.
SECRETS="CLICKHOUSE_HOST=marquee-clickhouse-host:latest,CLICKHOUSE_PASSWORD=marquee-clickhouse-password:latest,GOOGLE_API_KEY=marquee-google-api-key:latest"
if [ -n "${MARQUEE_API_KEY:-}" ]; then
  create_or_update_secret "marquee-api-key" "$MARQUEE_API_KEY"
  SECRETS="${SECRETS},MARQUEE_API_KEY=marquee-api-key:latest"
  echo "→ MARQUEE_API_KEY is set locally — auth will be required on the deployed instance too."
else
  echo "→ MARQUEE_API_KEY is not set locally — deployed instance will have /chat and /replay-scenario open, same as local."
fi

echo "→ Deploying to Cloud Run..."
gcloud run deploy "$SERVICE_NAME" \
  --source . \
  --region "$REGION" \
  --allow-unauthenticated \
  --set-env-vars "CLICKHOUSE_PORT=${CLICKHOUSE_PORT:-8443},CLICKHOUSE_USER=${CLICKHOUSE_USER:-default},CLICKHOUSE_DATABASE=${CLICKHOUSE_DATABASE:-default},CLICKHOUSE_SECURE=${CLICKHOUSE_SECURE:-true},FILM_TITLE=${FILM_TITLE:-Midnight Fracture},MARQUEE_RATE_LIMIT_PER_MINUTE=${MARQUEE_RATE_LIMIT_PER_MINUTE:-30}" \
  --set-secrets "$SECRETS" \
  --memory 512Mi \
  --min-instances 0

echo "✓ Deployed. Fetching service URL..."
gcloud run services describe "$SERVICE_NAME" --region "$REGION" --format 'value(status.url)'
