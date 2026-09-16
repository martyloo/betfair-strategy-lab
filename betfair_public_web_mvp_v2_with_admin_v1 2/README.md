# Betfair Strategy Lab — Public Web MVP v2

Public visitors never enter a Betfair token and cannot trigger historical downloads. The app automatically lists existing processed Parquet objects in R2 using date/country/plan partition prefixes.

## Features
- persistent result cache in R2
- persistent job metadata and unique job IDs
- configurable simultaneous worker pool
- automatic processed-Parquet lookup
- polished responsive web UI
- no public Betfair-token or download controls

Expected processed path:
`processed/horse-racing/win/<plan>/year=YYYY/month=MM/country=CC/*.parquet`

Result path:
`results/horse-racing/win/public-web-v2.1/<strategy-hash>.json`

Job path:
`jobs/horse-racing/win/public-web-v2.1/<job-id>.json`

## Run locally
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export R2_ACCESS_KEY_ID="..."
export R2_SECRET_ACCESS_KEY="..."
export R2_ENDPOINT="..."
export R2_BUCKET="betfair-historical-data"
export BACKTEST_WORKERS=4
uvicorn app.main:app --reload
```

Open `http://127.0.0.1:8000`.

For a single deployed application instance, the worker pool supports simultaneous users. Job state and completed results persist in R2. For multiple application instances with guaranteed recovery of jobs that are in-flight during a restart, the next production step is a durable external queue/worker.

## Admin Data Manager v1

Private URL: `/admin`

Required server-only variables:

```bash
export BETFAIR_SESSION_TOKEN="..."
export ADMIN_PASSWORD="choose-a-strong-password"
export ADMIN_SESSION_SECRET="a-long-random-secret"
```

For HTTPS production also set:

```bash
export COOKIE_SECURE=1
```

The admin ingestion path is resumable:
1. `processed-index/.../<source-id>.json` + existing Parquet => skip.
2. Existing raw R2 object => reuse raw without Betfair download.
3. Otherwise download the source from Betfair, upload raw to R2.
4. Parse final CLOSED WIN marketDefinition, create Zstandard Parquet, upload it, then write the processed index.

### Automatic updater

Disabled by default. Enable on the deployed server with:

```bash
export AUTO_UPDATE_ENABLED=1
export AUTO_UPDATE_COUNTRIES=GB,IE
export AUTO_UPDATE_PLAN="Basic Plan"
export AUTO_UPDATE_LOOKBACK_DAYS=7
export AUTO_UPDATE_INTERVAL_HOURS=24
```

The updater deliberately rechecks an overlapping lookback window; resumability means already-completed objects are skipped.

### Production note

The built-in updater is suitable for a single always-on application instance. When deploying multiple web instances, use exactly one dedicated ingestion worker or an external scheduler to avoid duplicate updater loops.
