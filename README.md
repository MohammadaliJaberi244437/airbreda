# AirBreda

AirBreda relates traffic on the A27 near Breda to nitrogen dioxide (NO2) in the city. It ingests hourly NO2 from Luchtmeetnet (station NL10240, Breda-Tilburgseweg) and NDW traffic data for four A27 sites near hectometer 63 (both mainline carriageways and the entry and exit slip roads), stores them on AWS, and serves a dashboard with the latest measurements and a simple NO2 prediction with an exceedance risk.

Course project, Breda University of Applied Sciences (BUas), MohammadAli Jaberi, October 2026.

Architecture Design Document (ADRs, diagram, costs, reflection): https://mohammadalijaberi244437.github.io/airbreda/

## Architecture

All on AWS, Region eu-north-1 (Stockholm):

- **EC2 t3.micro** (Amazon Linux 2023) runs three Docker images. cron starts two run-and-exit ingestion jobs: `airbreda-air` hourly at :25 UTC and `airbreda-traffic` every 10 minutes. `airbreda-dashboard` (FastAPI) runs all the time with `--restart unless-stopped`; the trained model is baked into the image.
- **RDS PostgreSQL 18** (db.t4g.micro): `sensor_readings` (primary key station_id, timestamp, component) and `ingestion_runs` (one row per job run, used by `/health`).
- **S3** bucket: `raw/ndw/` holds every raw NDW feed, uploaded before parsing; `ndw/YYYY-MM-DD/HH-{site}.csv` holds one CSV per site per hour.
- **Access:** the VM uses instance role `airbreda-ec2-role` (ListBucket, GetObject and PutObject on this bucket only, no keys on the VM). Security groups allow SSH and the dashboard port 8000 only from the developer IP (public access to the dashboard needs an extra inbound rule, 8000 from 0.0.0.0/0), and PostgreSQL only from the VM's security group and the developer IP. The bucket has Block Public Access on and no bucket policy.

Data quality: Luchtmeetnet values that are null or stuck for 3 or more hours are kept with `is_flagged = TRUE`. NDW speed `-1` and lanes marked `dataError` are logged as `DATA_QUALITY_ERROR` and the speed row is not stored. Each run records its bad-data count in `ingestion_runs`.

## Endpoints (port 8000)

| Endpoint | Returns |
|---|---|
| `GET /` | HTML dashboard built on `/site`, refreshes every 60 s, warns when data is stale |
| `GET /site/{site_id}` | `site_id` is `hrl`, `hrr`, `vwd` or `vwa`: latest NO2, the site's and the total intensity, predicted NO2, `no2_exceedance_risk`, model metadata, `prediction_error`. If the prediction fails, the real values are still returned with a null prediction. 404 for an unknown site, 503 if the database or S3 is unreachable. |
| `GET /health` | `ok`, or `degraded` when Luchtmeetnet has had no successful run for 2 hours or NDW for 30 minutes; the last successful fetch and the last-hour bad-data count per source |

## Run locally

Requires Python 3.10 or newer and Docker Desktop. Copy `.env.example` to `.env` and fill it in. Never commit `.env`.

```
pip install -r requirements-dev.txt
python -m pytest -q                      # unit tests, no cloud access needed
aws login                                # short-lived AWS credentials
pwsh tools/compose-up.ps1                # builds and runs all three containers against the real DB and bucket
```

The dashboard is then on http://localhost:8000.

Optional Redis messaging prototype. The two ingestion jobs run once against the real database and bucket and also publish each reading to the Redis list `readings`. Its services read `.env.aws`, which `tools/compose-up.ps1` deletes when it exits, so create it from the `aws login` session first and delete it afterwards (PowerShell; the file holds short-lived credentials and is git- and docker-ignored):

```
aws login
aws configure export-credentials --format env-no-export | Set-Content -Encoding ascii .env.aws
docker compose -f docker-compose.redis.yml up --build -d
docker exec redis redis-cli LLEN readings
docker compose -f docker-compose.redis.yml down
Remove-Item .env.aws
```

Train the model (`build_training_data.py` writes `training_data.csv` and redraws `docs/img/no2_vs_intensity.png`; `train_model.py` writes `model.pkl` and `model_meta.json`, which the dashboard image copies in, so rebuild the dashboard image afterwards):

```
python build_training_data.py
python train_model.py
```

## Deploy to the VM

Launch Amazon Linux 2023 with `infra/user-data.sh` (it installs Docker, cronie, git and the psql client and adds swap) and the instance profile `airbreda-ec2-profile` (role `airbreda-ec2-role`). Then on the VM:

```
git clone <repo> ~/airbreda && cd ~/airbreda
# create .env, then create the tables once: psql ... -f schema.sql
mkdir -p logs
docker build -t airbreda-air -f Dockerfile .
docker build -t airbreda-traffic -f Dockerfile.traffic .
docker build -t airbreda-dashboard -f Dockerfile.dashboard .
crontab infra/crontab.txt
docker run -d --name dashboard --restart unless-stopped --env-file .env -p 8000:8000 airbreda-dashboard
```

Job logs are JSON lines (one JSON object per line) in `~/airbreda/logs/air.log` and `~/airbreda/logs/traffic.log`; the dashboard's request log, with status and duration per request, is in `docker logs dashboard`.

## Repository layout

```
ingest_air.py              Luchtmeetnet NO2 job (runs once and exits)
ingest_traffic.py          NDW traffic job (runs once; --backfill DIR loads laptop captures)
common.py                  logging, database, S3 and HTTP retry helpers
broker.py                  optional Redis publishing, only when REDIS_HOST is set
features.py                feature code shared by training and serving
build_training_data.py     joins NO2 (RDS) with traffic CSVs (S3) into training_data.csv
train_model.py             fits LinearRegression, writes model.pkl and model_meta.json
predict.py                 prediction and no2_exceedance_risk
dashboard.py               FastAPI dashboard: /, /site/{site_id}, /health
schema.sql                 sensor_readings and ingestion_runs
Dockerfile                 air ingestion image
Dockerfile.traffic         traffic ingestion image
Dockerfile.dashboard       dashboard image
docker-compose.yml         local test of all three services
docker-compose.redis.yml   Redis messaging prototype
infra/                     EC2 user data, crontab, IAM policy and trust documents
tools/                     capture_ndw.py (bootstrap capture), compose-up.ps1
tests/                     pytest suite
docs/                      GitHub Pages site with the Architecture Design Document
```
