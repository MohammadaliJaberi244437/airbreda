---
layout: default
title: AirBreda lab notebook
permalink: /notebook/
---

# AirBreda lab notebook

Reflection, estimation and wrap-up answers for Days 1 to 4 of System Design and Cloud Platforms, written by me, MohammadAli Jaberi, to prepare the presentation. Every number was checked on 1 October 2026 at about 11:10 UTC against the live Luchtmeetnet API, the RDS database, the S3 bucket, the AWS account and the dashboard at [http://13.61.230.95:8000](http://13.61.230.95:8000). The design is in the [Architecture Design Document](https://mohammadalijaberi244437.github.io/airbreda/); this page is the thinking behind it.

## Day 1: storage and the first data

### Zalando: concern and excitement

*What was your biggest concern and your biggest excitement after the Zalando story?*

My concern was autonomy without a shared platform: hundreds of teams each picking a stack, an account and a deployment path means hundreds of security setups to audit and the same code written many times. My excitement was ownership: a team that runs its service end to end ships in hours. In miniature today: with one account I could deploy anything in minutes, and nothing stopped me deploying something wrong until a test run on every push was added.

### FinOps: the idle Kubernetes cluster

*A EUR 500 cluster runs one job from 08:00 to 20:00. How much is wasted, and what is the cloud-native fix?*

The job uses 12 of 24 hours, so half the cluster, EUR 250 a month, pays for idle nodes; on weekdays only it uses 60 of 168 hours and the waste is 64%, about EUR 321. Fix: run it as a Kubernetes CronJob on a Spot node group that Karpenter scales to zero. If the cluster exists only for this job, leave Kubernetes for EventBridge Scheduler plus a Fargate task, billed per second. My jobs run 3 to 5 seconds, 5,110 times a month, so cron on one EUR 11.34 t3.micro wins.

### Lab 1: fields in a record

*What fields does one measurement record contain?*

Five: `value`, `timestamp_measured`, `formula`, `timestamp_measured_start` and `timestamp_measured_end`. The newest record at 11:09:56 UTC was `{"value": 22.68, "timestamp_measured": "2026-10-01T10:00:00+00:00", "formula": "NO2", "timestamp_measured_start": "2026-10-01T09:00:00+00:00", "timestamp_measured_end": "2026-10-01T10:00:00+00:00"}`. Missing: a unit (ug/m3 is implied), the station id (it is in the URL) and any quality flag, so a frozen sensor looks like a working one. Pagination reports 50 records per page and 2,179 pages: about 109,000 hourly values, roughly 12 years.

### Lab 1: time resolution

*What is the time resolution?*

Hourly: in every record start and end are one hour apart and `timestamp_measured` equals the end, so the value labelled 10:00 is the mean of 09:00 to 10:00. Timestamps are UTC (+00:00); the hour labelled 10:00 UTC is 11:00 to 12:00 local, and ingestion stores it as given. So an NDW sample measured at 09:30 UTC (11:30 local) belongs to the NO2 row labelled 10:00; `features.no2_window_label` encodes that once for training and serving.

### Lab 1: an unmeasured formula

*What happens when you ask for a formula the station does not measure?*

With `formula=C6H6` the API returns HTTP 200, `"data": []` and `last_page: 0, page_list: [], next_page: 0`: no 404, no error. Without a `formula`, page 1 holds NO, NO2, NOx, PM10 and PM25, while the station endpoint lists only PM25, NO, PM10 and NO2. A successful request is not data: `ingest_air.py` raises `response contained no NO2 rows` on an empty list and records a failed run instead of silently writing nothing.

### Lab 1: how recent

*How recent is the latest reading compared with when you ran the call?*

I ran the call at 11:09:56 UTC. The newest value, 22.68 ug/m3, is labelled 10:00 UTC (12:00 local), so it was 70 minutes old by its label; the next hour had ended 10 minutes earlier and was not out yet. Luchtmeetnet publishes an hour roughly 20 to 25 minutes after it ends (today's 10:25:04 UTC cron run already held the hour ending 10:00 UTC), hence the `25 * * * *` schedule.

### Spotify: when one Postgres is wrong

*At what scale does a single Postgres become wrong, and which dashboard signals warn you?*

AirBreda writes about 100 rows an hour into a 104 kB table. One Postgres becomes wrong when the working set no longer fits in memory (not 0.09 GB a year), when writes reach tens of thousands of rows a second, when writes must happen in more than one Region, or when the availability target exceeds one primary with Multi-AZ failover. On RDS I would watch CPUUtilization and CPUCreditBalance, write and read latency p99, DiskQueueDepth against the gp3 IOPS baseline, FreeStorageSpace, and dead tuples on `sensor_readings`, since the air job rewrites 50 rows every hour.

### Checkpoint 1: least defensible decision

*Which Checkpoint 1 decision would you be least confident defending?*

On Day 1, RDS PostgreSQL: 0.09 GB a year at EUR 15.61 a month, 55% of the bill. I can defend the query shapes (about 0.1 ms for the latest-NO2 lookup, 0.07 to 0.13 ms across EXPLAIN ANALYZE runs), but an examiner could ask why SQLite on the VM with an hourly copy to S3 would not do the same for zero euros, and my honest answer is backups and point-in-time restore. By Day 4 it had moved to the assumption that A27 traffic plus hour of day explains NO2 at Breda-Tilburgseweg, chosen because I could build it in a day, not because data showed it.

### Wrap-up: before agreeing to DynamoDB

*Which three questions do you ask before agreeing to DynamoDB for this workload?*

1. What are all the access patterns? Latest NO2 per station is a key lookup; the last hour's bad-data sum (`/health`) and the skipped speed rows become queries plus code, or scans.
2. What does it cost at our volume? About 100 writes an hour fit in the free tier, so it would be cheaper than RDS; I must show EUR 15.61 buys enough SQL convenience.
3. Who maintains the data model in a year? RIVM revises past values, and a retrain must read a year of NO2 and traffic without a scan.

### Wrap-up: rows after 5 years

*How many rows after 5 years with 10 stations (show the arithmetic), and does it change the database choice?*

Per corridor and hour: 1 NO2 row plus 4 NDW sites x 2 metrics x 6 samples = 49 rows; per year 49 x 8,760 = 429,240. I read "10 stations" as 10 corridors, each with its own station and four NDW sites: 10 x 429,240 x 5 = 21,462,000 rows, or 4.3 GB at 200 bytes a row. The choice stands: 21 million narrow rows is routine for PostgreSQL and the primary key keeps the latest lookup below a millisecond. What changes is housekeeping: more storage, monthly partitions and a materialised hourly view.

### Wrap-up: latest NO2 and nulls

*What is the most recent NO2 value right now, what does a null look like, and what does the pipeline do with it?*

At 11:10 UTC on 1 October 2026 it is 22.68 ug/m3 for the hour ending 12:00 local (10:00 UTC); API, database and `/site/hrl` agree. I had not seen a null by then: none of the 50 API records or the 51 database rows at that time were null or flagged. A null arrives as `"value": null` in an otherwise normal record: `flag_bad_readings` turns it into NaN and sets `is_flagged = TRUE`, and `to_db_rows` converts NaN to `None` for a real SQL NULL. The row is kept and counted in `bad_data_count`; the dashboard skips nulls (`value IS NOT NULL`) and training excludes them.

## Day 2: messaging, resilience and data quality

### Shopify: one codebase or two

*One codebase or two services for the two ingestion scripts, and what would change the answer in 12 months?*

One codebase, which is what I have: both scripts share `common.py`, `broker.py` and the `ingestion_runs` contract, while operationally they are already two services (separate images and cron lines). Splitting now would give me two copies of the retry logic and two places to fix the `.env` loader bug. In 12 months the answer changes if a second team owns traffic, release cadences diverge, or 50 corridors make traffic a fleet of workers. Then I would split the repository but publish `features.py` and the schema as a versioned package.

### us-east-1: downtime and SLO

*How much downtime is acceptable for AirBreda, and what is a realistic SLO for /site/{id}?*

The dashboard is read a few times a day with no safety decision behind it, so a day down is harmless; ingestion is different, because NDW keeps no history, so every hour the traffic job is down is data gone for good. ADR-003 sets `/site/{id}` at 99% of requests per rolling 30 days answered 200 within 2 seconds, a 7.2-hour budget per 30 days (ADR-003 rounds it to 7.3 hours on a 730-hour month), enough for a 1-to-2-hour manual VM rebuild. I did not choose 99.5%: its 3.65 hours would go to one bad deploy or one AZ incident, and meeting it means Multi-AZ RDS, two load-balanced instances and alerting.

### Lab Extend: the polling interval

*Justify the polling interval. What would break at every minute?*

Luchtmeetnet publishes one value an hour about 20 minutes after it ends, so I poll hourly at :25 UTC: 24 requests a day against a fair-use limit of 100 per 5 minutes. Every minute stays under the limit for one station but fetches the same 50 rows 1,440 times a day and upserts 72,000 rows instead of 1,200 for no new information. NDW is different: 1-minute rates refreshed every minute, so more samples add information. Six an hour give a steady mean and satisfy the shared coverage rule (3 samples over 30 minutes); every minute would download 60 x 1.18 MB an hour, about 620 GB a year instead of 170 MB a day.

### Wrap-up: seven microservices

*Which three questions do you ask before splitting AirBreda into seven microservices?*

1. Which problem does the split solve that three containers on one VM do not: independent deploys, scaling or team ownership?
2. Where do the data boundaries fall? If each service owns its store, the NO2-traffic join becomes network calls, and the one rule against training-serving skew becomes seven copies that can drift.
3. What does running seven services cost? A registry, seven pipelines, a message bus, tracing and someone on call, against EUR 11.34 a month of compute; Fargate with a load balancer was already triple the VM (ADR-004).

### Wrap-up: bad_data_count = 47

*bad_data_count is 47 in the last hour: what do you check first, alert on, and let through?*

NDW can produce at most 6 runs x 4 sites x 2 metrics = 48 an hour, so 47 means almost every reading failed: the feed marks every lane `dataError`, or my parser is broken. Check first: `/health` for the source, then the newest raw file in `raw/ndw/` (archived before parsing). Alert on `BAD_DATA_THRESHOLD_EXCEEDED` (above 10 per source per hour), failed runs and `/health` degraded. Let through: Luchtmeetnet nulls and stale values, kept with `is_flagged = TRUE`; NDW speed `-1`, dropped while intensity and the raw file are kept. By 11:10 UTC on 1 October that had happened four times, all on the exit slip road (09:11, 09:22, 09:36, 10:26 UTC), each in a lane with zero flow: no car that minute, not a broken sensor; it keeps recurring a few times a day.

### Wrap-up: the error budget

*Minutes allowed per month at 99.5%, and how much of the annual budget does a 5-hour outage burn?*

A 30-day month has 43,200 minutes; 0.5% is 216 minutes, 3.6 hours. A year has 8,760 hours; 0.5% is 43.8 hours, so a 5-hour outage burns 5 / 43.8 = 11.4% of the annual budget. For the 99% SLO I chose: 432 minutes (7.2 hours) per 30 days, 87.6 hours a year, and the same outage is 5.7%. At 99.5% one incident a month leaves nothing for deploys.

## Day 3: compute and cost

### Cost check

*What did today's work cost?*

About USD 1 of Free plan credits. List price is USD 1.07 a day (EUR 28.58 a month, with the bucket at its month-12 size): t3.micro 0.0108 x 24 = USD 0.26, db.t4g.micro 0.016 x 24 = USD 0.38, two public IPv4 addresses 2 x 0.005 x 24 = USD 0.24, EBS and RDS storage about USD 0.12, and S3 about USD 0.06 at the month-12 size (today, with one day of raw files in the bucket, the S3 line is about a cent and the day costs about USD 1.00). The Free plan is credit-based, so the card is never charged and the USD 10 monthly budget (`infra/budget.json`) shows USD 0.00 actual spend. Two findings: the database's public IPv4 (13.63.11.20) appears in the account as an Elastic IP on the RDS network interface and costs USD 3.65 a month, the same as the VM's; making RDS not publicly accessible would save it, at the price of running psql only from the VM. And the raw archive grew by 16 files (18.9 MB) today, on track for 63 GB a year.

### An unanticipated operational concern

*Which operational concern did you not anticipate?*

That Amazon Linux 2023 ships without cron: my design was "cron starts two containers" and the fresh VM had no `crond`, so `infra/user-data.sh` now installs `cronie` and enables `docker` and `crond` at boot. Others: the AWS sign-up flow locked the account to Stockholm; containers got no instance-role credentials until the IMDSv2 hop limit became 2; the 147 MB NDW configuration XML would not fit in 1 GiB parsed, so it is archived unparsed; NDW `dataError` lanes carry placeholder zeros that would read as zero traffic; and `VARCHAR(20)` was too short for NDW's 27-character site ids, widened to 40.

## Day 4: the model and the dashboard

### Rules of ML: a more complex model

*What would you need to see in the data before trusting a more complex model?*

At 13:16 on 1 October (Amsterdam time) the model was linear regression on one training row (the hour ending 10:00 UTC), so both coefficients were 0.0 and it predicted the intercept, 22.68 ug/m3; the retrain before the deadline adds every hour collected since, and the current numbers are in ADR-006 of the design document. Before trusting anything more complex I would need: thousands of hourly rows, so traffic and hour of day stop being confounded; a time-based hold-out (never random: neighbouring hours are near copies) in which the complex model beats both linear regression and a naive same-hour-last-week baseline; a traffic coefficient with the expected positive sign; KNMI wind and temperature available at serving time; and a calibration check of the risk score against real hours above 40.

### Wrap-up: 31 rows and a random forest

*You have 31 rows and a teammate wants a random forest. What do you say?*

No. Thirty-one rows is one weekday and a bit, with no weekend and no weather variation. A forest of 100 trees would memorise 31 points and report a near-zero training error, and trees cannot extrapolate: outside the traffic range of those hours it predicts a constant. Instead: keep linear regression as the baseline, let `train_model.py` report the leave-one-out MAE next to a naive previous-hour baseline, and revisit the forest when a time-based split on thousands of rows shows it earns its complexity.

### Wrap-up: a blank NO2 field

*The dashboard shows a prediction but the NO2 field is blank. What do you check first, second, third?*

A prediction without a measurement means the traffic path, the model and the database all answered, but `latest_no2()` returned no row. First, the database: `SELECT timestamp, value FROM sensor_readings WHERE station_id = 'NL10240' AND component = 'NO2' ORDER BY timestamp DESC LIMIT 5`; the dashboard filters `value IS NOT NULL`, so blank means no non-null row exists. Second, ingestion: the last `Luchtmeetnet` rows in `ingestion_runs` and `~/airbreda/logs/air.log`, for `success = false` or a `fetch_failed` event. Third, the source: call the Luchtmeetnet URL by hand, since an empty `data` list or a 503 would fail every run.

### Wrap-up: model.pkl in the image

*model.pkl is baked into the image: what does that gain, and what does it cost at retrain time?*

Gain: code and model ship as one versioned artifact. The pickle comes from the same `features.py` and pinned scikit-learn (1.7.2) that serve it, so feature code and model cannot drift apart, and rollback is the previous commit. Cost: every retrain is a deploy: `train_model.py` on the laptop, `scp` to the VM, `docker build`, stop and start the container, seconds of downtime. When retraining becomes daily I would load the model from S3 at startup, version-checked against the feature code, trading a startup dependency for retrains without a rebuild.

### Wrap-up: the overnight reboot

*The VM reboots overnight. Which containers come back, and what did you add to make that true?*

Everything, and I added nothing: Day 3 already covered it. The dashboard runs with `--restart unless-stopped` and `docker` is enabled at boot in `infra/user-data.sh`, so Docker restarts it. The ingestion jobs need no restart: `crond` is also enabled at boot and the crontab lives on the VM, so the next :x0 and :25 slots run. A real reboot today: the dashboard answered again after about 75 seconds, and `ingestion_runs` shows no gap longer than 11 minutes between NDW runs all day, so not one 10-minute slot was missed.
