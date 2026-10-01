# Luchtmeetnet NO2 ingestion. Run-and-exit, triggered by cron on the EC2 host:
#   25 * * * *  docker run --rm --env-file /home/ec2-user/airbreda/.env airbreda-air
# (see infra/crontab.txt for the exact line, including the log redirect)
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements-ingest.txt .
RUN pip install --no-cache-dir -r requirements-ingest.txt \
    && useradd --system --no-create-home ingest

COPY common.py broker.py ingest_air.py ./
USER ingest

CMD ["python", "ingest_air.py"]
