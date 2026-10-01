-- AirBreda ingestion schema. Safe to re-run.

-- The course template uses VARCHAR(20) for station_id, but NDW site ids are 27 chars
-- (e.g. RWS01_MONIBAS_0271hrl0063ra), so it is widened to 40.
CREATE TABLE IF NOT EXISTS sensor_readings (
    station_id VARCHAR(40)      NOT NULL,
    timestamp  TIMESTAMPTZ      NOT NULL,
    component  VARCHAR(20)      NOT NULL,
    value      DOUBLE PRECISION,
    is_flagged BOOLEAN          NOT NULL DEFAULT FALSE,
    PRIMARY KEY (station_id, timestamp, component)
);

-- The ingestion containers exit after each run, so in-memory counters reset and they
-- cannot serve /health themselves. Each run persists its status here, and the
-- dashboard's /health endpoint reads it later.
CREATE TABLE IF NOT EXISTS ingestion_runs (
    id               BIGSERIAL   PRIMARY KEY,
    source           VARCHAR(20) NOT NULL,
    run_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    success          BOOLEAN     NOT NULL,
    last_measurement TIMESTAMPTZ,
    rows_written     INT         NOT NULL DEFAULT 0,
    bad_data_count   INT         NOT NULL DEFAULT 0,
    error            TEXT
);

CREATE INDEX IF NOT EXISTS ingestion_runs_source_run_at ON ingestion_runs (source, run_at DESC);
