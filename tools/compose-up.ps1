# Local Day 4 test: build the three images, export the current 'aws login' session into
# .env.aws (never printed), start the containers, and remove them again on exit (Ctrl+C).
# Usage: pwsh tools/compose-up.ps1
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)

docker info *> $null
if ($LASTEXITCODE -ne 0) { throw 'Docker is not running (docker info failed). Start Docker Desktop and retry.' }

# Build before exporting: the session lasts about 15 minutes, and a first build (numpy,
# scipy, scikit-learn) would otherwise use up part of it.
docker compose build
if ($LASTEXITCODE -ne 0) { throw 'docker compose build failed.' }

# Containers cannot read the host's 'aws login' token cache, so they get the short-lived
# session credentials as env vars, which they cannot refresh. .env.aws is git- and
# docker-ignored and removed on exit.
$creds = aws configure export-credentials --format env-no-export 2>$null
if ($LASTEXITCODE -ne 0 -or -not $creds) { throw "Could not export AWS credentials. Run 'aws login' first." }
Set-Content -Path .env.aws -Value $creds -Encoding ascii
$expiry = $creds | ForEach-Object { if ($_ -match '^AWS_CREDENTIAL_EXPIRATION=(.+)$') { $matches[1] } }
if ($expiry) {  # the expiry time is not a secret; the keys are never printed
    Write-Host "AWS session credentials expire at $expiry. From then on the containers have no S3 access:"
    Write-Host "/site answers 503 'S3 credentials expired' and traffic-ingest fails. Re-run this script to refresh."
}
try {
    docker compose up
} finally {
    docker compose down  # no container keeps running (or restarts) with credentials that expire
    Remove-Item -Force .env.aws -ErrorAction SilentlyContinue
}
