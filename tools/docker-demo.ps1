# Show the three AirBreda containers in Docker Desktop on this laptop.
# They use the same cloud database and bucket as the server, so the numbers are real.
#
#   Start:  powershell -ExecutionPolicy Bypass -File D:\Myself\airbreda\tools\docker-demo.ps1
#   Stop:   powershell -ExecutionPolicy Bypass -File D:\Myself\airbreda\tools\docker-demo.ps1 -Stop
param([switch]$Stop)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)

docker info *> $null
if ($LASTEXITCODE -ne 0) { throw 'Docker Desktop is not running. Open Docker Desktop, wait until it says "Engine running", and try again.' }

if ($Stop) {
    docker compose down
    Remove-Item -Force .env.aws -ErrorAction SilentlyContinue
    Write-Host "Stopped and removed the local containers."
    return
}

# Containers cannot use the laptop's 'aws login' session, so they get its short-lived
# credentials as a file (git- and docker-ignored, never printed, removed by -Stop).
$creds = & 'C:\Program Files\Amazon\AWSCLIV2\aws.exe' configure export-credentials --format env-no-export 2>$null
if ($LASTEXITCODE -ne 0 -or -not $creds) { throw "No AWS session. Run 'aws login' first." }
Set-Content -Path .env.aws -Value $creds -Encoding ascii

docker compose build --quiet   # keep the build log off the screen
if ($LASTEXITCODE -ne 0) { throw 'docker compose build failed.' }
docker compose up -d
if ($LASTEXITCODE -ne 0) { throw 'docker compose up failed.' }

Write-Host ""
Write-Host "In Docker Desktop, open Containers and the group 'airbreda':"
Write-Host "  dashboard        Running   (always on)      http://localhost:8000"
Write-Host "  air-ingest       Exited (0) after a few seconds: a short job"
Write-Host "  traffic-ingest   Exited (0) after a few seconds: a short job"
Write-Host "Click a container, then Logs, to see its JSON log lines."
Write-Host "The bucket access lasts about 15 minutes. Stop everything with:  tools\docker-demo.ps1 -Stop"
