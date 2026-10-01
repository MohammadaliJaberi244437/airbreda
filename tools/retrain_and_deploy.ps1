# Retrain the NO2 model on everything collected so far, redeploy the dashboard, update the
# document numbers and push. One command, safe to re-run.
# Usage: powershell -ExecutionPolicy Bypass -File D:\Myself\airbreda\tools\retrain_and_deploy.ps1
param([switch]$NoDeploy, [switch]$NoPush)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)

# Secrets are loaded into this process only; nothing is printed.
Get-Content .env | ForEach-Object { if ($_ -match '^([A-Z0-9_]+)=(.*)$') { [Environment]::SetEnvironmentVariable($matches[1], $matches[2]) } }

Write-Host "== training data"; python build_training_data.py; if ($LASTEXITCODE) { throw 'build_training_data.py failed' }
Write-Host "== model";         python train_model.py;         if ($LASTEXITCODE) { throw 'train_model.py failed' }
Write-Host "== tests";         python -m pytest -q;           if ($LASTEXITCODE) { throw 'tests failed, nothing deployed' }
Write-Host "== document";      python tools/fill_model_numbers.py; if ($LASTEXITCODE) { throw 'fill_model_numbers.py failed' }

$meta = Get-Content model_meta.json -Raw | ConvertFrom-Json
$rows = $meta.n_rows

if (-not $NoDeploy) {
    Write-Host "== deploy to the VM"
    $key = "$env:USERPROFILE\.ssh\airbreda-key.pem"; $vm = 'ec2-user@13.61.230.95'
    $files = @('dashboard.py', 'predict.py', 'features.py', 'common.py', 'model.pkl', 'model_meta.json',
               'Dockerfile.dashboard', 'requirements-dashboard.txt', '.dockerignore')
    scp -q -i $key $files "${vm}:~/airbreda/"
    if ($LASTEXITCODE) { throw 'scp failed' }
    ssh -i $key $vm "cd ~/airbreda && docker build -q -t airbreda-dashboard -f Dockerfile.dashboard . >/dev/null && docker rm -f dashboard >/dev/null && docker run -d --name dashboard --restart unless-stopped --env-file .env -p 8000:8000 --memory 450m airbreda-dashboard >/dev/null && sleep 8 && docker image prune -f >/dev/null && docker ps --format '{{.Names}} {{.Status}}'"
    if ($LASTEXITCODE) { throw 'remote build or restart failed' }
    $live = (Invoke-WebRequest 'http://13.61.230.95:8000/site/hrl' -UseBasicParsing -TimeoutSec 30).Content | ConvertFrom-Json
    Write-Host ("live: model_n_rows={0} predicted={1} risk={2}" -f $live.model_n_rows, $live.no2_ug_m3_predicted, $live.no2_exceedance_risk)
    if ($live.model_n_rows -ne $rows) { throw "VM serves a model with $($live.model_n_rows) rows, expected $rows" }
}

if (-not $NoPush) {
    Write-Host "== commit and push"
    $msg = Join-Path $env:TEMP 'airbreda_retrain_msg.txt'
    "model: retrain on $rows hourly rows and update the document numbers`n" | Set-Content -Encoding ascii $msg
    git add -A
    git commit -q -F $msg
    # The repo belongs to the BUas account; gh may have another account active. The push runs
    # in Git Bash because PowerShell mangles the quotes of an inline credential helper.
    $bash = Join-Path (Split-Path (Split-Path (Get-Command git).Source)) 'bin\bash.exe'
    & $bash tools/push.sh
    if ($LASTEXITCODE) { throw 'push failed: the commit is local only, run tools/push.sh in Git Bash' }
    Write-Host "GitHub Pages rebuilds in about a minute"
}
Write-Host "done: $rows rows"
