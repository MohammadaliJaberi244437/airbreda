# Sets the real RDS master password (typed by you, never shown) and writes .env for the app.
# The database was created with a random throwaway password that nobody knows.
# Run:  powershell -ExecutionPolicy Bypass -File D:\Myself\airbreda\infra\set-db-password.ps1

$ErrorActionPreference = 'Stop'
$aws = 'C:\Program Files\Amazon\AWSCLIV2\aws.exe'
$envFile = Join-Path (Split-Path $PSScriptRoot -Parent) '.env'

Write-Host "Waiting for the database to finish starting (can take up to 10 minutes)..."
& $aws rds wait db-instance-available --db-instance-identifier airbreda-db
$endpoint = & $aws rds describe-db-instances --db-instance-identifier airbreda-db --query "DBInstances[0].Endpoint.Address" --output text

function Read-Plain($prompt) {
    $secure = Read-Host $prompt -AsSecureString
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
    try { [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
    finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
}

while ($true) {
    Write-Host ""
    Write-Host "Choose a database password: 12 or more characters, only letters and numbers."
    $pw = Read-Plain "Password"
    $pw2 = Read-Plain "Type it again"
    if ($pw -ne $pw2) { Write-Host "They do not match, try again."; continue }
    if ($pw -notmatch '^[A-Za-z0-9]{12,41}$') { Write-Host "Use 12-41 letters and numbers only, try again."; continue }
    break
}

Write-Host "Setting the password on the database..."
& $aws rds modify-db-instance --db-instance-identifier airbreda-db --master-user-password $pw --apply-immediately --query "DBInstance.DBInstanceIdentifier" --output text | Out-Null

@"
DB_HOST=$endpoint
DB_PORT=5432
DB_NAME=airbreda
DB_USER=airbreda
DB_PASSWORD=$pw
DB_SSLMODE=require
S3_BUCKET=airbreda-raw-339879235060
AWS_REGION=eu-north-1
"@ | Set-Content -Encoding ascii $envFile
Remove-Variable pw, pw2

Write-Host ""
Write-Host "Done. Password set and saved to $envFile (this file is gitignored, never commit it)."
Write-Host "Save the same password in your password manager too."
