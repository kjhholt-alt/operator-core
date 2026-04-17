# snapshot-once.ps1 - publish one snapshot to Supabase.
#
# Belt-and-suspenders: even if the daemon is down, snapshots still publish.
# Invoked by Task Scheduler every 30 minutes via hidden-run.vbs.
# ASCII-only.

$ErrorActionPreference = "Continue"

$projectDir = "C:\Users\Kruz\Desktop\Projects\operator-core"
$logDir     = Join-Path $env:USERPROFILE ".operator\logs"
$stdoutLog  = Join-Path $logDir "snapshot.out.log"

if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}

Set-Location $projectDir
$env:PYTHONPATH = Join-Path $projectDir "src"
if (-not $env:OPERATOR_NODE) { $env:OPERATOR_NODE = "kruz" }

# Pull Supabase creds from prospector-pro/.env.local if not already set.
$envFile = "C:\Users\Kruz\Desktop\Projects\prospector-pro\.env.local"
if ((Test-Path $envFile) -and (-not $env:SUPABASE_SERVICE_ROLE_KEY)) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*SUPABASE_URL\s*=\s*(.+?)\s*$') {
            $env:SUPABASE_URL = $matches[1].Trim('"').Trim("'")
        }
        if ($_ -match '^\s*SUPABASE_SERVICE_ROLE_KEY\s*=\s*(.+?)\s*$') {
            $env:SUPABASE_SERVICE_ROLE_KEY = $matches[1].Trim('"').Trim("'")
        }
    }
}

$stamp = Get-Date -Format "yyyy-MM-ddTHH:mm:ssK"
Add-Content -Path $stdoutLog -Value "[$stamp] snapshot cron tick"

$out = & py -m operator_core.cli snapshot 2>&1
Add-Content -Path $stdoutLog -Value $out
