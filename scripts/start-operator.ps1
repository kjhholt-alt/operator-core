# start-operator.ps1 - launch operator-core daemon with logging.
#
# Invoked by Task Scheduler every 5 min via hidden-run.vbs.
# Self-guards via a pid file: if the existing pid is alive, exit immediately.
# If the pid is dead or missing, start a fresh daemon.
# ASCII-only - Windows PS 5.1 reads .ps1 as cp1252 without BOM.

$ErrorActionPreference = "Stop"

$projectDir = "C:\Users\Kruz\Desktop\Projects\operator-core"
$logDir     = Join-Path $env:USERPROFILE ".operator\logs"
$stdoutLog  = Join-Path $logDir "daemon.out.log"
$stderrLog  = Join-Path $logDir "daemon.err.log"
$pidFile    = Join-Path $env:USERPROFILE ".operator\daemon.pid"

if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}

function Write-Log($Line) {
    $stamp = Get-Date -Format "yyyy-MM-ddTHH:mm:ssK"
    Add-Content -Path $stdoutLog -Value "[$stamp] $Line"
}

# Guard: if daemon is already running, do nothing.
if (Test-Path $pidFile) {
    $existingPid = Get-Content $pidFile -ErrorAction SilentlyContinue
    if ($existingPid -and ($existingPid -match '^\d+$')) {
        $alive = Get-Process -Id $existingPid -ErrorAction SilentlyContinue
        if ($alive) {
            # Still running. Nothing to do.
            exit 0
        }
    }
}

Set-Location $projectDir
$env:PYTHONPATH = Join-Path $projectDir "src"

Write-Log "starting operator-core daemon"

$proc = Start-Process `
    -FilePath "py" `
    -ArgumentList @("-m", "operator_core.cli", "run") `
    -WorkingDirectory $projectDir `
    -RedirectStandardOutput $stdoutLog `
    -RedirectStandardError  $stderrLog `
    -WindowStyle Hidden `
    -PassThru

Set-Content -Path $pidFile -Value $proc.Id
Write-Log "daemon pid=$($proc.Id)"
