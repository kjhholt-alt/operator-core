# Register-Operator.ps1
#
# Idempotent Task Scheduler registration for operator-core.
# Registers two tasks:
#   - OperatorDaemon:   runs `operator run` at logon, silent via hidden-run.vbs
#   - OperatorSnapshot: runs `operator snapshot` every 30 min, also silent
#
# Also unregisters the legacy OperatorV3Daemon so the new one owns the post.
# ASCII-only. Run from an ELEVATED PowerShell (admin).
#
# Usage:
#   .\scripts\Register-Operator.ps1              # register or update
#   .\scripts\Register-Operator.ps1 -Unregister  # remove both tasks

[CmdletBinding()]
param(
    [switch]$Unregister
)

$ErrorActionPreference = "Stop"

$repoRoot   = Split-Path -Parent $PSScriptRoot
$vbs        = Join-Path $repoRoot "scripts\hidden-run.vbs"
$startPs1   = Join-Path $repoRoot "scripts\start-operator.ps1"
$snapPs1    = Join-Path $repoRoot "scripts\snapshot-once.ps1"
$logDir     = Join-Path $env:USERPROFILE ".operator\logs"
$schedLog   = Join-Path $logDir "scheduler.out"

if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
}

function Write-SchedLog($Line) {
    $stamp = (Get-Date).ToString("s")
    Add-Content -Path $schedLog -Value "[$stamp] $Line" -Encoding UTF8
}

function Remove-TaskIfPresent($TaskName) {
    $existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($null -ne $existing) {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
        Write-Host "[Register-Operator] Removed '$TaskName'."
        Write-SchedLog "unregister: removed '$TaskName'"
    } else {
        Write-Host "[Register-Operator] '$TaskName' not present - skipping."
    }
}

if ($Unregister) {
    Remove-TaskIfPresent "OperatorDaemon"
    Remove-TaskIfPresent "OperatorSnapshot"
    exit 0
}

# Legacy V3 daemon is superseded. Always remove if present.
Remove-TaskIfPresent "OperatorV3Daemon"

if (-not (Test-Path $vbs))      { Write-Error "Missing: $vbs";     exit 2 }
if (-not (Test-Path $startPs1)) { Write-Error "Missing: $startPs1"; exit 2 }
if (-not (Test-Path $snapPs1))  { Write-Error "Missing: $snapPs1";  exit 2 }

function Register-HiddenTask {
    param(
        [string]$TaskName,
        [string]$Ps1Path,
        [string]$Description,
        $Trigger
    )

    # wscript hidden-run.vbs "powershell.exe" "-NoProfile" "-ExecutionPolicy" "Bypass" "-File" "..."
    $execArgs = @(
        "`"$vbs`"",
        "`"powershell.exe`"",
        "`"-NoProfile`"",
        "`"-ExecutionPolicy`"",
        "`"Bypass`"",
        "`"-File`"",
        "`"$Ps1Path`""
    ) -join " "

    $action = New-ScheduledTaskAction `
        -Execute "wscript.exe" `
        -Argument $execArgs `
        -WorkingDirectory $repoRoot

    $settings = New-ScheduledTaskSettingsSet `
        -StartWhenAvailable `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -RestartCount 3 `
        -RestartInterval (New-TimeSpan -Minutes 1)

    $principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

    $existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($null -eq $existing) {
        Register-ScheduledTask `
            -TaskName $TaskName `
            -Action $action `
            -Trigger $Trigger `
            -Principal $principal `
            -Settings $settings `
            -Description $Description | Out-Null
        Write-Host "[Register-Operator] Registered '$TaskName'."
        Write-SchedLog "register: created '$TaskName' args=$execArgs"
    } else {
        Set-ScheduledTask -TaskName $TaskName -Action $action -Trigger $Trigger -Principal $principal -Settings $settings | Out-Null
        Write-Host "[Register-Operator] Updated '$TaskName'."
        Write-SchedLog "update: '$TaskName' args=$execArgs"
    }
}

# OperatorDaemon: every 5 min. start-operator.ps1 guards with a pid file,
# so this is effectively "respawn if dead, else no-op." Avoids needing
# admin-only AtLogOn triggers.
$daemonStartAt = (Get-Date).AddMinutes(1)
$daemonTrigger = New-ScheduledTaskTrigger -Once -At $daemonStartAt -RepetitionInterval (New-TimeSpan -Minutes 5)
Register-HiddenTask `
    -TaskName "OperatorDaemon" `
    -Ps1Path  $startPs1 `
    -Description "Operator Core daemon (respawns if dead; HTTP hooks + scheduler + snapshot publisher)" `
    -Trigger  $daemonTrigger

# OperatorSnapshot: every 30 min, starting 2 min from now
$snapStartAt = (Get-Date).AddMinutes(2)
$snapTrigger = New-ScheduledTaskTrigger -Once -At $snapStartAt -RepetitionInterval (New-TimeSpan -Minutes 30)
Register-HiddenTask `
    -TaskName "OperatorSnapshot" `
    -Ps1Path  $snapPs1 `
    -Description "Operator Core snapshot publisher (30-min cadence, belt-and-suspenders)" `
    -Trigger  $snapTrigger

$infoDaemon = Get-ScheduledTaskInfo -TaskName "OperatorDaemon"   -ErrorAction SilentlyContinue
$infoSnap   = Get-ScheduledTaskInfo -TaskName "OperatorSnapshot" -ErrorAction SilentlyContinue
Write-Host "[Register-Operator] OperatorDaemon   next run: $($infoDaemon.NextRunTime)"
Write-Host "[Register-Operator] OperatorSnapshot next run: $($infoSnap.NextRunTime)"
Write-SchedLog "next_run_daemon=$($infoDaemon.NextRunTime) next_run_snap=$($infoSnap.NextRunTime)"

exit 0
