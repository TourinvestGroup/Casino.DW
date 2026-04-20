#Requires -RunAsAdministrator
# ============================================================
# Casino DW - Fix Scheduled Tasks
# Replaces the broken Server+Worker approach with a single
# daily Task Scheduler job that runs the flow directly.
# No Prefect server or worker required.
#
# Usage: Right-click > Run with PowerShell (as Admin)
# ============================================================

$ErrorActionPreference = "Stop"
$projectDir = "C:\Users\N.motskobili\Desktop\Casino DW"
$batFile    = "$projectDir\scripts\run_daily_flow.bat"
$logDir     = "$projectDir\logs"

# --- Ensure log dir exists ---
if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
    Write-Host "[OK] Created logs directory: $logDir" -ForegroundColor Green
}

# --- Remove old broken tasks ---
$oldTasks = @(
    "Prefect Server - Casino DW",
    "Prefect Worker - Casino DW",
    "CasinoDW_PrefectServer",
    "CasinoDW_DailyFlow"
)

foreach ($task in $oldTasks) {
    $existing = Get-ScheduledTask -TaskName $task -ErrorAction SilentlyContinue
    if ($existing) {
        Unregister-ScheduledTask -TaskName $task -Confirm:$false
        Write-Host "[REMOVED] $task" -ForegroundColor Yellow
    }
}

# --- Register new daily task ---
$taskName = "CasinoDW_DailyRun"

$action = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument "/c `"$batFile`""

# Daily at 06:00 AM
$trigger = New-ScheduledTaskTrigger -Daily -At "06:00AM"

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 4) `
    -RestartCount 2 `
    -RestartInterval (New-TimeSpan -Minutes 5) `
    -MultipleInstances IgnoreNew

$principal = New-ScheduledTaskPrincipal `
    -UserId $env:USERNAME `
    -LogonType S4U `
    -RunLevel Highest

Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue

Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Description "Casino DW daily ETL at 06:00. Serverless (no Prefect server). Auto-catchup for missed days."

Write-Host "[OK] $taskName registered (daily 06:00, auto-catchup, serverless)" -ForegroundColor Green

# --- Summary ---
Write-Host ""
Write-Host "=== Current Casino DW Tasks ===" -ForegroundColor Cyan
Get-ScheduledTask -TaskName "CasinoDW_*" | ForEach-Object {
    $info = Get-ScheduledTaskInfo -TaskName $_.TaskName -ErrorAction SilentlyContinue
    [PSCustomObject]@{
        TaskName   = $_.TaskName
        State      = $_.State
        NextRun    = $info.NextRunTime
        Description = $_.Description
    }
} | Format-Table -AutoSize

Write-Host ""
Write-Host "NEXT STEPS:" -ForegroundColor Yellow
Write-Host "  1. Run the backfill now to catch up the 21-day gap:" -ForegroundColor White
Write-Host "     Double-click: $projectDir\scripts\run_backfill.bat" -ForegroundColor Gray
Write-Host "  2. Daily runs will execute automatically at 06:00 AM going forward." -ForegroundColor White
Write-Host "  3. Check logs at: $logDir\daily_run.log" -ForegroundColor Gray
Write-Host ""
Read-Host "Press Enter to close"
