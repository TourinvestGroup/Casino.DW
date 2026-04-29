#Requires -RunAsAdministrator
# ============================================================
# Register the Casino DW Daily Visits Report scheduled task.
# Runs at 07:00 daily, one hour after CasinoDW_DailyRun (06:00).
#
# Run ONCE manually, as Administrator:
#   powershell -ExecutionPolicy Bypass -File scripts\register_visits_report_task.ps1
# ============================================================

$projectDir = "C:\Users\N.motskobili\Desktop\Casino DW"
$batFile    = "$projectDir\scripts\run_daily_report.bat"
$logDir     = "$projectDir\logs"
$taskName   = "CasinoDW_VisitsReport"

if (-not (Test-Path $batFile)) {
    Write-Error "[ERROR] Bat file not found: $batFile"
    exit 1
}
if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
    Write-Host "[CREATED] $logDir"
}

# Remove any prior registration so this script is idempotent
if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    Write-Host "[REMOVED] existing task $taskName"
}

$action   = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$batFile`""
$trigger  = New-ScheduledTaskTrigger -Daily -At "07:00AM"
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 30)

# Match the logon-type pattern used by the existing CasinoDW_DailyRun task.
# If you've upgraded that task to S4U, mirror that change here too.
$principal = New-ScheduledTaskPrincipal `
    -UserId $env:USERNAME `
    -LogonType Interactive `
    -RunLevel Limited

Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Description "Casino DW daily visits xlsx report. Reads gold.fn_visit_sessions and emails the result. Runs 1h after CasinoDW_DailyRun." | Out-Null

Write-Host "[REGISTERED] $taskName at 07:00 daily"
Write-Host ""
Write-Host "To verify:"
Write-Host "  Get-ScheduledTask -TaskName $taskName"
Write-Host ""
Write-Host "To trigger a manual test run now:"
Write-Host "  Start-ScheduledTask -TaskName $taskName"
Write-Host ""
Write-Host "To inspect logs after a run:"
Write-Host "  Get-Content $logDir\daily_report.log -Tail 30"
