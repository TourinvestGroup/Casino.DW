
# Casino DW - Task Scheduler Setup
# Run this script as Administrator to register startup tasks
# Usage: Right-click > Run with PowerShell (as Admin)

$ErrorActionPreference = "Stop"
$projectDir = "C:\Users\N.motskobili\Desktop\Casino DW"

# Check admin
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "Restarting as Administrator..." -ForegroundColor Yellow
    Start-Process powershell.exe -ArgumentList "-ExecutionPolicy Bypass -File `"$PSCommandPath`"" -Verb RunAs
    exit
}

# --- Task 1: Prefect Server (starts on boot with 30s delay) ---
$taskName1 = "CasinoDW_PrefectServer"
$action1 = New-ScheduledTaskAction `
    -Execute "$projectDir\.venv\Scripts\python.exe" `
    -Argument "-m prefect server start" `
    -WorkingDirectory "$projectDir"

$trigger1 = New-ScheduledTaskTrigger -AtStartup
$trigger1.Delay = "PT30S"

$settings1 = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit (New-TimeSpan -Days 365)

$principal1 = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType S4U -RunLevel Highest

Unregister-ScheduledTask -TaskName $taskName1 -Confirm:$false -ErrorAction SilentlyContinue

Register-ScheduledTask `
    -TaskName $taskName1 `
    -Action $action1 `
    -Trigger $trigger1 `
    -Settings $settings1 `
    -Principal $principal1 `
    -Description "Starts Prefect server for Casino DW pipeline"

Write-Host "[OK] $taskName1 registered (runs at startup + 30s delay)" -ForegroundColor Green

# --- Task 2: Daily Flow (starts on boot with 60s delay, runs as scheduled cron inside) ---
$taskName2 = "CasinoDW_DailyFlow"
$action2 = New-ScheduledTaskAction `
    -Execute "$projectDir\.venv\Scripts\python.exe" `
    -Argument "prefect_flow.py --serve --cron `"0 6 * * *`"" `
    -WorkingDirectory "$projectDir\medallion_pg\orchestration\python"

$trigger2 = New-ScheduledTaskTrigger -AtStartup
$trigger2.Delay = "PT60S"

$settings2 = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit (New-TimeSpan -Days 365)

$principal2 = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType S4U -RunLevel Highest

Unregister-ScheduledTask -TaskName $taskName2 -Confirm:$false -ErrorAction SilentlyContinue

Register-ScheduledTask `
    -TaskName $taskName2 `
    -Action $action2 `
    -Trigger $trigger2 `
    -Settings $settings2 `
    -Principal $principal2 `
    -Description "Casino DW daily ETL flow (cron 06:00, auto-catchup missed days)"

Write-Host "[OK] $taskName2 registered (runs at startup + 60s delay, cron 06:00)" -ForegroundColor Green

# --- Summary ---
Write-Host ""
Write-Host "=== Registered Tasks ===" -ForegroundColor Cyan
Get-ScheduledTask -TaskName "CasinoDW_*" | Format-Table TaskName, State, Description -AutoSize

Write-Host ""
Write-Host "Both tasks start on reboot automatically." -ForegroundColor Yellow
Write-Host "The daily flow runs at 06:00 and auto-catches up any missed days." -ForegroundColor Yellow
Write-Host ""
Write-Host "To start them now without rebooting:" -ForegroundColor Yellow
Write-Host "  Start-ScheduledTask -TaskName 'CasinoDW_PrefectServer'" -ForegroundColor White
Write-Host "  Start-ScheduledTask -TaskName 'CasinoDW_DailyFlow'" -ForegroundColor White
Write-Host ""
Read-Host "Press Enter to close"
