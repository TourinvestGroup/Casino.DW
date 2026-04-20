$projectDir = "C:\Users\N.motskobili\Desktop\Casino DW"
$pythonExe = "$projectDir\.venv\Scripts\python.exe"

# Task 1: Prefect Server - starts at logon
$action1 = New-ScheduledTaskAction -Execute $pythonExe -Argument "-m prefect server start" -WorkingDirectory $projectDir
$trigger1 = New-ScheduledTaskTrigger -AtLogon
$settings1 = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Days 365)
Unregister-ScheduledTask -TaskName "CasinoDW_PrefectServer" -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask -TaskName "CasinoDW_PrefectServer" -Action $action1 -Trigger $trigger1 -Settings $settings1 -Description "Starts Prefect server for Casino DW pipeline"
Write-Host "[OK] CasinoDW_PrefectServer registered" -ForegroundColor Green

# Task 2: Daily Flow - starts at logon +30s, cron 06:00 inside
$flowDir = "$projectDir\medallion_pg\orchestration\python"
$action2 = New-ScheduledTaskAction -Execute $pythonExe -Argument "prefect_flow.py --serve" -WorkingDirectory $flowDir
$trigger2 = New-ScheduledTaskTrigger -AtLogon
$trigger2.Delay = "PT30S"
$settings2 = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Days 365)
Unregister-ScheduledTask -TaskName "CasinoDW_DailyFlow" -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask -TaskName "CasinoDW_DailyFlow" -Action $action2 -Trigger $trigger2 -Settings $settings2 -Description "Casino DW daily ETL (cron 06:00, auto-catchup)"
Write-Host "[OK] CasinoDW_DailyFlow registered" -ForegroundColor Green

Write-Host ""
Get-ScheduledTask -TaskName "CasinoDW_*" | Format-Table TaskName, State -AutoSize
