$startupDir = [Environment]::GetFolderPath('Startup')
$projectDir = "C:\Users\N.motskobili\Desktop\Casino DW"

$ws = New-Object -ComObject WScript.Shell

# Shortcut 1: Prefect Server
$sc1 = $ws.CreateShortcut("$startupDir\CasinoDW_PrefectServer.lnk")
$sc1.TargetPath = "$projectDir\start_prefect_server.bat"
$sc1.WorkingDirectory = $projectDir
$sc1.Description = "Starts Prefect server for Casino DW"
$sc1.WindowStyle = 7
$sc1.Save()
Write-Host "[OK] CasinoDW_PrefectServer.lnk created in Startup" -ForegroundColor Green

# Shortcut 2: Daily Flow
$sc2 = $ws.CreateShortcut("$startupDir\CasinoDW_DailyFlow.lnk")
$sc2.TargetPath = "$projectDir\start_daily_flow.bat"
$sc2.WorkingDirectory = "$projectDir\medallion_pg\orchestration\python"
$sc2.Description = "Casino DW daily ETL (cron 06:00, auto-catchup)"
$sc2.WindowStyle = 7
$sc2.Save()
Write-Host "[OK] CasinoDW_DailyFlow.lnk created in Startup" -ForegroundColor Green

Write-Host ""
Get-ChildItem $startupDir -Filter "CasinoDW*" | Format-Table Name, LastWriteTime -AutoSize
