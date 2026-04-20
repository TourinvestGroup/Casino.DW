@echo off
:: ============================================================
:: Casino DW - Daily Pipeline Runner
:: Triggered by Windows Task Scheduler at 06:00 daily.
:: No Prefect server or worker required (ephemeral mode).
:: Auto-catchup: detects missed days and backfills automatically.
:: ============================================================
title Casino DW - Daily Pipeline

set SCRIPT_DIR=%~dp0
set PROJECT_DIR=%SCRIPT_DIR%..
set PYTHON_EXE=%PROJECT_DIR%\.venv\Scripts\python.exe
set FLOW_DIR=%PROJECT_DIR%\medallion_pg\orchestration\python
set LOG_FILE=%PROJECT_DIR%\logs\daily_run.log

:: Prefect: run without a persistent server (ephemeral SQLite backend)
set PREFECT_SERVER_ALLOW_EPHEMERAL_MODE=true

echo ============================================================ >> "%LOG_FILE%"
echo [%DATE% %TIME%] Casino DW daily run starting... >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

cd /d "%FLOW_DIR%"
"%PYTHON_EXE%" prefect_flow.py >> "%LOG_FILE%" 2>&1

if %ERRORLEVEL% EQU 0 (
    echo [%DATE% %TIME%] Run SUCCEEDED >> "%LOG_FILE%"
) else (
    echo [%DATE% %TIME%] Run FAILED with exit code %ERRORLEVEL% >> "%LOG_FILE%"
)

echo. >> "%LOG_FILE%"
