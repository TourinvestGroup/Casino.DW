@echo off
:: ============================================================
:: Casino DW - Daily Visits Report
:: Triggered by Windows Task Scheduler at 07:00 daily.
:: Runs ONE HOUR after the data pipeline (CasinoDW_DailyRun at 06:00)
:: so the data SLA and the report delivery SLA stay decoupled.
::
:: This is a strictly read-only consumer of the warehouse — it does
:: NOT trigger the bronze/silver/gold loads. If the 06:00 pipeline
:: failed, the report will detect missing gold data and fall back
:: to the SAFE recipient list only (REPORT_TO_EMAILS_SAFE).
:: ============================================================
title Casino DW - Daily Visits Report

set SCRIPT_DIR=%~dp0
set PROJECT_DIR=%SCRIPT_DIR%..
set PYTHON_EXE=%PROJECT_DIR%\.venv\Scripts\python.exe
set FLOW_DIR=%PROJECT_DIR%\medallion_pg\orchestration\python
set LOG_FILE=%PROJECT_DIR%\logs\daily_report.log

echo ============================================================ >> "%LOG_FILE%"
echo [%DATE% %TIME%] Casino DW daily report starting... >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

cd /d "%FLOW_DIR%"
"%PYTHON_EXE%" export_visit_sessions.py >> "%LOG_FILE%" 2>&1

if %ERRORLEVEL% EQU 0 (
    echo [%DATE% %TIME%] Report SUCCEEDED >> "%LOG_FILE%"
) else (
    echo [%DATE% %TIME%] Report FAILED with exit code %ERRORLEVEL% >> "%LOG_FILE%"
)

echo. >> "%LOG_FILE%"
