@echo off
:: ============================================================
:: Casino DW - One-Time Backfill Runner
:: Run this ONCE to catch up all missed days.
:: The flow auto-detects the gap and backfills from the last
:: known gold day through yesterday.
:: ============================================================
title Casino DW - BACKFILL (catching up missed days)

set SCRIPT_DIR=%~dp0
set PROJECT_DIR=%SCRIPT_DIR%..
set PYTHON_EXE=%PROJECT_DIR%\.venv\Scripts\python.exe
set FLOW_DIR=%PROJECT_DIR%\medallion_pg\orchestration\python
set LOG_FILE=%PROJECT_DIR%\logs\backfill.log

:: Prefect: run without a persistent server (ephemeral SQLite backend)
set PREFECT_SERVER_ALLOW_EPHEMERAL_MODE=true

echo ============================================================
echo  Casino DW - BACKFILL
echo  Auto-catchup will detect all missing days and reload them.
echo  This may take 15-60 minutes depending on the gap size.
echo  Log: %LOG_FILE%
echo ============================================================

echo ============================================================ >> "%LOG_FILE%"
echo [%DATE% %TIME%] BACKFILL run starting... >> "%LOG_FILE%"
echo ============================================================ >> "%LOG_FILE%"

cd /d "%FLOW_DIR%"
"%PYTHON_EXE%" prefect_flow.py >> "%LOG_FILE%" 2>&1

if %ERRORLEVEL% EQU 0 (
    echo [%DATE% %TIME%] BACKFILL SUCCEEDED >> "%LOG_FILE%"
    echo.
    echo SUCCESS - Backfill complete. Check LAST_REFRESH.md for details.
) else (
    echo [%DATE% %TIME%] BACKFILL FAILED with exit code %ERRORLEVEL% >> "%LOG_FILE%"
    echo.
    echo FAILED - Check %LOG_FILE% for errors.
)

echo. >> "%LOG_FILE%"
pause
