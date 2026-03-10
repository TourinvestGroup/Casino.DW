@echo off
title Casino DW - Daily Flow (cron 06:00)
cd /d "C:\Users\N.motskobili\Desktop\Casino DW\medallion_pg\orchestration\python"
call "C:\Users\N.motskobili\Desktop\Casino DW\.venv\Scripts\activate.bat"
set PREFECT_API_URL=http://127.0.0.1:4200/api

echo Waiting 30 seconds for Prefect server to start...
timeout /t 30 /nobreak >nul

echo Starting daily flow scheduler (cron 06:00)...
python prefect_flow.py --serve --cron "0 6 * * *"
