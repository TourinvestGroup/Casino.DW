@echo off
cd /d "C:\Users\N.motskobili\Desktop\Casino DW\medallion_pg\orchestration\python"
call "C:\Users\N.motskobili\Desktop\Casino DW\.venv\Scripts\activate.bat"
set PREFECT_API_URL=http://127.0.0.1:4200/api
prefect worker start --pool default-agent-pool
