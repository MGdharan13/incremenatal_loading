@echo off
title Incremental ETL Pipeline Web UI
echo Starting Incremental ETL Web Dashboard...
echo.

REM Try Python from venv or default
if exist ".venv\Scripts\python.exe" (
    start "" http://localhost:8000
    .venv\Scripts\python.exe app.py
) else (
    start "" http://localhost:8000
    python app.py
)

pause

