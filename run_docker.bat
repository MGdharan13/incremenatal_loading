@echo off
title Incremental ETL Docker Launcher
echo ===================================================
echo   Starting Incremental ETL + MySQL in Docker...
echo ===================================================
echo.

docker-compose up -d

echo.
echo Containers are running!
echo Web UI is available at: http://localhost:8000
echo MySQL is mapped to host port: 3307 (internal 3306)
echo.
echo Opening Web UI in browser...
start "" http://localhost:8000
echo.
echo Use 'docker-compose logs -f' to stream logs.
echo Use 'docker-compose down' to stop.
echo.
pause
