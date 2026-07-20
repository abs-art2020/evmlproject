@echo off
title MLflow Tracking Server
cd /d "%~dp0"

echo [MLFLOW] Activating virtual environment...
call venv\Scripts\activate

echo [MLFLOW] Verifying packages...
:: Install sqlite engine support if missing
pip install pysqlite3-binary >nul 2>&1

echo [MLFLOW] Booting server on http://127.0.0.1:5000...
echo CRITICAL: DO NOT CLOSE THIS WINDOW WHILE RUNNING PIPELINES!
echo -----------------------------------------------------------

:: Using standard relative pathing to ensure Windows compatibility
mlflow server --backend-store-uri sqlite:///mlflow.db --default-artifact-root ./mlruns --host 127.0.0.1 --port 5000

echo -----------------------------------------------------------
echo [MLFLOW] Server stopped or crashed.
pause
