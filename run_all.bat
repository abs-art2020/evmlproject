@echo off
title Medallion and ML Pipeline Orchestrator
cls

:: Force the command prompt to use the folder where this batch file lives
cd /d "%~dp0"

echo ===================================================
echo [1/3] CHECKING MLFLOW SERVER STATUS...
echo ===================================================
netstat -ano | findstr :5000 >nul
if %errorlevel% equ 0 (
    echo [OK] MLflow server is already online on port 5000.
) else (
    :: Explicitly force Windows to launch the script inside its exact current directory
    start "MLflow Background Server" /d "%~dp0" cmd /c start_mlflow.bat
    :: Give the local server 5 seconds to wake up and initialize sqlite
    timeout /t 5 /nobreak >nul

    echo [BROWSER] Opening MLflow Dashboard automatically...
    :: This CLI command launches your default Windows browser straight to the UI
    start http://127.0.0.1:5000
    
    :: Wait another 2 seconds to make sure everything settles before running scripts
    timeout /t 2 /nobreak >nul
)

echo.
echo ===================================================
echo [2/3] ACTIVATING VIRTUAL ENVIRONMENT...
echo ===================================================
if not exist "venv\Scripts\activate.bat" (
    echo [ERROR] Virtual environment 'venv' not found in this folder!
    echo Please create it first using: python -m venv venv
    goto :error
)
call venv\Scripts\activate
echo [OK] Python virtual environment active.

echo.
echo ===================================================
echo [3/3] EXECUTING PIPELINE LAYERS...
echo ===================================================

echo [RUN] Processing Bronze Layer...
python bronze.py
if %errorlevel% neq 0 (
    echo [FAIL] Bronze layer execution failed!
    goto :error
)
echo [SUCCESS] Bronze layer finished.
echo ---------------------------------------------------

@REM echo [RUN] Processing Silver Layer...
@REM python silver2.py
@REM if %errorlevel% neq 0 (
@REM     echo [FAIL] Silver layer execution failed!
@REM     goto :error
@REM )
@REM echo [SUCCESS] Silver layer finished.
@REM echo ---------------------------------------------------

echo [RUN] Processing Gold Layer...
python gold2.py
if %errorlevel% neq 0 (
    echo [FAIL] Gold layer execution failed!
    goto :error
)
echo [SUCCESS] Gold layer finished.
echo ---------------------------------------------------

echo [RUN] Running MLflow Inference...
python train_and_predict_regression.py
if %errorlevel% neq 0 (
    echo [FAIL] ML inference script failed!
    goto :error
)
echo [SUCCESS] Inference finished.
echo ---------------------------------------------------

echo ===================================================
echo ALL PIPELINE STAGES COMPLETED SUCCESSFULLY!
echo ===================================================
pause
exit /b 0

:error
echo ===================================================
echo PIPELINE ABORTED DUE TO AN ERROR. CHECK LOGS ABOVE.
echo ===================================================
pause
exit /b %errorlevel%
