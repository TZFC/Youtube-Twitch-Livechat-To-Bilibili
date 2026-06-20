@echo off
title Chat Bridge Launcher
echo ========================================================
echo Installing Required Libraries...
echo ========================================================
pip install -r requirements.txt

echo.
echo ========================================================
echo Starting Application...
echo ========================================================
start http://127.0.0.1:8000
python app.py

pause