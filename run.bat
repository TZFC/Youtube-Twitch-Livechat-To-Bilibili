@echo off
title Chat Bridge Launcher
setlocal

:: Completely isolate the python environment from the user's global/AppData packages
set "PYTHONNOUSERSITE=1"

set "PYTHON_DIR=%~dp0python"
set "PYTHON_EXE=%PYTHON_DIR%\python.exe"

if not exist "%PYTHON_DIR%\python.exe" (
    echo ========================================================
    echo Setting up portable Python environment...
    echo ========================================================
    
    echo Downloading Portable Python 3.11...
    powershell -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri 'https://www.python.org/ftp/python/3.11.9/python-3.11.9-embed-amd64.zip' -OutFile 'python.zip'"
    
    echo Extracting Python...
    powershell -Command "Expand-Archive -Path 'python.zip' -DestinationPath '%PYTHON_DIR%' -Force"
    del python.zip
    
    echo Enabling package support...
    powershell -Command "(Get-Content '%PYTHON_DIR%\python311._pth') -replace '#import site', 'import site' | Set-Content '%PYTHON_DIR%\python311._pth'"
    
    echo Downloading get-pip.py...
    powershell -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri 'https://bootstrap.pypa.io/get-pip.py' -OutFile 'get-pip.py'"
    
    echo Installing pip...
    "%PYTHON_EXE%" get-pip.py
    del get-pip.py
    
    echo Installing build dependencies...
    "%PYTHON_EXE%" -m pip install setuptools wheel --force-reinstall
)

echo.
echo ========================================================
echo Installing Required Libraries...
echo ========================================================
"%PYTHON_EXE%" -m pip install -r requirements.txt

echo.
echo ========================================================
echo Starting Application...
echo ========================================================
start http://127.0.0.1:8000
"%PYTHON_EXE%" app.py

pause