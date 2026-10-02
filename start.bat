@echo off
setlocal
cd /d "%~dp0"

echo ============================================
echo   Media Batch Downloader WebUI
echo ============================================
echo.

set "PY="
where python >nul 2>nul && set "PY=python"
if not defined PY (
  where py >nul 2>nul && set "PY=py -3"
)
if not defined PY (
  echo [ERROR] Python not found. Install Python 3.8+ and enable "Add to PATH".
  pause
  exit /b 1
)

echo [1/3] Checking virtual env...
if not exist ".venv" (
  echo        Creating .venv ...
  %PY% -m venv .venv
  if errorlevel 1 (
    echo [ERROR] Failed to create virtual env.
    pause
    exit /b 1
  )
)

set "VPY=.venv\Scripts\python.exe"

echo [2/3] Checking dependencies...
"%VPY%" -c "import flask,aiohttp,bs4" >nul 2>nul
if errorlevel 1 (
  echo        Installing dependencies - needs internet...
  set HTTP_PROXY=
  set HTTPS_PROXY=
  set ALL_PROXY=
  "%VPY%" -m pip install -r requirements.txt
  if errorlevel 1 (
    echo [ERROR] Dependency install failed. Check network or run pip manually.
    pause
    exit /b 1
  )
)

echo [3/3] Starting server ...
echo.
echo   Open in browser:  http://127.0.0.1:8891
echo   Close this window to stop.
echo.

start "" "http://127.0.0.1:8891"
"%VPY%" app.py

echo.
echo   Server stopped.
pause
