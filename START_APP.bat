@echo off
setlocal

cd /d "%~dp0"

if exist ".venv\Scripts\python.exe" (
  set "PYTHON_EXE=.venv\Scripts\python.exe"
) else (
  set "PYTHON_EXE=python"
)

echo Starting Sydney Traffic...
echo.
echo Open http://127.0.0.1:8000 after startup completes.
echo Keep this window open while the website is running.
echo.

"%PYTHON_EXE%" -m uvicorn app:app --reload

endlocal
