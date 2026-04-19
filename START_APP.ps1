Set-Location $PSScriptRoot

if (Test-Path ".venv\Scripts\python.exe") {
    $pythonExe = ".venv\Scripts\python.exe"
} else {
    $pythonExe = "python"
}

Write-Host "Starting Sydney Traffic..." -ForegroundColor Cyan
Write-Host ""
Write-Host "Open http://127.0.0.1:8000 after startup completes." -ForegroundColor Yellow
Write-Host "Keep this window open while the website is running." -ForegroundColor Yellow
Write-Host ""

& $pythonExe -m uvicorn app:app --reload
