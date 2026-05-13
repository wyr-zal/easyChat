@echo off
chcp 65001 >nul
title EasyChat Remark Manager Builder
cd /d "%~dp0\.."

echo.
echo [1/3] Cleaning previous remark manager build artifacts...
if exist "build\remark_manager" rmdir /s /q "build\remark_manager"
if exist "dist\remark_manager.exe" del /f /q "dist\remark_manager.exe"

echo.
echo [2/3] Building remark_manager.exe...
python -m PyInstaller --noconfirm --clean --onefile --windowed --name remark_manager remark_manager\remark_manager_gui.py

if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Build failed.
    echo Please make sure dependencies are installed:
    echo   pip install -r requirements.txt
    echo   pip install pyinstaller
    echo.
    pause
    exit /b %errorlevel%
)

echo.
echo [3/3] Build complete.
echo Output: %cd%\dist\remark_manager.exe
echo.
pause
