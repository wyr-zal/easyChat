@echo off
chcp 65001 >nul
title EasyChat Remark Manager
cd /d "%~dp0"

python -m remark_manager.remark_manager_gui
if %errorlevel% neq 0 (
    echo.
    echo Launch failed. Please run: pip install -r requirements.txt
    pause
)
