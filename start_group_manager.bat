@echo off
chcp 65001 >nul
title EasyChat Group Manager
cd /d "%~dp0"

python group_manager_gui.py
if %errorlevel% neq 0 (
    echo.
    echo Launch failed. Please run: pip install -r requirements.txt
    pause
)
