@echo off
chcp 65001 >nul
title EasyChat Excel Sender
cd /d "%~dp0"

python excel_sender_gui.py
if %errorlevel% neq 0 (
    echo.
    echo Launch failed. Please run: pip install -r requirements.txt
    pause
)
