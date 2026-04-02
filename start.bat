@echo off
chcp 65001 >nul
title EasyChat

python wechat_gui.py
if %errorlevel% neq 0 (
    echo.
    echo 启动失败，请检查是否已安装依赖：pip install -r requirements.txt
    pause
)
