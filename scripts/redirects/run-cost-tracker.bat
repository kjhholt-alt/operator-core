@echo off
chcp 65001 >nul
cd /d "%~dp0"
if not exist ..\..\logs mkdir ..\..\logs
py -m operator_core.cli recipe run cost_tracker >> ..\..\logs\cost_tracker.log 2>&1
exit /b %errorlevel%
