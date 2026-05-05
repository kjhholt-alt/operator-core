@echo off
chcp 65001 >nul
cd /d "%~dp0"
if not exist ..\..\logs mkdir ..\..\logs
py -m operator_core.cli recipe run client_health >> ..\..\logs\client_health.log 2>&1
exit /b %errorlevel%
