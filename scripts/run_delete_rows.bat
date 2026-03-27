@echo off
cd /d %~dp0\..

python src\services\delete_rows.py %1

pause