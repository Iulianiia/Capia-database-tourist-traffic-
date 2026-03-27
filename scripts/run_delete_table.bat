@echo off
cd /d %~dp0\..

python src\services\delete_table.py %1

pause