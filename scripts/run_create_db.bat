@echo off
cd /d %~dp0\..

python src\create_db.py 

echo Done!
pause