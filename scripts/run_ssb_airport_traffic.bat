@echo off
cd /d %~dp0

python ..\src\create_ssb_monthly_traffic_table.py || exit /b
python ..\src\load_ssb_monthly_traffic_data.py || exit /b


echo Done!
pause