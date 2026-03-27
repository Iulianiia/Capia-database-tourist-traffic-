@echo off
cd /d %~dp0

python ..\src\create_avinor_flights_table.py || exit /b
python ..\src\load_avinor_flights_data.py || exit /b


echo Done!
pause