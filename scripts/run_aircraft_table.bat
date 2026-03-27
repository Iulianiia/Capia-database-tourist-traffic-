@echo off
cd /d %~dp0

python ..\src\create_aircrafts_types_table.py || exit /b
python ..\src\load_aircraft_types_data.py || exit /b


echo Done!
pause