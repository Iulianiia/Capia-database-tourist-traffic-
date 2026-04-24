@echo off
cd /d %~dp0

python ..\src\create_aircrafts_types_seats.py || exit /b
python ..\src\load_aircraft_types_seats_table.py || exit /b


echo Done!
pause