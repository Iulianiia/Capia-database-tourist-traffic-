@echo off
cd /d %~dp0

python ..\src\create_airports_table.py || exit /b
python ..\src\load_airports_data.py || exit /b


echo Done!
pause