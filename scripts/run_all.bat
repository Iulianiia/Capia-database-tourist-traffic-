@echo off
cd /d %~dp0

call run_aircraft_table.bat
call run_airports_table.bat
call run_avinor_flights_table.bat
call run_ssb_airport_traffic.bat

echo.
echo ALL DONE!
pause