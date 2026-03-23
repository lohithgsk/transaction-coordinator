@echo off
echo ===================================================
echo [SYSTEM] Starting Chaos Failure Injection Test...
echo ===================================================

echo [1/4] Starting Participant Database...
start "Database" cmd /c "go run . -mode=participant -port=8081"

echo [2/4] Starting Hybrid Coordinator...
start "Coordinator" cmd /c "go run . -mode=coordinator -port=8082"

:: Wait for servers to boot
timeout /t 2 /nobreak > nul

echo [3/4] Firing 200-Transaction Zipfian Load Test...
start "LoadTest" cmd /c "go run . -mode=loadtest -type=zipf"

:: Let it process transactions for exactly 1 second, then assassinate it
timeout /t 1 /nobreak > nul

echo [4/4] [CHAOS] Forcefully assassinating Coordinator Node...
taskkill /FI "WINDOWTITLE eq Coordinator" /F /T > nul

timeout /t 1 /nobreak > nul

echo [RECOVERY] Rebooting new Coordinator from WAL...
start "RecoveredCoordinator" cmd /k "go run . -mode=coordinator -port=8082"

echo ===================================================
echo DONE! Look at the 'RecoveredCoordinator' window.
echo Find the line that says: 'MEAN TIME TO RECOVERY (MTTR)'
echo ===================================================
echo Press any key to clean up and kill all servers...
pause > nul

taskkill /FI "WINDOWTITLE eq Database" /F /T > nul
taskkill /FI "WINDOWTITLE eq RecoveredCoordinator" /F /T > nul
taskkill /FI "WINDOWTITLE eq LoadTest" /F /T > nul