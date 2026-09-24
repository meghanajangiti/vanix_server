@echo off

echo ==========================================
echo       VANIX MULTI-KIOSK SERVER
echo ==========================================
echo.

set GEMINI_API_KEY=YOUR_API_KEY_HERE 
set GEMINI_MODEL=gemini-3.8-flash
set GEMINI_FALLBACK_MODEL=gemini-3.5-flash-lite
set GEMINI_MAX_RETRIES=3
set GEMINI_BASE_RETRY_MS=1000

set MQTT_BROKER=mqtt://broker.emqx.io:1883
set FRONTDESK_NUMBER=6305141921

cd /d C:\VANIX\server

echo Starting VANIX server...
echo.

npm start

pause