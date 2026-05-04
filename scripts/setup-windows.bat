@echo off
setlocal enabledelayedexpansion

echo === Configuration ===
echo.

set /p api_key="??? Please enter your TorBox API Key found on https://torbox.app/settings ???"

cls

if "%api_key%"=="" (
    echo !!! API Key is required. !!!
    exit /b 1
)

set /p run_as="??? Would you like to run TorBox Media Center as Fuse or Strm (fuse/strm) ???"

if not "%run_as%"=="fuse" if not "%run_as%"=="strm" (
    echo !!! Invalid input. Please enter fuse or strm. !!!
    exit /b 1
)

cls

for /f "tokens=*" %%i in ('whoami') do set current_user=%%i

set /p storage_path="??? Where would you like the files to be stored ???"
echo --- Default: C:\Users\%current_user%\torbox-media-center ---

if "%storage_path%"=="" (
    set storage_path=C:\Users\%current_user%\torbox-media-center
)

cls

echo === Checking dependencies ===
echo.

REM Check if Docker is installed
docker --version >nul 2>&1
if errorlevel 1 (
    echo !!! Docker is not installed or not in the PATH. Please install Docker and try again. !!!
    exit /b 1
) else (
    echo --- Found Docker. Checking if Docker is running... ---
    docker info >nul 2>&1
    if errorlevel 1 (
        echo !!! Docker is not running. Please start Docker and try again. !!!
        exit /b 1
    ) else (
        echo --- Docker is running. ---
    )
)

REM Check if Fuse is installed only if run_as is fuse
if "%run_as%"=="fuse" (
    wsl which fuse >nul 2>&1
    if errorlevel 1 (
        echo !!! Fuse is not installed in WSL. Please install Fuse in WSL and try again. !!!
        exit /b 1
    ) else (
        echo --- Fuse is installed in WSL. ---
    )
)

cls

echo === Creating directories ===
echo.

if not exist "%storage_path%" mkdir "%storage_path%"
echo --- Created directory: %storage_path% ---

cls

echo === Running TorBox Media Center ===
echo.

REM Stop and remove existing container if it exists
docker stop torbox-media-center >nul 2>&1
docker rm torbox-media-center >nul 2>&1

REM Run the container
docker run -it -d --name=torbox-media-center --restart=always --init ^
    -v "%storage_path%:/torbox" ^
    -e TORBOX_API_KEY="%api_key%" ^
    -e MOUNT_METHOD="%run_as%" ^
    -e MOUNT_PATH="/torbox" ^
    anonymoussystems/torbox-media-center:latest

cls

echo === TorBox Media Center is now running! ===
echo.
echo --- Mount Path: %storage_path% ---
echo --- Run as: %run_as% ---
echo --- API Key: %api_key% ---
echo -- To view logs run: docker logs -f torbox-media-center
echo -- To restart the container run: docker restart torbox-media-center
echo -- To stop the container run: docker stop torbox-media-center
echo -- To remove the container run: docker rm torbox-media-center
echo.
pause
