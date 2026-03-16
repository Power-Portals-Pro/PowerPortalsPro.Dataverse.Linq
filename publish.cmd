@echo off
setlocal enabledelayedexpansion

:: ============================================================================
:: publish.cmd - Build and publish PowerPortalsPro.Dataverse.Linq to NuGet.org
:: ============================================================================

set "PROJECT=src\PowerPortalsPro.Dataverse.Linq\PowerPortalsPro.Dataverse.Linq.csproj"
set "NUPKG_DIR=src\PowerPortalsPro.Dataverse.Linq\bin\Release"
set "LOCAL_FEED=D:\Code\LocalNugetFeed"
set "SECRETS_ID=f2866066-518b-43f7-997a-83d178b60059"

:: -------------------------------------------------------------------
:: 1. Prompts
:: -------------------------------------------------------------------
echo.
set "PRERELEASE="
set /p "PRERELEASE=Pre-release suffix [e.g. alpha, beta.1, rc1] or press Enter for stable: "
set "PUSH_NUGET=n"
set /p "PUSH_NUGET=Push to NuGet.org? [y/N]: "

:: -------------------------------------------------------------------
:: Read NuGet API key from user secrets
:: -------------------------------------------------------------------
set "API_KEY="
if /i "!PUSH_NUGET!"=="y" (
    for /f "tokens=2 delims==" %%K in ('dotnet user-secrets list --project "src\PowerPortalsPro.Dataverse.Linq.Tests\PowerPortalsPro.Dataverse.Linq.Tests.csproj" --id "%SECRETS_ID%" 2^>nul ^| findstr "NuGet:ApiKey"') do (
        set "API_KEY=%%K"
    )
    if defined API_KEY (
        set "API_KEY=!API_KEY:~1!"
        echo   API key : loaded from user secrets
    ) else (
        echo   WARNING: NuGet:ApiKey not found in user secrets.
    )
)

:: -------------------------------------------------------------------
:: 2. Generate version: yyyy.M.d.HHmm
:: -------------------------------------------------------------------
for /f "delims=" %%V in ('powershell -NoProfile -Command "[string](Get-Date).Year+'.'+[string](Get-Date).Month+'.'+[string](Get-Date).Day+'.'+[string]((Get-Date).Hour*100+(Get-Date).Minute)"') do set "VERSION=%%V"

if not defined PRERELEASE (
    set "FULL_VERSION=!VERSION!"
) else (
    set "FULL_VERSION=!VERSION!-!PRERELEASE!"
)

echo.
echo   Version : !FULL_VERSION!
echo.

:: -------------------------------------------------------------------
:: 3. Build and pack
:: -------------------------------------------------------------------
echo --- Building and packing v!FULL_VERSION! [Release] ---
echo.

set "NUPKG_NAME=PowerPortalsPro.Dataverse.Linq.!FULL_VERSION!.nupkg"

dotnet pack "%PROJECT%" -c Release -p:Version=!FULL_VERSION! --output "%NUPKG_DIR%"
if errorlevel 1 (
    echo.
    echo   ERROR: Pack failed.
    exit /b 1
)

:: -------------------------------------------------------------------
:: 4. Copy to local feed
:: -------------------------------------------------------------------
echo.
echo --- Copying to local feed ---
echo.

if not exist "%LOCAL_FEED%" mkdir "%LOCAL_FEED%"
copy "%NUPKG_DIR%\!NUPKG_NAME!" "%LOCAL_FEED%\"
if errorlevel 1 (
    echo.
    echo   ERROR: Copy to local feed failed.
    exit /b 1
)

echo   Copied to %LOCAL_FEED%

:: -------------------------------------------------------------------
:: 5. Push to NuGet.org
:: -------------------------------------------------------------------
if /i not "!PUSH_NUGET!"=="y" goto :done
if not defined API_KEY (
    echo.
    echo   ERROR: No API key provided. Skipping push.
    goto :done
)

echo.
echo --- Pushing to NuGet.org ---
echo.

dotnet nuget push "%NUPKG_DIR%\!NUPKG_NAME!" ^
    --api-key "!API_KEY!" ^
    --source https://api.nuget.org/v3/index.json ^
    --skip-duplicate ^
    --no-symbols

if errorlevel 1 (
    echo.
    echo   ERROR: Push failed.
    exit /b 1
)

echo.
echo   Successfully published PowerPortalsPro.Dataverse.Linq v!FULL_VERSION!

:done
echo.
echo   Done.

endlocal
