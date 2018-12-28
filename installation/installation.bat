@echo off

REM ====================================================================================================
REM SETTINGS
set bin_path=%~dp0
set PGCLIENTENCODING=utf-8 & chcp 65001>nul
if exist "%appdata%\postgresql\psqlrc.conf" ren "%appdata%\postgresql\psqlrc.conf" blank_psqlrc >nul
set template=template0
set encoding=UTF-8
set "collate=Danish, Denmark"
set "type=Danish, Denmark"
REM ====================================================================================================


REM ====================================================================================================
REM INITIAL TEXT
echo Installation af database til Grøn Registrering
echo.
echo Databasen er udviklet i PostgreSQL 10 / PostGIS 2.4 med udgangspunkt i QGIS 3 som brugerflade.
REM ====================================================================================================


REM ====================================================================================================
REM CONNECT TO SERVER
call "%~dp0bin\connection.bat"
call "%~dp0bin\database.bat"
REM ====================================================================================================

REM ====================================================================================================
REM CHECK IF CHOSEN DATABASE EXISTS OR NOT
find /c " %database% " "%~dp0bin\temp">nul && ( goto:db_exist ) || ( goto:db_not_exist )
REM ====================================================================================================


REM ====================================================================================================
REM DATABASE DOES NOT EXIST
:db_not_exist
echo.
echo ---------------------------
echo Databasen oprettes, da den ikke eksisterer på serveren.
echo ---------------------------
echo.
:db_remake
"%bin_path%bin\psql.exe" --dbname=postgresql://%username%:%password%@%server%:%port%/postgres -q -c "CREATE DATABASE %database% TEMPLATE %template% ENCODING '%encoding%' LC_COLLATE '%collate%' LC_CTYPE '%type%';" >nul 2> nul

if %errorlevel% EQU 0 (
echo.
echo ---------------------------
echo Database [%database%] oprettet
echo Indstillinger:
echo ENCODING: %encoding%
echo LC_COLLATE: %collate%
echo LC_CTYPE: %type%
echo ---------------------------
echo.
goto:con_install
) else (
echo.
echo ---------------------------
echo FEJL!
echo Databasen blev ikke oprettet!
echo Installationen stopper!
echo ---------------------------
echo.
pause
goto:exit
)
REM ====================================================================================================


REM ====================================================================================================
REM DATABASE DOES EXIST
:db_exist
echo.
echo Databasen eksisterer allerede.
:reuse
set /P con="Benyt eksisterende database, ved nej omdøbes den eksisterende database (y|n)? : "
if /I "%con%" EQU "y" goto:con_install
if /I "%con%" EQU "n" goto:reuse_n
goto:reuse

:reuse_n
"%bin_path%bin\psql.exe" --dbname=postgresql://%username%:%password%@%server%:%port%/postgres -q -c "ALTER DATABASE %database% RENAME TO %database%_old;" >nul 2> nul

if %errorlevel% EQU 0 (
echo.
echo ---------------------------
echo Database [%database%] er omdøbt til [%database%_old]
echo ---------------------------
echo.
goto:db_remake
) else (
echo.
echo ---------------------------
echo FEJL!
echo Databasen blev ikke omdøbt!
echo Installationen stopper!
echo ---------------------------
echo.
pause
goto:exit
)
REM ====================================================================================================


REM ====================================================================================================
REM INSTALLATION
:con_install
for /F "delims=" %%G in ('dir /b /o:n "%~dp0*.sql"') do set db_script=%%~G
"%bin_path%bin\psql.exe" --dbname=postgresql://%username%:%password%@%server%:%port%/%database% -f "%~dp0%db_script%" >nul 2> nul
REM ====================================================================================================

REM ====================================================================================================
REM IMPORT DATA
echo.
"%bin_path%bin\psql.exe" --dbname=postgresql://%username%:%password%@%server%:%port%/%database% -f "%~dp0data\copy"
REM ====================================================================================================





:exit
REM ====================================================================================================
REM RENAME CONFIG FOR PSQL IF EXISTS
if exist "%appdata%\postgresql\blank_psqlrc" ren "%appdata%\postgresql\blank_psqlrc" psqlrc.conf >nul
REM ====================================================================================================
::pause


