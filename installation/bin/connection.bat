REM Connection information used for the different tools

REM ====================================================================================================
REM LOGIN INFORMATION
:login
set /a try+=1
echo.
if %try% EQU 1 echo -- Login --
if %try% GTR 1 echo -- Prøv igen --
echo.
if %try% EQU 1 set server=localhost
set /P server="Server [%server%]: "

if %try% EQU 1 set port=5432
set /P port="Port [%port%]: "

if %try% EQU 1 set username=postgres
set /P username="Username [%username%]: "

:pw
set "password="
set "psCommand=powershell -Command "$pword = read-host 'Password' -AsSecureString ; ^
    $BSTR=[System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($pword); ^
        [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($BSTR)""
for /f "usebackq delims=" %%p in (`%psCommand%`) do set password=%%p

if not defined password goto:pw
REM ====================================================================================================


REM ====================================================================================================
REM CHECK LOGIN INFORMATION
"%toolbox_path%bin\psql.exe" --dbname=postgresql://%username%:%password%@%server%:%port%/postgres -q -c "SELECT current_database();" >nul && echo. && echo Login: %username%@%server%:%port%
REM ====================================================================================================


REM ====================================================================================================
REM TRY AGAIN IF LOGIN FAILS
if %errorlevel% NEQ 0 goto:login
REM ====================================================================================================