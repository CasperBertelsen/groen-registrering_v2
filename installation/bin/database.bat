REM Database input with list of databases on the server

REM ====================================================================================================
REM GENERATE LIST OF DATABSES
"%toolbox_path%bin\psql.exe" --dbname=postgresql://%username%:%password%@%server%:%port%/postgres -o "%toolbox_path%bin\temp" -t -c "SELECT datname || ' ' FROM pg_catalog.pg_database WHERE datistemplate IS FALSE ORDER BY 1";
REM ====================================================================================================


REM ====================================================================================================
REM CHOOSE DATABASE
echo.
:again
set "database="
set /P database="Database: "
if not defined database goto:again
REM ====================================================================================================