@ECHO OFF

SET configuration=%1
IF "%configuration%"=="" SET configuration=Debug

copy .\x64\%configuration%\pg_diffix.dll %PGROOT%\lib\
IF %errorlevel% NEQ 0 EXIT /b %errorlevel%

copy .\*.sql %PGROOT%\share\extension\
IF %errorlevel% NEQ 0 EXIT /b %errorlevel%

copy .\pg_diffix.control %PGROOT%\share\extension\
IF %errorlevel% NEQ 0 EXIT /b %errorlevel%
