@Echo OFF
REM storing file path
SET BINDIR=%~dp0
CD /D "%BINDIR%"
REM adding current dir to python path
set PYTHONPATH=%cd%;%PYTHONPATH%
..\venv\Scripts\luigi.exe --scheduler-url http://ubuntu@tp-hadoop-28:8082 --module etl RunProcessTask --workers 4