@echo off
REM build.bat - Script per compilare il sistema MapReduce su Windows

echo ======================================
echo   Compilazione Sistema MapReduce
echo ======================================

REM Creazione struttura directory
echo.
echo [1/5] Creazione struttura directory...
if not exist "build\client" mkdir build\client
if not exist "build\master" mkdir build\master
if not exist "build\example" mkdir build\example
if not exist "dist" mkdir dist
if not exist "jars" mkdir jars

REM Compilazione Client
echo.
echo [2/5] Compilazione MapReduceClient...
javac -d build\client -sourcepath src src\it\mapreduce\client\MapReduceClient.java

if %ERRORLEVEL% NEQ 0 (
    echo Errore nella compilazione del Client!
    exit /b 1
)

REM Compilazione Master
echo.
echo [3/5] Compilazione MapReduceMaster...
javac -d build\master -sourcepath src src\it\mapreduce\master\MapReduceMaster.java src\it\mapreduce\master\JobClassLoader.java

if %ERRORLEVEL% NEQ 0 (
    echo Errore nella compilazione del Master!
    exit /b 1
)

REM Creazione JAR del sistema
echo.
echo [4/5] Creazione mapreduce-system.jar...
cd build
jar cvf ..\dist\mapreduce-system.jar -C client . -C master .
cd ..

REM Compilazione job di esempio
echo.
echo [5/5] Compilazione ExampleMapReduceJob...
javac -d build\example -sourcepath src src\it\mapreduce\example\ExampleMapReduceJob.java

if %ERRORLEVEL% NEQ 0 (
    echo Errore nella compilazione del job di esempio!
    exit /b 1
)

REM Creazione JAR del job di esempio
echo.
echo Creazione example-job.jar...
cd build\example
jar cvfe ..\..\dist\example-job.jar it.mapreduce.example.ExampleMapReduceJob .
cd ..\..

echo.
echo ======================================
echo   Compilazione completata!
echo ======================================
echo.
echo File generati in dist\
dir dist
echo.
echo Per avviare il sistema:
echo   1. Master: java -cp dist\mapreduce-system.jar it.mapreduce.master.MapReduceMaster 9999
echo   2. Client: java -cp dist\mapreduce-system.jar it.mapreduce.client.MapReduceClient localhost 9999 dist\example-job.jar it.mapreduce.example.ExampleMapReduceJob
echo.