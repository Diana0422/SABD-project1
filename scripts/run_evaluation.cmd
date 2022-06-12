@echo off
@rem get command argument

SET "var="&for /f "delims=0123456789" %%i in ("%1") do set var=%%i
if defined var (
	echo %1 NOT numeric 
	GOTO esci
) else (echo %1 numeric)

if %1 gtr 4 (GOTO esci)
if %1 lss 1 (GOTO esci)

CALL mvn package
@rem run docker compose scale
CALL docker-compose up --scale spark-worker=%1 -d
@rem run evaluation
CALL docker exec spark-master /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.evaluation.Evaluation ./taxi-app/sabd1-1.0-jar-with-dependencies.jar

@rem stop docker compose
CALL docker-compose down
exit

:esci
echo Usage run_evaluation "<1|2|3|4>"
