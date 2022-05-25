@echo off

@rem the current dir is %~dp0

set directory=%~dp0..\target
if NOT exist %directory% (
    echo "target directory does not exists - packaging app "
    CALL mvn -f %~dp0..\pom.xml clean compile jar:jar
) else (
    CALL mvn compile jar:jar
)

@rem if argument 1 is number in 1-3, then execute the query
if "%1" == "1" GOTO q1
if "%1" == "2" GOTO q2
if "%1" == "3" GOTO q3

echo "Usage: ./submit_query <query number>"
GOTO done

:q1
echo query1
docker exec spark-master /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.spark.Query1 ./taxi-app/sabd1-1.0.jar
goto done

:q2
echo query2
docker exec spark-master /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.spark.Query2 ./taxi-app/sabd1-1.0.jar
goto done

:q3
echo query3
docker exec spark-master /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.spark.Query3 ./taxi-app/sabd1-1.0.jar

:done
exit
