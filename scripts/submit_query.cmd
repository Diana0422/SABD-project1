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
if "%1" == "4" GOTO q4
if "%1" == "5" GOTO q5
if "%1" == "6" GOTO q6

echo "Usage: ./submit_query <query number in 1-6>"
GOTO done

:q1
echo query1
docker exec spark-master /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.spark.Query1 ./taxi-app/sabd1-1.0-jar-with-dependencies.jar
goto done

:q2
echo query2
docker exec spark-master /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.spark.Query2 ./taxi-app/sabd1-1.0.jar
goto done

:q3
echo query3
docker exec spark-master /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.spark.Query3 ./taxi-app/sabd1-1.0.jar
goto done

:q4
docker exec spark-master /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.sparksql.QuerySQL1 ./taxi-app/sabd1-1.0.jar
goto done

:q5
docker exec spark-master /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.sparksql.QuerySQL2 ./taxi-app/sabd1-1.0.jar
goto done

:q6
docker exec spark-master /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.sparksql.QuerySQL3 ./taxi-app/sabd1-1.0.jar
goto done

:done
exit
