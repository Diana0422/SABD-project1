#!/bin/bash
# PREPARE ENVIRONMENT
DIR=../target
if [ -d "$DIR" ];
then
    echo "$DIR directory exists."
else
	echo "$DIR directory does not exist."
	./package_app.sh
fi
# EXECUTE QUERIES (MANDATORY)
if [ $1 -eq 1 ]
then
       docker exec -it spark-master /bin/bash /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.Query1 ./taxi-app/sabd1-1.0.jar
 elif [ $1 -eq 2 ]
then
       docker exec -it spark-master /bin/bash /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.Query2 ./taxi-app/sabd1-1.0.jar
 elif [ $1 -eq 3 ]
then
       docker exec -it spark-master /bin/bash /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.Query3 ./taxi-app/sabd1-1.0.jar
else
      printf "Usage: ./submit_query <query number>"
fi