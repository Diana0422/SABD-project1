# EXECUTE QUERIES (MANDATORY)
if [ $1 -eq 1 ]
then
       docker exec spark-master /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.spark.Query1 --master local ./taxi-app/sabd1-1.0-jar-with-dependencies.jar
 elif [ $1 -eq 2 ]
then
       docker exec spark-master /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.spark.Query2 --master local ./taxi-app/sabd1-1.0-jar-with-dependencies.jar
 elif [ $1 -eq 3 ]
then
       docker exec spark-master /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.spark.Query3 --master local ./taxi-app/sabd1-1.0-jar-with-dependencies.jar
 elif [ $1 -eq 4 ]
then
       docker exec spark-master /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.sparksql.QuerySQL1 --master local ./taxi-app/sabd1-1.0-jar-with-dependencies.jar
 elif [ $1 -eq 5 ]
then
       docker exec spark-master /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.sparksql.QuerySQL2 --master local ./taxi-app/sabd1-1.0-jar-with-dependencies.jar
 elif [ $1 -eq 6 ]
then
       docker exec spark-master /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.sparksql.QuerySQL3 --master local ./taxi-app/sabd1-1.0-jar-with-dependencies.jar

else
      printf "Usage: ./run_query <query number>"
fi