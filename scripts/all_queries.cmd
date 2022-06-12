CALL mvn package

@rem docker exec spark-master /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.spark.Query1 ./taxi-app/sabd1-1.0-jar-with-dependencies.jar
@rem docker exec spark-master /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.spark.Query2 ./taxi-app/sabd1-1.0-jar-with-dependencies.jar
docker exec spark-master /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.spark.Query3 ./taxi-app/sabd1-1.0-jar-with-dependencies.jar
docker exec spark-master /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.sparksql.QuerySQL1 ./taxi-app/sabd1-1.0-jar-with-dependencies.jar
docker exec spark-master /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.sparksql.QuerySQL2 ./taxi-app/sabd1-1.0-jar-with-dependencies.jar
docker exec spark-master /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.sparksql.QuerySQL3 ./taxi-app/sabd1-1.0-jar-with-dependencies.jar
