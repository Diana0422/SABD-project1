#!/bin/bash
docker exec -it spark-master /bin/bash /opt/bitnami/spark/bin/spark-submit --class com.sparkling_taxi.Query1 --master local ./sabd1-1.0.jar ciao.txt