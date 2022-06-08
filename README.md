# SABD-project1
This repository contains a code base that permits to analyze data of NYC Taxi and Limousine Commission (TLC) datasets of December 2021, January 2022, February 2022. The main objective is to answer the following queries:

* **Query1**: For each month calculate the average percentage of the tip amount with respect to the total cost of the trip excludong the toll amount.  
* **Query2**: For each hour, calculate the distribution (percentage) of the trips number with respect to the departure zones (PULocationID), the average tip and its standard deviation and the most popular payment type.
* **Query3**: For each day, identify the 5 most popular destination zones and specify for each result the average number of passengers, the average fare amount and its standard deviation.

The code for the queries can be found in the codebase at these directories:
- *src/main/java/com/sparkling_taxi/spark/Query1.java*
- *src/main/java/com/sparkling_taxi/spark/Query2.java*
- *src/main/java/com/sparkling_taxi/spark/Query3.java*

We also implemented alternatives of these queries using the SparkSQL and those implementations can be found at:
- *src/main/java/com/sparkling_taxi/sparksql/Query1SQL.java*
- *src/main/java/com/sparkling_taxi/sparksql/Query2SQL.java*
- *src/main/java/com/sparkling_taxi/sparksql/Query3SQL.java*

## Requirements:
This project uses **Docker** and **DockerCompose** to instantiate the HDFS, Spark, Redis, Nifi and Grafana containers.

* [get docker](https://docs.docker.com/get-docker/)
* [install docker compose](https://docs.docker.com/compose/install/)

## Frameworks:
* **Nifi**: used for data acquisition and preprocessing
* **Spark**: used for data batch processing
* **HDFS**: used as data storage. Contains the the batch to analyze in the processing layer and also contains the processing output.
* **Redis**: used to cache output data to be read by the data visualization layer.
* **Grafana**: used to visualize output data after processing.

[![nifi](https://svn-eu.apache.org/repos/asf/nifi/site/trunk/assets/images/nifiDrop.svg)](https://nifi.apache.org)
[![spark](https://upload.wikimedia.org/wikipedia/commons/thumb/f/f3/Apache_Spark_logo.svg/320px-Apache_Spark_logo.svg.png)](https://spark.apache.org)
[![redis](https://static.cdnlogo.com/logos/r/31/redis.svg)](https://redis.io)
[![hdfs](https://cdn.worldvectorlogo.com/logos/hadoop.svg)](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html)
[![grafana](https://cdn.worldvectorlogo.com/logos/grafana.svg)](https://grafana.com)
