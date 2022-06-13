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

## Deployment:
To deploy this project use **DockerCompose**:
``` 
docker compose up --scale spark-worker <num-worker-replicas> --scale datanode <num-datanode-replicas>
```
`--scale` permits to modify the number of replicas of the containers and services.

## Execute Query:
> **_NOTE:_**  Do this after the [deployment phase](##Deployment:).
Open a terminal in the project base directory and follow these steps

To execute a query and the build is not yet executed use the following scripts:
> **_NOTE:_**  These comands also trigger the execution of the query once the project is built. 
* Bash:
```
./scripts/submit_query.sh <num-query>
```
* Shell:
```
.\scripts\submit_query.cmd <num-query>
```

To execute a query and you already built the project, then you can use the following scripts:
* Bash:
```
./scripts/run_query.sh <num-query>
```
* Shell:
```
.\scripts\run_query.bat <num-query>
```
where `num-query` is the number of the query to execute:
 - 1 -> Query1
 - 2 -> Query2
 - 3 -> Query3
 - 4 -> Query1SQL
 - 5 -> Query2SQL
 - 6 -> Query3SQL

## UIs:
* **NiFi**: http://localhost:8181/nifi
* **HDFS(Master)**: http://localhost:9870/
* **Grafana**: http://localhost:8000/
* **Spark(Master)**: http://localhost:8080/
* **Spark(Worker)**: http://localhost:4040/

You can find the **Grafana Dashboard** of the project at:
http://localhost:8000/d/2J8ln097k/sabd-project-1?orgId=1


## Frameworks:
* **NiFi**: used for data acquisition and preprocessing
* **Spark**: used for data batch processing
* **HDFS**: used as data storage. Contains the the batch to analyze in the processing layer and also contains the processing output.
* **Redis**: used to cache output data to be read by the data visualization layer.
* **Grafana**: used to visualize output data after processing.

[![nifi](https://svn-eu.apache.org/repos/asf/nifi/site/trunk/assets/images/nifiDrop.svg)](https://nifi.apache.org)
[![spark](https://upload.wikimedia.org/wikipedia/commons/thumb/f/f3/Apache_Spark_logo.svg/320px-Apache_Spark_logo.svg.png)](https://spark.apache.org)
<a href="https://redis.io">
<img src="https://static.cdnlogo.com/logos/r/31/redis.svg" width="150" height="150">
</a>
<a href="https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html">
<img src="https://cdn.worldvectorlogo.com/logos/hadoop.svg" width="150" height="150">
</a>
<a href="https://grafana.com">
<img src="https://cdn.worldvectorlogo.com/logos/grafana.svg" width="150" height="150">
</a>
