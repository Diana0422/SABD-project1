package com.sparkling_taxi.utils;

public class Const {
    public static final String DOWNLOAD_TEMPLATE = "/home/templates/download_to_hdfs.xml";
    // ================ QUERY 1 =========================
    public static final String PRE_PROCESSING_TEMPLATE_Q1 = "/home/templates/template_query1.xml";
    public static final String FILE_Q1 = "hdfs://namenode:9000/home/dataset-batch/Query1.parquet";
    public static final String OUT_DIR_Q1 = "hdfs://namenode:9000/home/dataset-batch/output-query1";
    // ================ QUERY 2 =========================
    public static final String PRE_PROCESSING_TEMPLATE_Q2 = "/home/templates/template_query2.xml";
    public static final String FILE_Q2 = "hdfs://namenode:9000/home/dataset-batch/Query2.parquet";


    public static final Long NUM_PAYMENT_TYPES = 6L;
    public static final Long UNKNOWN_PAYMENT_TYPE = 5L;
    // ================ QUERY 3 =========================
    public static final String PRE_PROCESSING_TEMPLATE_Q3 = "/home/templates/template_query3.xml";
    public static final String FILE_Q3 = "hdfs://namenode:9000/home/dataset-batch/Query3.parquet";
    public static final String OUT_DIR_Q3 = "hdfs://namenode:9000/home/dataset-batch/output-query3";

    public static final String ZONES_CSV = "hdfs://namenode:9000/home/zones/taxi+_zone_lookup.csv";
    // ================ NIFI ============================
    static final String[] FILE_TO_DOWNLOAD = new String[]{
            "hdfs://namenode:9000/home/download/yellow_tripdata_2021-12.parquet",
            "hdfs://namenode:9000/home/download/yellow_tripdata_2022-01.parquet",
            "hdfs://namenode:9000/home/download/yellow_tripdata_2022-02.parquet",
            ZONES_CSV
    };

    public static final String NIFI_URL = "http://nifi:8181/nifi-api/";
    // ================ OTHER ===========================
    public static final String LINUX_SEPARATOR = "/";
    public static final String WINDOWS_SEPARATOR = "\\";
}
