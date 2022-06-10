package com.sparkling_taxi.utils;

public class Const {
    private Const() {
    }

    public static final String DOWNLOAD_TEMPLATE = "/home/templates/download_to_hdfs.xml";
    public static final String RESULT_DIR1 = "/home/results/query1";
    public static final String RESULT_DIR2 = "/home/results/query2";
    public static final String RESULT_DIR3 = "/home/results/query3";
    // ================ QUERY 1 =========================
    public static final String PRE_PROCESSING_TEMPLATE_Q1 = "/home/templates/template_query1.xml";
    public static final String FILE_Q1 = "hdfs://namenode:9000/home/dataset-batch/Query1.parquet";
    public static final String OUT_HDFS_URL_Q1 = "hdfs://namenode:9000/home/dataset-batch/output-query1";
    public static final String OUT_HDFS_DIR_Q1 = "/home/dataset-batch/output-query1";

    // ================ QUERY 2 =========================
    public static final String PRE_PROCESSING_TEMPLATE_Q2 = "/home/templates/template_query2.xml";
    public static final String FILE_Q2 = "hdfs://namenode:9000/home/dataset-batch/Query2.parquet";
    public static final String OUT_HDFS_URL_Q2 = "hdfs://namenode:9000/home/dataset-batch/output-query2";
    public static final String OUT_HDFS_DIR_Q2 = "/home/dataset-batch/output-query2";

    public static final Long NUM_PAYMENT_TYPES = 6L;
    public static final Long UNKNOWN_PAYMENT_TYPE = 5L;
    public static final Long NUM_LOCATIONS = 265L;
    public static final int RANKING_SIZE = 5;
    // ================ QUERY 3 =========================
    public static final String PRE_PROCESSING_TEMPLATE_Q3 = "/home/templates/template_query3.xml";
    public static final String FILE_Q3 = "hdfs://namenode:9000/home/dataset-batch/Query3.parquet";
    public static final String OUT_HDFS_URL_Q3 = "hdfs://namenode:9000/home/dataset-batch/output-query3";
    public static final String OUT_HDFS_DIR_Q3 = "/home/dataset-batch/output-query3";

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

    public static final String REDIS_URL = "redis://redis:6379";
    public static final String LINUX_SEPARATOR = "/";
    public static final String WINDOWS_SEPARATOR = "\\";

    public static final int NUM_EVALUATIONS = 40;
    public static final String EVALUATION_FILE = "/home/results/evaluation.csv";
}
