package org.spark.phoenix;

import org.apache.spark.sql.*;


/**
 * phoenix demo
 * created by yqq 2020/8/17
 */
public class Demo2 {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Demo2")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("org.apache.phoenix.spark")
                .option("zkUrl", "192.168.1.243,192.168.1.244,192.168.1.239:2181/hbase")
                .option("table", "WEB_STAT")
                .load();

        df.show();




        spark.stop();
    }
}
