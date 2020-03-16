package org.spark;

import org.apache.spark.sql.SparkSession;

/**
 * created by yqq 2020/3/12
 */
public class TestBroadcast {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("TestBroadcast")
                .master("local[2]")
                .config("spark.sql.autoBroadcastJoinThreshold", 10)
                .getOrCreate();




    }
}
