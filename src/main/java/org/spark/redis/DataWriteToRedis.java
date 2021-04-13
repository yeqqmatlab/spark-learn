package org.spark.redis;

import org.apache.spark.sql.SparkSession;


/**
 * created by yqq 2020/3/20
 */
public class DataWriteToRedis {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("DataWriteToRedis")
                .master("local[2]")
                .getOrCreate();




        spark.stop();
    }
}
