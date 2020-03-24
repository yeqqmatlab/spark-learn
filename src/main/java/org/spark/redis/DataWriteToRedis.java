package org.spark.redis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import redis.clients.jedis.Jedis;

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
