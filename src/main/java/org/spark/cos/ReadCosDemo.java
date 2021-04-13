package org.spark.cos;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * TOD
 * 有问题
 */
public class ReadCosDemo {
    public static void main(String[] args) {


        SparkSession spark = SparkSession
                .builder()
                .appName("ReadCosDemo")
                .master("local[2]")
                .getOrCreate();


        //spark.read().csv("cos://ip243:8020/tmp/test_sqoop");

        Dataset<Row> csv = spark.read().csv("https://test-20210408-1259037786.cos.ap-shanghai.myqcloud.com/topic/query-hive-1480.csv");
        csv.show();


        spark.stop();
    }
}
