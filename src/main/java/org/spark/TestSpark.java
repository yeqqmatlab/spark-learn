package org.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TestSpark {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[2]")
                .appName("TestSpark")
                //.config("","")
                .getOrCreate();

        Dataset<Row> df = sparkSession.read().parquet("hdfs://192.168.1.243:8020/zsy/warehouse/dws/exam_extend/");

        df.show();

        sparkSession.stop();
    }
}
