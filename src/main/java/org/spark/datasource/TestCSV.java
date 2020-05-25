package org.spark.datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * created by yqq 2020/5/20
 */
public class TestCSV {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("TestCSV")
                .master("local[2]")
                .getOrCreate();
        //Dataset<Row> csvDF = spark.read().csv("C:\\Users\\Administrator\\Desktop\\test_sqoop.csv");
        //csvDF.repartition(1).write().parquet("C:\\Users\\Administrator\\Desktop\\parquet4");

        Dataset<Row> parquetDF = spark.read().parquet("hdfs://ip243:8020/tmp/test_sqoop");
        Dataset<Row> parquetDF2 = spark.read().parquet("hdfs://ip243:8020/tmp/test_sqoop2");

        parquetDF.printSchema();

        System.out.println("--->"+parquetDF.count());

        parquetDF2.printSchema();

        System.out.println("--->"+parquetDF2.count());


        spark.stop();
    }
}
