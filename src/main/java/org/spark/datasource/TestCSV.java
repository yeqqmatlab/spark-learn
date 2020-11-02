//package org.spark.datasource;
//
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.scala.spark.streaming.StreamingExamples;
//
///**
// * created by yqq 2020/5/20
// */
//public class TestCSV {
//
//    public static void main(String[] args) {
//
//        StreamingExamples.setStreamingLogLevels();
//        SparkSession spark = SparkSession
//                .builder()
//                .appName("TestCSV")
//                .master("local[2]")
//                .getOrCreate();
//        //Dataset<Row> csvDF = spark.read().csv("C:\\Users\\Administrator\\Desktop\\test_sqoop.csv");
//        //csvDF.repartition(1).write().parquet("C:\\Users\\Administrator\\Desktop\\parquet4");
//
//        Dataset<Row> parquetDF = spark.read().parquet("hdfs://ip243:8020/tmp/test_sqoop");
//        //Dataset<Row> parquetDF2 = spark.read().parquet("hdfs://ip243:8020/tmp/test_sqoop2");
//
//        parquetDF.printSchema();
//
//        System.out.println("--->"+parquetDF.count());
//
//        double[] doubles = new double[2];
//        doubles[0] = 0.5;
//        doubles[1] = 0.5;
//        Dataset<Row>[] df = parquetDF.randomSplit(doubles);
//        System.out.println("0-->"+df[0].count());
//        System.out.println("1-->"+df[1].count());
//
//
//
//
//
//        spark.stop();
//    }
//}
