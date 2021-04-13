package org.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CSVDemo {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder().appName("CSVDemo").master("local[10]").getOrCreate();

        Dataset<Row> csv = sparkSession.read().option("header",true).csv("F:\\software\\neo4j-community-3.5.24\\import\\method_to_topic.csv");

        csv.repartition(10).write().option("header", true).csv("C:\\Users\\Administrator\\Desktop\\tmp\\00");

        csv.printSchema();

        csv.show();


        sparkSession.stop();
    }
}
