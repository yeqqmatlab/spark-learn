package org.spark;

import org.apache.spark.sql.SparkSession;

public class TestSpark {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[2]")
                .appName("TestSpark")
                //.config("","")
                .getOrCreate();






        sparkSession.stop();
    }
}
