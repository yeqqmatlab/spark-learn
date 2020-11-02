package org.spark.phoenix;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * phoenix demo
 * created by yqq 2020/8/17
 */
public class Demo {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("phoenix-test");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);

        // Load data from TABLE1
        Dataset<Row> df = sqlContext
                .read()
                .format("phoenix")
                .option("table", "WEB_STAT")
                .option("zkUrl", "ip243:2181")
                .load();
        df.show();

        jsc.stop();



    }
}
