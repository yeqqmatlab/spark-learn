package org.spark.delta.lake;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * description:
 * Created by yqq
 * 2021-09-03
 */
public class WriteDemo {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("WriteDemo")
                .master("local[1]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();

        System.out.println("sparkSession.version() = " + sparkSession.version());

        Dataset<Long> data = sparkSession.range(0, 500);

        data.write().format("delta").mode("overwrite").parquet("hdfs://ip243:8020/delta_data");

        Dataset<Row> readData = sparkSession
                .read()
                .format("delta")
                //.option("versionAsOf",0)
                .load("hdfs://ip243:8020/delta_data");
        readData.show();
        readData.printSchema();

        sparkSession.stop();
    }
}
