package org.spark.delta.lake;

import org.apache.spark.sql.Dataset;
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
                .master("local[2]")
                .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();

        System.out.println("sparkSession.version() = " + sparkSession.version());

        Dataset<Long> data = sparkSession.range(0, 50);

        //data.write().format("delta").save("./delta-table");

        sparkSession.stop();
    }
}
