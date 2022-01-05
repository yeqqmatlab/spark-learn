package org.spark.hive;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * description:
 * Created by yqq
 * 2021-09-16
 */
public class ReadHive {
    public static void main(String[] args) {


        SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("ReadHive").getOrCreate();


        Dataset<Row> parquet = sparkSession.read().parquet("hdfs://ip243:8020/zsy/warehouse/ods/school/student_paper_topic_rs_partition/school_id=3853");
        parquet.printSchema();

        sparkSession.stop();
    }
}
