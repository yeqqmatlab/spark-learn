package org.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class PartitionDemo {
    public static void main(String[] args) throws InterruptedException {

        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("PartitionDemo")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> df = spark.read().parquet("hdfs://ip243:8020/zsy/warehouse/dws/exam_extend");

        df.show(100);

        df.write().mode(SaveMode.Overwrite).partitionBy("school_id","grade").parquet("hdfs://ip243:8020/test");

        Dataset<Row> df2 = spark.read().parquet("hdfs://ip243:8020/test");

        df2.filter("grade is not null and type is not null").select("school_id","exam_group_id","name","type","create_time","grade").write()
                .partitionBy("school_id","grade")
                .bucketBy(10,"type")
                .sortBy("create_time")
                .mode(SaveMode.Overwrite)
                .option("path","hdfs://ip243:8020/test5/")
                .saveAsTable("school_exam");


        spark.catalog().uncacheTable("school_exam");





//        Thread.sleep(1000*60*5);

        /*StringBuffer bucketedSql = new StringBuffer();
        bucketedSql.append(" CREATE TABLE school_exam ");
        bucketedSql.append(" (school_id int,exam_group_id string,name string,type int,create_time int,grade int) ");
        bucketedSql.append(" USING PARQUET ");
        //bucketedSql.append(" PARTITIONED BY (school_id) ");
        bucketedSql.append(" CLUSTERED BY (type) INTO 10 BUCKETS ");
        bucketedSql.append(" LOCATION 'hdfs://ip243:8020/test5/school_id=115' ");

        spark.sql(bucketedSql.toString());

        spark.sql("select * from school_exam").show();*/


        spark.stop();
    }
}
