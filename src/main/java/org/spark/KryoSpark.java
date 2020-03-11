package org.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * created by yqq 2020/3/10
 */
public class KryoSpark {
    public static void main(String[] args) throws InterruptedException {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("KryoSpark")
                .master("local[2]")
                .config("spark.rdd.compress", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                //.config("spark.kryo.registrator", "com.zhixinhuixue.bd.model.ZSYKryoRegistrator")
                .getOrCreate();

        Dataset<Row> df = sparkSession.read().parquet("hdfs://192.168.1.243:8020/zsy/warehouse/dws/exam_student_extend");

        //List<Row> rows = df.collectAsList();

        //System.out.println("rows_size--->"+rows.size());

        df.show();

        Thread.sleep(60000);
        sparkSession.stop();

    }
}
