package org.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Iterator;

/**
 * created by yqq 2020/3/11
 */
public class KryoApp2 {
    public static void main(String[] args) {



        SparkSession sparkSession = SparkSession
                .builder()
                .appName("KryoSpark")
                .master("local[2]")
                .config("spark.rdd.compress", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryo.registrator", "com.zhixinhuixue.bd.model.ZSYKryoRegistrator")
                .getOrCreate();

        //sparkSession.sparkContext().


    }
}
