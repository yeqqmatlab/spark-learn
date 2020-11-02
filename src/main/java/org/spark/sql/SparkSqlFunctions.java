package org.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.Struct;

/**
 * spark-sql 内置函数 demo
 * created by yqq 2020/7/2
 */
public class SparkSqlFunctions {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSqlFunctions")
                .master("local[2]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> examDF = spark.read().parquet("hdfs://ip243:8020/zsy/warehouse/ods/school/exam");
        examDF.printSchema();

        Dataset<Row> scoreDF = spark.read().parquet("hdfs://ip243:8020/zsy/warehouse/dws/student_method_score");
        scoreDF.printSchema();

        examDF.createOrReplaceTempView("exam");
        scoreDF.createOrReplaceTempView("score");

        /**
         * 近似去重统计函数 approx_count_distinct（）
         */
        spark.sql("SELECT approx_count_distinct(col1) FROM VALUES (1), (1), (2), (2), (3) tab(col1)").show();

        spark.sql("SELECT approx_count_distinct(school_id) as approx_count_distinct FROM exam").show();

        spark.sql("SELECT count(distinct school_id) as count_distinct FROM exam").show();

        /**
         * percentile_approx(grade, 0.5) 取得排位在倒数第50%的成绩。（使用时会对成绩进行排序,一般可以用于求中位数）
         */
        //spark.sql("select percentile_approx(scoring,0.5) from student_methods_score where school_id = 115 and topic_id = 485483").show();

        /**
         * 求平均数
         */
        //spark.sql("SELECT exam_group_id,avg(total_score) FROM exam group by exam_group_id").show();

        /**
         * 报错
         * bit_or 位 或 运算
         */
        //spark.sql("SELECT bit_or(col) FROM VALUES (3), (5) AS tab(col)").show();


        spark.sql("SELECT school_id,count_if(status == 1) FROM score group by school_id").show();






        spark.stop();
    }
}
