package org.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * created by yqq 2020/2/28
 */
public class TestParquet {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("TestParquet")
                .master("local[2]")
                //.config("spark.sql.parquet.writeLegacyFormat", "true")
                .config("spark.sql.hive.convertMetastoreParquet", "true")
                .getOrCreate();

        //Dataset<Row> df = spark.read().parquet("hdfs://192.168.1.243:8020/test/student_paper_topic_rs");
        //Dataset<Row> df = spark.read().option("mergeSchema", false).parquet("hdfs://192.168.1.243:8020/test/student_paper_topic_rs");
        //df.printSchema();



        /*df.select("student_id").show();
        df.select("paper_id").show();
        df.select("topic_id").show();
        df.select("type").show();
        df.select("is_right").show();
        df.select("handler_index").show();
        df.select("answer").show();
        df.select("answer_url").show();
        df.select("ods_update_time").show();*/
        //df.select("scoring").show();

        //df.show();

        //JavaRDD<Row> rowJavaRDD = df.toJavaRDD();

        Dataset<Row> sqlDF = spark.sql(" SELECT * FROM parquet.`hdfs://192.168.1.243:8020/zsy/warehouse/dws/exam_extend/part-00000-7866e082-2b06-4a71-9424-002a50edb2c3-c000.snappy.parquet`");
        sqlDF.show();

        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("school_id", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("sptr_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("student_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("paper_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("topic_id", DataTypes.LongType, true));
        structFields.add(DataTypes.createStructField("type", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("is_right", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("handler_index", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("answer", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("answer_url", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("scoring", DataTypes.DoubleType, true));
        structFields.add(DataTypes.createStructField("ods_update_time", DataTypes.StringType, true));

        StructType schema = DataTypes.createStructType(structFields);

        //Dataset<Row> dataFrame = spark.createDataFrame(rowJavaRDD, schema);
        //dataFrame.show();

        spark.stop();


    }
}
