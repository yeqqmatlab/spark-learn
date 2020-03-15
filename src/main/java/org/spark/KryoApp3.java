package org.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * created by yqq 2020/3/11
 */
public class KryoApp3 {
    public static void main(String[] args) throws InterruptedException {

        SparkConf sparkConf = new SparkConf().setAppName("SparkRDD03").setMaster("local[2]");
        sparkConf.set("spark.rdd.compress","true");
        sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
        //conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        //sparkConf.registerKryoClasses(new Class[]{Row.class});
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        SparkSession spark = SparkSession
                .builder()
                .config(sc.getConf())
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("delimiter", ",").csv("G:\\txt");

        StructType schema = getSchema();
        Dataset<Row> dataFrame = spark.createDataFrame(dataset.toJavaRDD(), schema);

        /*JavaRDD<String> textFile = sc.textFile("G:\\txt");
        JavaRDD<Row> javaRDD = textFile.map(s -> {
            String[] split = s.split("[,]");
            return RowFactory.create(split[0], split[1],split[2], split[3],split[4], split[5],split[6], split[7]);
        });*/


        dataFrame.persist();
        dataFrame.createOrReplaceTempView("t1");
        //JavaRDD<Row> persist = javaRDD.persist(StorageLevel.MEMORY_ONLY_SER());

       //Dataset<Row> dataFrame = sparkSession.createDataFrame(javaRDD, getSchema());

        //Dataset<Row> persist = dataFrame.persist(StorageLevel.MEMORY_ONLY_SER());
//        System.out.println("--->"+persist.count());
        spark.sql("select count(*) from t1").show();

        spark.sql("select count(distinct school_id) from t1").show();

        Thread.sleep(100000);


        sc.stop();
    }

    private static StructType getSchema(){

        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("school_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("sptr_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("student_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("paper_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("topic_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("type", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("is_right", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("handler_index", DataTypes.StringType, true));

        return DataTypes.createStructType(structFields);
    }

}
