package org.scala.spark.ml

import java.util.Properties
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SaveMode, SparkSession}
object FPGrowthMethods {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("FPGrowthMethods").getOrCreate()

    val connectionProperties = new Properties()
    connectionProperties.put("user", "zsy")
    connectionProperties.put("password", "lc12345")

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val topicMethodsDF = spark.read.parquet("hdfs://ip243:8020/zsy/warehouse/ods/tiku/topic_methods")

    val dataFrame: DataFrame = topicMethodsDF.select("method_ids").map(row => row.getString(0).split("[,]")).toDF("items")

    val fPGrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.0001).setMinConfidence(0.7)

    val model = fPGrowth.fit(dataFrame)

    val itemsets = model.freqItemsets

    //itemsets.printSchema()

    /**
     * 关联考点
     */
    val relevanceMethodDF: Dataset[Row] = itemsets.filter(row => row.getList(0).size() > 1)

    val schema: StructType = relevanceMethodDF.schema add(StructField("relevance_id",LongType))

    val indexRDD: RDD[(Row, Long)] = relevanceMethodDF.rdd.zipWithIndex()

    val rowRDD = indexRDD.map(r => Row.merge(r._1, Row(r._2)))

    val relevanceMethod2DF = spark.createDataFrame(rowRDD, schema)

    relevanceMethod2DF.withColumn("items",explode(col("items"))).createOrReplaceTempView("relevance_method")
    val relevanceMethod3DF = spark.sql("select items as method_id,cast(freq as int) as relevance_num,cast(relevance_id as int) from relevance_method")

    relevanceMethod3DF.write
      .mode(SaveMode.Overwrite)
      .option("truncate","true")
      .jdbc("jdbc:mysql://192.168.1.210:3307/test?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&rewriteBatchedStatements=true", "relevance_method", connectionProperties);

    /**
     * 考点在题库出现的频次统计
     */
    val methodTopicStatDF: Dataset[Row] = itemsets.filter(row => row.getList(0).size() == 1)
    methodTopicStatDF.withColumn("items",explode(col("items"))).createOrReplaceTempView("method_stat")

    val resDF = spark.sql("select items as method_id,cast(freq as int) as method_topic_num from method_stat")
    resDF.write
      .mode(SaveMode.Overwrite)
      .option("truncate","true")
      .jdbc("jdbc:mysql://192.168.1.210:3307/test?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&rewriteBatchedStatements=true", "method_topic_stat", connectionProperties);

    model.associationRules.show()

    spark.stop()
  }

}
