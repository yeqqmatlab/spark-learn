package org.scala.spark.ml

import java.util.Properties
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql._

/**
 * yqq 2020/11/17
 * 考点关联分析
 */
object FPGrowthMethods {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("FPGrowthMethods")
      .getOrCreate()

    val connectionProperties = new Properties()
    connectionProperties.put("user", "zsy")
    connectionProperties.put("password", "lc12345")

    val url = "jdbc:mysql://192.168.1.210:3307/data_mining?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&rewriteBatchedStatements=true"

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val topicMethodsDF = spark.read.parquet("hdfs://ip243:8020/zsy/warehouse/ods/tiku/topic_methods")

    val dataFrame: DataFrame = topicMethodsDF.select("method_ids").map(row => row.getString(0).split("[,]")).toDF("items")

    val fPGrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.0001).setMinConfidence(0.7)

    val model = fPGrowth.fit(dataFrame)

    val itemsets = model.freqItemsets

    /**
     * 关联考点
     */
    val relevanceMethodDF: Dataset[Row] = itemsets.filter(row => row.getList(0).size() > 1)

    val schema: StructType = relevanceMethodDF.schema add(StructField("association_id",LongType))

    val indexRDD: RDD[(Row, Long)] = relevanceMethodDF.rdd.zipWithIndex()

    val rowRDD = indexRDD.map(r => Row.merge(r._1, Row(r._2)))

    val relevanceMethod2DF = spark.createDataFrame(rowRDD, schema)

    relevanceMethod2DF.withColumn("items",explode(col("items"))).createOrReplaceTempView("association_method")
    spark.sql("select items as method_id,cast(freq as int) as association_num,cast(association_id as int) from association_method").createOrReplaceTempView("association_method")

    /**********************************************************insert to mysql*********************************************************************************/

    val methodDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://192.168.1.210:3307/data_mining?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&rewriteBatchedStatements=true")
      .option("dbtable", "special_solve_method")
      .option("user", "zsy")
      .option("password", "lc12345")
      .load();

    //topicMethodDF.write().mode(SaveMode.Overwrite).parquet("hdfs://ip243:8020/zsy/warehouse/ods/tiku/topic_method");

    methodDF.createOrReplaceTempView("method");

    spark.sql("select ssm_id as method_id ,ssm_content as method_name  from method ").createOrReplaceTempView("method_name");

    val dataframe = spark.sql(" select association_method.*,method_name.method_name from association_method left join method_name on association_method.method_id = method_name.method_id")

    /**
     * 关联考点统计分析
     */
    dataframe.write
      .mode(SaveMode.Overwrite)
      .option("truncate","true")
      .jdbc(url, "association_method", connectionProperties)

    /**
     * 考点在题库出现的频次统计
     */
    val methodTopicStatDF: Dataset[Row] = itemsets.filter(row => row.getList(0).size() == 1)
    methodTopicStatDF.withColumn("items",explode(col("items"))).createOrReplaceTempView("method_stat")

    val resDF = spark.sql("select items as method_id,cast(freq as int) from method_stat")
    resDF.write
      .mode(SaveMode.Overwrite)
      .option("truncate","true")
      .jdbc(url, "method_freq_stat", connectionProperties)

    /**
     * 关联规则条件概率统计
     */
    //model.associationRules.printSchema()
    model.associationRules.createOrReplaceTempView("association_rules")
    val associationRulesDF = spark.sql("select concat_ws(',',antecedent) as antecedent,concat_ws('',consequent) as consequent,round(confidence,3) as confidence,round(lift,3) as lift from association_rules")
    associationRulesDF.write
      .mode(SaveMode.Overwrite)
      .option("truncate","true")
      .jdbc(url, "association_rules", connectionProperties)

    spark.stop()
  }
}
