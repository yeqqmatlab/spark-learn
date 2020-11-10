package org.scala.spark.ml

import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object FPGrowthMethods {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("FPGrowthMethods").getOrCreate()

    import spark.implicits._

    val topicMethodsDF = spark.read.parquet("hdfs://ip243:8020/zsy/warehouse/ods/tiku/topic_methods")

    val dataFrame: DataFrame = topicMethodsDF.select("method_ids").map(row => row.getString(0).split("[,]")).toDF("items")

    val fPGrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.0001).setMinConfidence(0.7)

    val model = fPGrowth.fit(dataFrame)

    model.freqItemsets.show()

    model.associationRules.show()

    spark.stop()
  }

}
