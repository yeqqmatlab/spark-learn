package org.scala.spark.phoenix

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object Demo {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
                                    .builder()
                                    .master("local[2]")
                                    .appName("phoenix")
                                    .getOrCreate()

    /*val df: DataFrame = spark.sqlContext
      .read
      .format("org.apache.phoenix.spark")
      .options(Map("table" -> "web_stat", "zkUrl" -> "ip244,ip243,ip239:2181"))
      .load()

    df.show(50)*/

    val personDF: DataFrame = spark.createDataFrame(Seq(
      ("20", "tom2", 28),
      ("21", "jack2", 38),
      ("22", "java3", 58)
    )).toDF("id", "name", "age")

    personDF.write.format("org.apache.phoenix.spark")
      .mode(SaveMode.Overwrite)
      .option("zkUrl", "ip244,ip243,ip239:2181")
      .option("table", "test_phoenix2")
      .save()

    spark.stop()
  }
}
