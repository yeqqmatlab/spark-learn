package org.sql

import org.apache.spark.sql.SparkSession

object ExceptDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ExceptDemo").master("local[*]").getOrCreate()

    val df1 = spark.createDataFrame(Seq((1, "aa"), (2, "bb"), (3, "dd"))).toDF("id","name")
    val df2 = spark.createDataFrame(Seq((1, "aa"), (2, "bb"))).toDF("id","name")
    val df3 = df1.except(df2)
    df3.show()



    spark.stop()
  }

}
