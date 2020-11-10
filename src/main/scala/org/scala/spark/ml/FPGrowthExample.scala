package org.scala.spark.ml

import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 关联性分析
 */
object FPGrowthExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
                          .builder()
                          .appName("FPGrowthExample")
                          .master("local[2]")
                          .getOrCreate()
    import spark.implicits._

    val dataset: DataFrame = spark.createDataset(Seq(
      "1 2 5",
      "1 2 3 5",
      "1 2")
    ).map(t => t.split(" ")).toDF("items")

    val fPGrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.5).setMinConfidence(0.6)

    val model = fPGrowth.fit(dataset)

    model.freqItemsets.show()

    model.associationRules.show()

    spark.stop()
  }

}
