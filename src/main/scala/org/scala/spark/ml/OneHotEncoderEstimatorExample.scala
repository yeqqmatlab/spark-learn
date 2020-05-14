package org.scala.spark.ml

import org.apache.spark.ml.feature.OneHotEncoderEstimator
import org.apache.spark.sql.SparkSession

object OneHotEncoderEstimatorExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OneHotEncoderEstimatorExample").master("local[2]").getOrCreate()

    val df = spark.createDataFrame(Seq(
      (0.0, 1.0),
      (1.0, 0.0),
      (2.0, 1.0),
      (0.0, 2.0),
      (0.0, 1.0),
      (2.0, 0.0)
    )).toDF("categoryIndex1", "categoryIndex2")

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("categoryIndex1", "categoryIndex2"))
      .setOutputCols(Array("categoryVec1", "categoryVec2")).setDropLast(false)
    val model = encoder.fit(df)

    
    val encoded = model.transform(df)
    encoded.show()

    /**
     * +--------------+--------------+-------------+-------------+
     * |categoryIndex1|categoryIndex2| categoryVec1| categoryVec2|
     * +--------------+--------------+-------------+-------------+
     * |           0.0|           1.0|(3,[0],[1.0])|(3,[1],[1.0])|
     * |           1.0|           0.0|(3,[1],[1.0])|(3,[0],[1.0])|
     * |           2.0|           1.0|(3,[2],[1.0])|(3,[1],[1.0])|
     * |           0.0|           2.0|(3,[0],[1.0])|(3,[2],[1.0])|
     * |           0.0|           1.0|(3,[0],[1.0])|(3,[1],[1.0])|
     * |           2.0|           0.0|(3,[2],[1.0])|(3,[0],[1.0])|
     * +--------------+--------------+-------------+-------------+
     */

    spark.stop()
  }

}
