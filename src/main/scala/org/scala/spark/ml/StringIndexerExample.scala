package org.scala.spark.ml

import org.apache.spark.ml.feature.{OneHotEncoder, OneHotEncoderEstimator, StringIndexer}
import org.apache.spark.sql.SparkSession

object StringIndexerExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    val tuples: Seq[(Int, String)] = Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))

    val df = spark.sqlContext.createDataFrame(tuples).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("category_index")

    val indexerModel = indexer.fit(df)

    val resDF = indexerModel.transform(df)

    resDF.show()

    /**
     * OneHotEncoder 独热编码
     */
    /*val encoder = new OneHotEncoder().setInputCol("category_index").setOutputCol("category_vector").setDropLast(false)

    val encoderDF = encoder.transform(resDF)
    encoderDF.show()*/

    /**
     * OneHotEncoderEstimator
     */
    val estimator = new OneHotEncoderEstimator().setInputCols(Array("category_index")).setOutputCols(Array("category_vector")).setDropLast(false)
    val model = estimator.fit(resDF)
    val esDF = model.transform(resDF)
    esDF.show()

    /**
     * +---+--------+--------------+---------------+
     * | id|category|category_index|category_vector|
     * +---+--------+--------------+---------------+
     * |  0|       a|           0.0|  (3,[0],[1.0])|
     * |  1|       b|           2.0|  (3,[2],[1.0])|
     * |  2|       c|           1.0|  (3,[1],[1.0])|
     * |  3|       a|           0.0|  (3,[0],[1.0])|
     * |  4|       a|           0.0|  (3,[0],[1.0])|
     * |  5|       c|           1.0|  (3,[1],[1.0])|
     * +---+--------+--------------+---------------+
     */

    spark.stop()

  }
}
