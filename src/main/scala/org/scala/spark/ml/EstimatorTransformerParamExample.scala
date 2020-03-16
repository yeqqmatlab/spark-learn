package org.scala.spark.ml

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object EstimatorTransformerParamExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("EstimatorTransformerParamExample")
      .master("local[2]")
      .getOrCreate()

    //带训练的数据集
    val training = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5)),
    )).toDF("label", "features")

    // 创建一个逻辑回归模型
    val lr = new LogisticRegression()

    println("----->"+lr.explainParams())






    spark.stop()
  }

}
