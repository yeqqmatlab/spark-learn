package org.scala.spark.ml

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{Row, SparkSession}

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

    lr.setMaxIter(10)
    lr.setRegParam(0.01)

    val model1 = lr.fit(training)

    println(s"Model 1 was fit using parameters------>: ${model1.parent.extractParamMap}")

    val paraMap = ParamMap(lr.maxIter -> 20)
      .put(lr.maxIter, 30)
      .put(lr.regParam -> 0.1, lr.threshold -> 0.55)

    val paraMap2 = ParamMap(lr.probabilityCol -> "myProbability")

    val paramMap3 = paraMap++paraMap2

    val model2 = lr.fit(training, paramMap3)
    println(s"Model 2 was fit using parameters: ${model2.parent.extractParamMap}")

    // Prepare test data.
    val test = spark.createDataFrame(Seq(
      (0.0, Vectors.dense(-1.0, 1.5, 1.3)),
      (0.0, Vectors.dense(3.0, 2.0, -0.1)),
      (1.0, Vectors.dense(0.0, 2.2, -1.5))
    )).toDF("label", "features")

    model2.transform(test)
      .select("features", "label", "myProbability", "prediction")
      .collect()
      .foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
        println(s"($features, $label) -> prob=$prob, prediction=$prediction")
      }


    spark.stop()
  }

}
