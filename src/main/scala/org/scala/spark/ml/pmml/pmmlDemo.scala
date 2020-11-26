package org.scala.spark.ml.pmml

import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint


object pmmlDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("pmmlDemo")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    val training = spark.sparkContext
      .parallelize(Seq("0,1 2 3 1", "1,2 4 1 5", "0,7 8 3 6", "1,2 5 6 9").map( line => LabeledPoint.parse(line)))

    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(training)

    val test = spark.sparkContext
      .parallelize(Seq("0,1 2 3 1","1,2 5 1 5","1,2 4 1 2").map( line => LabeledPoint.parse(line)))


    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    predictionAndLabels.toDF().show(10)



    val v1 = Vectors.dense(1.0,2.0,3.0,5)

    val d = model.predict(v1)

    println("d--->",d)

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics.accuracy
    println(s"Accuracy = $accuracy")

    // Save and load model
    //    model.save(spark.sparkContext, "target/tmp/scalaLogisticRegressionWithLBFGSModel")
    //    val sameModel = LogisticRegressionModel.load(spark.sparkContext,"target/tmp/scalaLogisticRegressionWithLBFGSModel")

    //model.toPMML(spark.sparkContext, "G:\\pmml\\spark\\lr\\lr.xml")
    model.save(spark.sparkContext,"G:\\pmml\\spark\\lr\\model")




    spark.stop()

  }
}
