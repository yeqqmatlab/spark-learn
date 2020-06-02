package org.scala.spark.ml

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.SparkSession

object KMeansExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("KMeansExample")
      .master("local[2]")
      .getOrCreate()

    val dataset = spark.read.format("libsvm").load("data/mllib/sample_kmeans_data.txt")
    val kMeans = new KMeans()
    val model = kMeans.fit(dataset)

    val predictions = model.transform(dataset)
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    spark.stop()
  }
}
