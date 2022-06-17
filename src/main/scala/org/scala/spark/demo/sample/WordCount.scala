package org.scala.spark.demo.sample

import org.apache.spark.sql.SparkSession

/**
 * 通过采样查看key分布情况
 */
object WordCount {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("WordCount").master("local[2]").getOrCreate()

    val data = Array("A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A",
      "A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A",
      "A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A",
      "A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A",
      "A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A","A",
      "B","B","B","B","B","B","B","B","C","D","E","F","B","B","B","B","B","B","B","C","D","E","F",
      "B","B","B","B","B","B","B","B","C","D","E","F","G")

    val rdd = sparkSession.sparkContext.parallelize(data)

    val tuples = rdd.map(line => (line, 1))
      .sample(true, 0.4)
      .reduceByKey((x, y) => x + y)
      .map(line => (line._2, line._1))
      .sortBy(line => line._1, false, 2)
      .take(3)

    for (elem <- tuples) {
      println(elem._2 + "--->" + elem._1)
    }

    sparkSession.stop()
  }

}
