package graph

import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.spark.sql.SparkSession

/**
 * 三角形设计社交网络
 * 三角计数
 */
object TriangleCountingExample {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
                          .builder()
                          .master("local[2]")
                          .appName("TriangleCountingExample")
                          .getOrCreate()
    val sc = sparkSession.sparkContext
    val graph = GraphLoader.edgeListFile(sc, "data/graphx/followers.txt", true)
      .partitionBy(PartitionStrategy.RandomVertexCut)

    val triCounts = graph.triangleCount().vertices

    val users = sc.textFile("data/graphx/users.txt").map { line =>
      val fields = line.split("[,]")
      (fields(0).toLong, fields(1))
    }

    val triCountByUserName = users.join(triCounts).map { case (id, (username, tc)) =>
      (username, tc)
    }

    println(triCountByUserName.collect().mkString("\n"))

    sparkSession.stop()
  }

}
