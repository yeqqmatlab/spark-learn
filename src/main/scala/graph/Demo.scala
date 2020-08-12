package graph

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * graph demo
 */
object Demo {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
                            .builder()
                            .appName("Demo")
                            .master("local[2]")
                            .getOrCreate()

    val sc = sparkSession.sparkContext

    val user: RDD[(Long, (String, String))] = sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))


    val relationships: RDD[Edge[String]] = sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    //构建图结构  图 = 定点 + 边
    val graph: Graph[(String, String), String] = Graph(user, relationships)

    val count: Long = graph.vertices.filter(_._2._2 == "postdoc").count()

    println("count--->"+count)

    val count2: Long = graph.edges.filter(e => e.attr == "colleague").count()

    println(count2)


    sparkSession.stop()
  }

}
