package graph.neo4j

import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random

object WriteNeo4j2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("WriteNeo4j2")
      .getOrCreate()

    import spark.implicits._

    val total = 10
    val rand = Random
    val ds = (1 to total)
      .map(i => {
        Person(name = "java " + i, "spark " + i, rand.nextInt(100),
        Point3d(srid = 4979, x = 12.5811776, y = 41.9579492, z = 1.3))
      }).toDS()

    ds.write
      .format("org.neo4j.spark.DataSource")
      .mode(SaveMode.ErrorIfExists)
      .option("url", "bolt://localhost:7687")
      .option("authentication.basic.username", "neo4j")
      .option("authentication.basic.password", "1qaz2wsx")
      .option("labels", ":Person:Customer")
      .save()

    spark.stop()
  }
}

case class Point3d(`type`: String = "point-3d",
                   srid: Int,
                   x: Double,
                   y: Double,
                   z: Double)

case class Person(name: String, surname: String, age: Int, livesIn: Point3d)
