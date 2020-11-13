package graph.neo4j

import org.apache.spark.sql.SparkSession

object ReadRelationshipFromNeo4j {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("ReadRelationshipFromNeo4j").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val dataFrame = spark.read.format("org.neo4j.spark.DataSource")
      .option("url", "bolt://localhost:7687")
      .option("authentication.basic.username", "neo4j")
      .option("authentication.basic.password", "1qaz2wsx")
      .option("relationship", "TOPIC")
      .option("relationship.source.labels", "Method")
      .option("relationship.target.labels", "Method")
      .load()

    dataFrame.show()

    spark.stop()
  }

}
