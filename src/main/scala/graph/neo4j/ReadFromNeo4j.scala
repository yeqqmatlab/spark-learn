package graph.neo4j

import org.apache.spark.sql.SparkSession

object ReadFromNeo4j {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("ReadFromNeo4j").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val dataFrame = spark.read.format("org.neo4j.spark.DataSource")
      .option("url", "bolt://localhost:7687")
      .option("authentication.basic.username", "neo4j")
      .option("authentication.basic.password", "1qaz2wsx")
      .option("labels", "Method")
      .load()

    dataFrame.show()

    spark.stop()
  }

}
