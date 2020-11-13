package graph.neo4j

import org.apache.spark.sql.{SaveMode, SparkSession}

object WriteNeo4j {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("WriteNeo4j").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val df = Seq(
      ("John"),
      ("Jane")
    ).toDF("name")

    df.write.format("org.neo4j.spark.DataSource")
      .mode(SaveMode.ErrorIfExists)
      .option("url", "bolt://localhost:7687")
      .option("authentication.basic.username", "neo4j")
      .option("authentication.basic.password", "1qaz2wsx")
      .option("labels", ":Person")
      .save()


    spark.stop()
  }
}
