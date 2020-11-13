package graph.neo4j

import org.apache.spark.sql.SparkSession

object WriteNeo4j3 {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("WriteNeo4j3")
      .getOrCreate()

    import spark.implicits._

    val musicDf = Seq(
      (12, "John Bonham", "Orange", "Drums"),
      (19, "John Mayer", "White", "Guitar"),
      (32, "John Scofield", "Black", "Guitar"),
      (15, "John Butler", "Wooden", "Guitar")
    ).toDF("experience", "name", "instrument_color", "instrument")

    musicDf.write
      .format("org.neo4j.spark.DataSource")
      .option("url", "bolt://localhost:7687")
      .option("authentication.basic.username", "neo4j")
      .option("authentication.basic.password", "1qaz2wsx")
      .option("relationship", "PLAYS")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.source.save.mode", "overwrite")
      .option("relationship.source.node.keys", "name:name")
      .option("relationship.target.labels", ":Instrument")
      .option("relationship.target.node.keys", "instrument:name")
      .option("relationship.target.node.properties", "instrument_color:color")
      .option("relationship.target.save.mode", "overwrite")
      .save()
  }
}
