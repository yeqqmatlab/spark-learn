package org.scala.spark.structure.streaming

import org.apache.spark.sql.SparkSession
import java.util.UUID

import org.apache.spark.sql.streaming.Trigger

object ReadDataFromKafka {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local[2]").appName("ReadDataFromKafka").getOrCreate()

    import sparkSession.implicits._

    val df = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "ip239:9092,ip247:9092,ip248:9092")
      .option("subscribe", "topic_03")
      .option("startingOffsets", "earliest")
      .option("group.id","test-group-01")
      .load()

    val checkpointLocation = "G:\\tmp\\temporary-" + UUID.randomUUID.toString

    val lines = df.selectExpr(" CAST(value AS STRING)")
      .as[(String)]

    // Generate running word count
    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("checkpointLocation", "hdfs://192.168.1.243:8020/test/checkpoint")
      .trigger(Trigger.ProcessingTime(1))
      .start()

    query.awaitTermination()


  }
}
