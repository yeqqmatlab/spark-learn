package org.scala.spark.structure.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.streaming.Trigger


object ReadDataFromKafka2 {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local[2]").appName("ReadDataFromKafka2").getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)
    import sparkSession.implicits._

    val lines: Dataset[String] = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "ip239:9092,ip247:9092,ip248:9092")
      .option("subscribe", "payTopic")
      //.option("startingOffsets", "earliest")
      .option("group.id", "test-group-02")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    val wordCounts: DataFrame = lines.flatMap(_.split(" ")).groupBy("value").count()
    // Generate running word count

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("checkpointLocation", "hdfs://192.168.1.243:8020/test/checkpoint5")
      //.option("enable.auto.commit", true)
      //.trigger(Trigger.ProcessingTime(19000))
      .start()

    query.awaitTermination()


  }
}
