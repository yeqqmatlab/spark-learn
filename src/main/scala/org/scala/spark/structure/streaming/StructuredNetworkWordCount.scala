package org.scala.spark.structure.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
  * Created by yqq on 2020/3/11.
  */
object StructuredNetworkWordCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._

    val lines = spark
      .readStream
      .format("socket")
      .option("host", "192.168.1.248")
      .option("port", 9999)
      .load()

    val words = lines.as[String].flatMap(_.split(" "))

    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("checkpointLocation", "hdfs://192.168.1.243:8020/test/checkpoint")
      .trigger(Trigger.ProcessingTime(3))
      .start()

    query.awaitTermination()

  }

}
