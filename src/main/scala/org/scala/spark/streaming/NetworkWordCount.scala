package org.scala.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCount {

  def main(args: Array[String]): Unit = {

    //StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines = ssc.socketTextStream("192.168.1.248", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordsCount: DStream[(String, Int)] = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordsCount.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
