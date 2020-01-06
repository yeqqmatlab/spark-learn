package org.scala.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable

object KafkaDirectWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("DirectKafka")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(2))

    val topicsSet = Array("topic_01")
    val kafkaParams = mutable.HashMap[String, String]()
    //必须添加以下参数，否则会报错
    kafkaParams.put("bootstrap.servers", "192.168.1.239:9092,192.168.1.247:9092,192.168.1.248:9092")
    kafkaParams.put("group.id", "test-group-01")
    kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams
      )
    )

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_.value)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }


}
