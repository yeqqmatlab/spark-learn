package org.scala.spark.structure.streaming

import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import org.apache.spark.sql.functions._


object WindowOnEventTimeDemo {

  case class TimeWord(word:String, timestamp:Timestamp)
  def main(args: Array[String]): Unit = {

    val host = "192.168.1.248"
    val port = 9999
    //窗口长度 单位秒
    val windowSize = 10
    //窗口滑动距离 单位秒
    val slideSize = 10
    if(slideSize > windowSize){
      System.err.println("窗口滑动距离应当小于等于窗口距离")
    }

    //以秒为单位
    val windowDuration = s"$windowSize seconds"
    val slideDuration = s"$slideSize seconds"

    val sparkSession = SparkSession
      .builder()
      .master("local[2]")
      .appName("WindowOnEventTimeDemo")
      .getOrCreate()

    import sparkSession.implicits._

    // 创建DataFrame
    val lines = sparkSession.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .option("includeTimestamp", true)//添加时间戳
      .load()

    //分词
    val wordsDF = lines.as[(String, Timestamp)]
                        .flatMap(line => line._1.split(" ").map(word => TimeWord(word, line._2)))
                        .toDF()

    //计数
    val windowedCounts = wordsDF.groupBy(
      window($"timestamp",windowDuration,slideDuration),$"word"
    ).count().orderBy("window")

    //查询
    val query = windowedCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()

  }
}
