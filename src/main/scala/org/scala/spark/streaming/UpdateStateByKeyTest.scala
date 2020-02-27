package org.scala.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateByKeyTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("UpdateStateByKeyTest")
      .setMaster("local[2]")


    val ssc = new  StreamingContext(conf, Seconds(3))

    // open checkpoint
    ssc.checkpoint("hdfs://192.168.243:8020/input")

    //连接nc(netcat)服务，接收数据源，产生DStream对象
    val linesDStream = ssc.socketTextStream("192.168.1.248", 9999)

    val pairsDStream = linesDStream.flatMap(_.split(" ")).map(word => (word, 1))

    val result = pairsDStream.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      val newValue: Int = state.getOrElse(0)
      // 创建一个变量，用于记录单词出现的次数
      for (value <- values) {
        newValue += value
      }
      Option(newValue)
    })

    result.print()

    ssc.start() //开启实时计算
    ssc.awaitTermination()  //等待应用停止
  }
}
