package org.scala.spark.streaming

import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable

object KafkaDirectWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("DirectKafka")
      .setMaster("local[2]")

    /**
     * 每5秒处理一个批次的数据
     */
    val ssc = new StreamingContext(conf, Seconds(5))

    /**
     * 设置日志级别
     */
    Logger.getRootLogger.setLevel(Level.WARN)

    val topicsSet = Array("payTopic")
    val kafkaParams = mutable.HashMap[String, String]()
    //kafka参数
    kafkaParams.put("bootstrap.servers", "ip239:9092,ip247:9092,ip248:9092")
    kafkaParams.put("group.id", "test-group-02")
    kafkaParams.put("auto.offset.reset", "earliest")
    kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val messages: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams
      )
    )
    // Get kafka message
    val jsonObj: DStream[String] = messages.map(_.value)
    //jsonObj.print()
    val dataDS: DStream[dataModel] = jsonObj.map(json => {
      val jsonObject = JSON.parseObject(json)
      val fee = jsonObject.getBigDecimal("fee")
      val orderCode = jsonObject.getString("orderCode")
      val sendTime = jsonObject.getLong("sendTime")
      dataModel(fee, orderCode, sendTime)
    })

    /**
     * 每5秒订单合计价格
     */
    val sumDS: DStream[BigDecimal] = dataDS.map(vo => vo.fee).reduce(_ + _)
    sumDS.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}

/**
 * 定义案例类
 * @param fee
 * @param orderCode
 * @param sendTime
 */
case class dataModel(fee:BigDecimal,orderCode:String,sendTime:Long)
