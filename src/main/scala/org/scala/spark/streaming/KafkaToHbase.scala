package org.scala.spark.streaming

import java.lang

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.collection.mutable



object KafkaToHbase {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("KafkaToHbase").master("local[2]").getOrCreate()
    val sparkContext = spark.sparkContext
    /**
     * 设置日志级别
     */
    Logger.getRootLogger.setLevel(Level.WARN)

    val ssc = new StreamingContext(sparkContext, Seconds(3))

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

    val msgDS = messages.map(_.value())

    val dataDS: DStream[(java.math.BigDecimal, String, lang.Long)] = msgDS.map(json => {
      val jsonObject = JSON.parseObject(json)
      val fee = jsonObject.getBigDecimal("fee")
      val orderCode = jsonObject.getString("orderCode")
      val sendTime = jsonObject.getLong("sendTime")
      (fee, orderCode, sendTime)
    })

    dataDS.foreachRDD(rdd => {
      rdd.foreachPartition( part => {
        // hbase conf
        val tableName = "pay_info"
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum","ip239,ip243,ip244")
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
        val connection = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hbaseConf)
        val table = connection.getTable(TableName.valueOf(tableName))

        part.foreach( row => {
          val fee: java.math.BigDecimal = row._1
          val orderCode = row._2
          val sendTime = row._3
          val put = new Put(Bytes.toBytes(orderCode))
          put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("fee"),Bytes.toBytes(fee))
          put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("sendTime"),Bytes.toBytes(sendTime))
          table.put(put)
        })
        connection.close()
      })
    })
    dataDS.print(20)
    ssc.start()
    ssc.awaitTermination()
  }

}
