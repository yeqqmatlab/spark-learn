package org.scala.spark.streaming

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe



object KafkaToHbase {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("KafkaToHbase").master("local[*]").getOrCreate()
    val sparkContext = spark.sparkContext
    val ssc = new StreamingContext(sparkContext, Seconds(1))

    val topic = "pay_topic2"
    val topics = Array(topic)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ip239:9092,ip247:9092,ip248:9092",
      "group.id"->"test-group-01",
      "auto.offset.reset"->"latest",
      "key.deserializer"-> classOf[StringDeserializer],
      "value.deserializer"-> classOf[StringDeserializer],
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val messages: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    val jsonData: DStream[JSONObject] = messages.flatMap(line => {
      val data: JSONObject = JSON.parseObject(line.value())
      Some(data)
    })

    jsonData.foreachRDD(rdd => {
      rdd.foreachPartition( part => {
        // hbase conf
        val tableName = "pay_info"
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum","ip239,ip243,ip244")
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
        val connection = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hbaseConf)
        val table = connection.getTable(TableName.valueOf(tableName))

        part.foreach( obj => {
          val orderCode = obj.getString("orderCode")
          val fee = obj.getFloat("fee")
          val sendTime = obj.getLong("sendTime")
          val put = new Put(Bytes.toBytes(orderCode))
          put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("fee"),Bytes.toBytes(fee))
          put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("sendTime"),Bytes.toBytes(sendTime))
          table.put(put)

        })

      })

    })

    ssc.start()
    ssc.awaitTermination()
  }

}
