package org.scala.spark.kyro

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object KryoApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("KryoApp").setMaster("local[2]")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.rdd.compress", "true")
    conf.registerKryoClasses(Array(classOf[Row]))

    val sc = new SparkContext(conf)

    val logRDD: RDD[Logger] = sc.textFile("G:\\txt").map(x => {
      val fields: Array[String] = x.split("[,]")
      Logger(fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6), fields(7))
    })

    //Dataset<Row> df = spark.read().parquet("hdfs://192.168.1.243:8020/test/student_paper_topic_rs");


    val persistRDD = logRDD.persist(StorageLevel.MEMORY_ONLY_SER)
    //val persistRDD = logRDD.persist(StorageLevel.MEMORY_ONLY_SER)

    //println("---->"+logRDD.count())
    println("---->"+persistRDD.count())

    Thread.sleep(90000)
    sc.stop()
  }

  case class Logger(filed1: String, filed2: String, filed3: String,
                    filed4: String, filed5: String, filed6: String,
                    filed7: String, filed8: String)

}
