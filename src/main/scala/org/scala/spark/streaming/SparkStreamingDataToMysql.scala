package org.scala.spark.streaming

import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingDataToMysql {

  def main(args: Array[String]): Unit = {
   //定义状态更新函数
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount  = values.foldLeft(0)(_ + _)
      val previousCount  = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    //创建一个流工具集
    val ssc = new StreamingContext("local[2]", "SparkStreamingDataToMysql", Seconds(3))
    // get DStream from socket
    val lines = ssc.socketTextStream("192.168.1.248", 9999)
    //设置检查点，检查点具有容错机制
    ssc.checkpoint("hdfs://192.168.1.243:8020/test")

    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordAndOne: DStream[(String, Int)] = words.map(x => (x, 1))
    val stateDStream: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc)
    stateDStream.print()

    //把DStream保存到mysql数据库
    stateDStream.foreachRDD(rdd => {
        //内部函数
        def func(records: Iterator[(String,Int)]): Unit ={
            var conn: Connection = null
            var stmt: PreparedStatement = null
          try{
            val url = "jdbc:mysql://192.168.1.210:3307/test"
            val user = "zsy"
            val password = "lc12345"
            conn = DriverManager.getConnection(url, user, password)
            records.foreach(p => {
              val sql = "insert into word_count(word,count) values (?,?)"
              stmt = conn.prepareStatement(sql);
              stmt.setString(1, p._1.trim)
              stmt.setInt(2,p._2.toInt)
              stmt.executeUpdate()
            })
          }catch {
            case e: Exception => e.printStackTrace()
          }finally {
            if(stmt != null){
              stmt.close()
            }
            if(conn != null){
              conn.close()
            }
          }
        }
        val repartitionedRDD = rdd.repartition(3)
        repartitionedRDD.foreachPartition(func)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
