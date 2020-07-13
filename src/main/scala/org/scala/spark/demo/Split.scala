package org.scala.spark.demo

import org.apache.hadoop.io.NullWritable

import org.apache.spark._
import org.apache.spark.SparkContext._

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    key.toString+"/"+"aaa"
}

object Split {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.parallelize(List((20180701, "aaa"), (20180702, "bbb"), (20180701, "ccc")))
      .saveAsHadoopFile("output", classOf[String], classOf[String],
        classOf[RDDMultipleTextOutputFormat])
    sc.stop()
  }
}