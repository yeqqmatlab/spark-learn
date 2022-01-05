package org.scala.spark.demo

import java.lang

import org.apache.spark.sql.{Dataset, SparkSession}


object TestRandomSplit {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("TestRandomSplit")
      .master("local[2]")
      .getOrCreate()

    val rdd: Dataset[lang.Long] = spark.range(10)
    val longs = rdd.collect()
    for ( vo <- longs){
      println(vo)
    }

    val array: Array[Dataset[lang.Long]] = rdd.randomSplit(Array(0.5, 0.5),2)
    array(0).foreach( x => println(x))
    array(1).foreach( x => println(x))


    

    spark.stop()
  }

}
