package org.scala.spark.streaming

/**
 * 定义案例类
 * @param fee
 * @param orderCode
 * @param sendTime
 */
case class dataModel(fee:BigDecimal,orderCode:String,sendTime:Long)
