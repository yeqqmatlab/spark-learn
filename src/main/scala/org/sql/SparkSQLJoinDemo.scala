package org.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQLJoinDemo {

  def main(args: Array[String]): Unit = {

    //程序的入口
    val spark = SparkSession.builder().appName("SparkSQLJoinDemo").master("local[*]").getOrCreate()
    //导入spark对象的隐式转换
    import spark.implicits._



    //spark.sql.autoBroadcastJoinThreshold = -1
    //不限定小表的大小
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    // 每个分区的平均大小不超过spark.sql.autoBroadcastJoinThreshold设定的值
    spark.conf.set("spark.sql.join.preferSortMergeJoin", true)

    println(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))


    //定义两个集合，转换成dataframe
    val df1: DataFrame = Seq(
      ("0", "a"),
      ("1", "b"),
      ("2", "c")
    ).toDF("id", "name")


    val df2: DataFrame = Seq(
      ("0", "d"),
      ("1", "e"),
      ("2", "f")
    ).toDF("aid", "aname")

    //重新分区
    df2.repartition()

    //df1.cache().count()

    //进行连接
    val result = df1.join(df2,$"id" === $"aid")

    //查看执行计划
    result.explain()

    //展示结果
    result.show()

    //释放资源
    spark.stop()

  }


}
