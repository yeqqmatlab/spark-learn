package org.scala.spark.ml

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.linalg.Vector

/**
 * pipeline管线测试
 * 模型保存和加载
 */
object PipelineExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("PipelineExample")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val training = spark.createDataFrame(Seq(
      (0L, "a b c d e  python", 1.0),
      (1L, "spark hadoop java", 0.0),
      (2L, "python f g h ", 1.0),
      (3L, "apache hadoop mapreduce kafka hbase flink", 0.0)
    )).toDF("id", "text", "label")

    //分词器
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    //hash转换器
    val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")
    //逻辑回归分类器
    val lr = new LogisticRegression().setMaxIter(100).setRegParam(0.001)
    //创建管道
    val pipeline = new Pipeline().setStages(Array(tokenizer,hashingTF,lr))

    //训练数据
    val model = pipeline.fit(training)

    //save model to disk or hdfs
    //model.write.overwrite().save("G:\\ml_model")
    model.write.overwrite().save("hdfs://192.168.1.243:8020/test/model")

    //load model
    //val loadModel = PipelineModel.load("G:\\ml_model")
    val loadModel = PipelineModel.load("hdfs://192.168.1.243:8020/test/model")

    //test data
    val test = spark.createDataFrame(Seq(
      (4L, "python a b c aa"),
      (5L, "l m n"),
      (6L, "spark hadoop spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    //test
    loadModel.transform(test).select("id","text","probability","prediction")
        .collect()
        .foreach{
          case Row(id:Long,text:String,prob:Vector,prediction:Double) =>
          println(s"($id,$text)--->prob=$prob,prediction=$prediction")
        }

    //Thread.sleep(30000)
    spark.stop()
  }
}
