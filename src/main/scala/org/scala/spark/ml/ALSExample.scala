package org.scala.spark.ml

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scala.spark.streaming.StreamingExamples



/***
 * spark 协同过滤算法
 */
object ALSExample {

  case class Rating(userId: Int,movieId: Int, rating: Float, timestamp: Long)
  def parseRating(str: String):Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt,fields(1).toInt,fields(2).toFloat,fields(3).toLong)
  }

  def main(args: Array[String]): Unit = {

    //Logger.getRootLogger.setLevel(Level.WARN)
    StreamingExamples.setStreamingLogLevels()

    val spark = SparkSession.builder().master("local[2]").appName("ALSExample").getOrCreate()
    import spark.implicits._

    val ratings = spark
                      .read
                      .textFile("data/mllib/als/sample_movielens_ratings.txt")
                      .map(parseRating)
                      .toDF()

    //把数据切割成训练数据和测试数据
    val Array(training,test) = ratings.randomSplit(Array(0.8, 0.2))

    //Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    val model = als.fit(training)

    /**
     * Spark允许用户将coldStartStrategy参数设置为“drop”，以便删除包含NaN值的预测的DataFrame中的任何行。然后将根据非NaN数据计算评估度量并且该评估度量将是有效的
     */
    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    val rmse = evaluator.evaluate(predictions)

    println(s"Root-mean-square error = $rmse")

    //给所有人推荐10部电影
    val userRecs = model.recommendForAllUsers(10)

    //给所有的电影推荐10个人
    val movieRecs = model.recommendForAllItems(10)

    //前3人推荐10部电影
    val users = ratings.select(als.getUserCol).distinct().limit(3)
    val userSubsetRecs = model.recommendForUserSubset(users, 10)

    // 前3部电影关联10个人
    val movies = ratings.select(als.getItemCol).distinct().limit(3)
    val movieSubSetRecs = model.recommendForItemSubset(movies, 10)

    // $example off$
    userRecs.show()
    userRecs.coalesce(1).write.mode(SaveMode.Overwrite).json("C:\\Users\\Administrator\\Desktop\\als1")

    movieRecs.show()
    movieRecs.coalesce(1).write.mode(SaveMode.Overwrite).json("C:\\Users\\Administrator\\Desktop\\als2")

    userSubsetRecs.show()
    userSubsetRecs.coalesce(1).write.mode(SaveMode.Overwrite).json("C:\\Users\\Administrator\\Desktop\\als3")

    movieSubSetRecs.show()
    movieSubSetRecs.coalesce(1).write.mode(SaveMode.Overwrite).json("C:\\Users\\Administrator\\Desktop\\als4")

    spark.stop()
  }

}
