package org.scala.spark.ml

import org.apache.spark.ml.attribute.Attribute
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.sql.SparkSession

object IndexToStringDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
                    .builder()
                    .master("local[2]")
                    .appName("IndexToString")
                    .getOrCreate()

    val df = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")

    val indexer = new StringIndexer().setInputCol("category").setOutputCol("categoryIndex").fit(df)

    val indexed  = indexer.transform(df)

    indexed.show(10)

    println(s"Transformed string column '${indexer.getInputCol}' " + s"to indexed column '${indexer.getOutputCol}'")

    val inputColSchema  = indexed.schema(indexer.getOutputCol)

    println(s"StringIndexer will store labels in output column metadata: " +
      s"${Attribute.fromStructField(inputColSchema).toString}\n")

    val converter : IndexToString = new IndexToString().setInputCol("categoryIndex").setOutputCol("originalCategory")

    val originalDF = converter.transform(indexed)

    originalDF.show()

    spark.stop()

  }

}
