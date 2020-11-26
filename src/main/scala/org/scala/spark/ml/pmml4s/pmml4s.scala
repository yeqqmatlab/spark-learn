package org.scala.spark.ml.pmml4s

import scala.io.Source
import org.pmml4s.model.Model

/**
 * 从网络上获取 pmml 文件
 */
object pmml4s {
  def main(args: Array[String]): Unit = {

    // The main constructor accepts an object of org.pmml4s.model.Model
    val model = Model(Source.fromURL(new java.net.URL("http://dmg.org/pmml/pmml_examples/KNIME_PMML_4.1_Examples/single_iris_dectree.xml")))

    val result = model.predict(Map("sepal_length" -> 5.1, "sepal_width" -> 3.5, "petal_length" -> 1.4, "petal_width" -> 0.2))

    print(result)
  }
}
