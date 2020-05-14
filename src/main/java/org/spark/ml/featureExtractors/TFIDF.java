package org.spark.ml.featureExtractors;

import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * created by yqq 2020/5/14
 */
public class TFIDF {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("TFIDF")
                .master("local[2]")
                .getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(0.0, "Hi I heard about Spark"),
                RowFactory.create(0.0, "I wish Java could use case classes"),
                RowFactory.create(1.0, "Logistic regression models are neat")
        );
        //创建Schema给Row定义数据类型和fieldName
        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> sentenceData = spark.createDataFrame(data, schema);

        //使用Tokenizer来将句子分割成单词
        Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
        Dataset<Row> wordsData = tokenizer.transform(sentenceData);

        //使用HashingTF将句子中的单词哈希成特征向量（这个可以在前一章节的最后输出打印截图中看到具体的值）
        int numFeatures = 20;
        HashingTF hashingTF = new HashingTF()
                .setInputCol("words")
                .setOutputCol("rawFeatures")
                .setNumFeatures(numFeatures);

        Dataset<Row> featurizedData = hashingTF.transform(wordsData);
        // alternatively, CountVectorizer can also be used to get term frequency vectors

        //使用IDF对上面产生的特征向量进行rescale
        IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
        IDFModel idfModel = idf.fit(featurizedData); //fit得到IDF的模型

        Dataset<Row> rescaledData = idfModel.transform(featurizedData); //对特征向量进行rescale
        rescaledData.printSchema();
        rescaledData.show();

        spark.stop();

    }
}
