package org.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by yqq on 2020/2/25.
 */
public class NetworkWordCount2 {
    public static void main(String[] args) throws InterruptedException {

        //create a local StreamingContext with two working thread and batch interval of 1 second
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount2");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        JavaReceiverInputDStream<String> linesRDD = jssc.socketTextStream("localhost", 9999);

        JavaDStream<String> wordsRDD = (JavaDStream<String>) linesRDD.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        JavaPairDStream<String, Integer> wordAndOne = (JavaPairDStream<String, Integer>) wordsRDD.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairDStream<String, Integer> wordCounts = wordAndOne.reduceByKey((x1, x2) -> x1 + x2);

        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();


    }
}
