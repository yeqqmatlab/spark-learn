//package org.spark.streaming;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.StorageLevels;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.storage.StorageLevel;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.State;
//import org.apache.spark.streaming.StateSpec;
//import org.apache.spark.streaming.StreamingContext;
//import org.apache.spark.streaming.api.java.*;
//import scala.Function;
//import scala.Function3;
//import scala.Option;
//import scala.Tuple2;
//
//import java.util.Arrays;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Optional;
//import java.util.regex.Pattern;
//
///**
// * created by yqq 2020/3/2
// */
//public class JavaStatefulNetworkWordCount {
//
//    private static final Pattern SPACE = Pattern.compile(" ");
//
//    public static void main(String[] args) throws Exception{
//
//        SparkConf sparkconf = new SparkConf().setAppName("JavaStatefulNetworkWordCount").setMaster("local[2]");
//
//        //定义状态更新函数
//        Function3<String, Optional<Integer>,State<Integer>,Tuple2<String, Integer>> updateFunc =
//                (word, one , state) -> {
//                    int sum = one.orElse(0)+(state.exists()?state.get():0);
//                    Tuple2<String , Integer> output = new Tuple2<>(word,sum);
//                    state.update(sum);
//                    return output;
//                };
//
//        //获取流引擎
//        //StreamingContext streamingContext = new StreamingContext(sparkconf, Durations.seconds(2));
//        JavaStreamingContext jssc = new JavaStreamingContext(sparkconf, Durations.seconds(5));
//
//        // Initial state RDD input to mapWithState
//        @SuppressWarnings("unchecked")
//        List<Tuple2<String, Integer>> tuples =
//                Arrays.asList(new Tuple2<>("hello", 1), new Tuple2<>("world", 1));
//        JavaPairRDD<String, Integer> initialRDD = jssc.sparkContext().parallelizePairs(tuples);
//
//
//        // open checkpoint
//        jssc.checkpoint("hdfs://192.168.1.243:8020/test");
//
//        JavaReceiverInputDStream<String> linesDStream = jssc.socketTextStream("192.168.1.248", 9999);
//
//        //JavaDStream<String> words = (JavaDStream<String>)linesDStream.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
//
//        //JavaPairDStream<String, Integer> wordAndOne = (JavaPairDStream<String, Integer>)words.mapToPair(x -> new Tuple2<>(x, 1));
//
//        //拆分单词
//        /*JavaDStream<String> words = linesDStream.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String s) throws Exception {
//
//                final Iterator<String> iterator = Arrays.asList(s.split(" ")).iterator();
//                return iterator;
//            }
//        });*/
//
//
//
//        jssc.start();
//        jssc.awaitTermination();
//
//
//    }
//}
