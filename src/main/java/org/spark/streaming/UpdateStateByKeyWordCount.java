//package org.spark.streaming;
//
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.Optional;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaPairInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka.KafkaUtils;
//
//import kafka.serializer.StringDecoder;
//import scala.Tuple2;
//
///**
// * use updateStateByKey implement global wordcount
// *
// */
//public class UpdateStateByKeyWordCount {
//
//    public static void main(String[] args) throws Exception {
//
//        SparkConf conf = new SparkConf().setAppName("global wordcount");
//
//        // create context
//        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
//
//        // open checkpoint mechanism
//        // args[0] is hdfs directory
//        jssc.checkpoint(args[0]);
//
//        // properties map
//        Map<String, String> KafkaParams = new HashMap<String, String>();
//        KafkaParams.put("bootstrap.servers", "hserver-1:9092,hserver-2:9092,hserver-3:9092");
//        KafkaParams.put("gropu.id", "global_word_count");
//        KafkaParams.put("auto.offset.reset", "latest");
//
//        // topic set
//        // args[1] is topic --> topic1,topic2,topic3
//        Set<String> topics = new HashSet<String>();
//        String[] topicsArray = args[1].split(",");
//        for(String topic: topicsArray){
//            topics.add(topic);
//        }
//
//        // create kafka data flow
//        JavaPairInputDStream<String, String> KafkaInputDStream =
//                KafkaUtils.createDirectStream(
//                        jssc,
//                        String.class,
//                        String.class,
//                        StringDecoder.class,
//                        StringDecoder.class,
//                        KafkaParams,
//                        topics
//                );
//
//        // flatMap
//        JavaDStream<String> WordDstream =
//                KafkaInputDStream.flatMap(
//
//                        new FlatMapFunction<Tuple2<String,String>, String>() {
//
//                            private static final long serialVersionUID = -7468479466334529905L;
//
//                            @Override
//                            public Iterator<String> call(Tuple2<String, String> tuple) throws Exception {
//
//                                return Arrays.asList(tuple._2.split(" ")).iterator();
//                            }
//
//                        });
//
//        // mapToPair --> (word, 1)
//        JavaPairDStream<String, Integer> WordPairDStream =
//                WordDstream.mapToPair(
//
//                        new PairFunction<String, String, Integer>() {
//
//                            private static final long serialVersionUID = 1L;
//
//                            @Override
//                            public Tuple2<String, Integer> call(String word) throws Exception {
//
//                                return new Tuple2<String, Integer>(word, 1);
//                            }
//                        });
//
//        // updateStateByKey
//        JavaPairDStream<String, Integer> UpdateDStream =
//                WordPairDStream.updateStateByKey(
//
//                        new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
//
//                            private static final long serialVersionUID = 1359222521577679802L;
//
//                            @Override
//                            public Optional<Integer> call(List<Integer> values, Optional<Integer> currentValue) throws Exception {
//
//                                Integer newValue = 0;
//
//                                if(currentValue.isPresent()){
//                                    newValue = currentValue.get();
//                                }
//
//                                for(Integer value: values){
//                                    newValue += value;
//                                }
//
//                                return Optional.of(newValue);
//                            }
//
//                        });
//
//        // sort
//        JavaPairDStream<String, Integer> resultDStream =
//                UpdateDStream.transformToPair(
//
//                        new Function<JavaPairRDD<String,Integer>, JavaPairRDD<String,Integer>>() {
//
//                            private static final long serialVersionUID = -4037579884645976648L;
//
//                            @Override
//                            public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> inputPairRDD) throws Exception {
//
//                                // swap key - value
//                                JavaPairRDD<Integer, String> reseverPairRDD = inputPairRDD.mapToPair(
//
//                                        new PairFunction<Tuple2<String,Integer>, Integer, String>() {
//
//                                            private static final long serialVersionUID = 8784589314191257879L;
//
//                                            @Override
//                                            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
//
//                                                return new Tuple2<Integer, String>(tuple._2, tuple._1);
//                                            }
//                                        });
//
//                                // sortByKey
//                                JavaPairRDD<Integer, String> sortPairRDD =
//                                        reseverPairRDD.sortByKey(false);
//
//                                // swap key - value
//                                JavaPairRDD<String, Integer> resultPairRDD =
//                                        sortPairRDD.mapToPair(
//
//                                                new PairFunction<Tuple2<Integer,String>, String, Integer>() {
//
//                                                    private static final long serialVersionUID = 8784589314191257879L;
//
//                                                    @Override
//                                                    public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
//
//                                                        return new Tuple2<String, Integer>(tuple._2, tuple._1);
//                                                    }
//                                                });
//
//                                return resultPairRDD;
//                            }
//                        });
//
//        // print result
//        resultDStream.print();
//
//        jssc.start();
//
//        jssc.awaitTermination();
//
//        jssc.close();
//    }
//}
