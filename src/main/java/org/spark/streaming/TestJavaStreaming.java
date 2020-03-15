//package org.spark.streaming;
//
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.apache.spark.SparkConf;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaInputDStream;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka010.ConsumerStrategies;
//import org.apache.spark.streaming.kafka010.KafkaUtils;
//import org.apache.spark.streaming.kafka010.LocationStrategies;
//import scala.Tuple2;
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.Map;
//
//public class TestJavaStreaming {
//
//    public static void main(String[] args) throws InterruptedException {
//
//        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
//        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
//
//        Map<String, Object> kafkaParams = new HashMap<>();
//        kafkaParams.put("bootstrap.servers", "localhost:9092");
//        kafkaParams.put("key.deserializer", StringDeserializer.class);
//        kafkaParams.put("value.deserializer", StringDeserializer.class);
//        kafkaParams.put("group.id", "test-group-01");
//        kafkaParams.put("auto.offset.reset", "earliest");
//        kafkaParams.put("enable.auto.commit", true);
//
//        Collection<String> topics = Arrays.asList("test2020");
//
//        JavaInputDStream<ConsumerRecord<String, String>> stream =
//                KafkaUtils.createDirectStream(
//                        jssc,
//                        LocationStrategies.PreferConsistent(),
//                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
//                );
//
//        JavaPairDStream<String, String> javaPairDStream = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
//
//        JavaDStream<String> map = javaPairDStream.map(x -> x._2);
//
//        map.dstream().saveAsTextFiles("/Users/yqq/logs/","csv");
//
//
//        jssc.start();              // Start the computation
//        jssc.awaitTermination();   // Wait for the computation to terminate
//
//    }
//}
