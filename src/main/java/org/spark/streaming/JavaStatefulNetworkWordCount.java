package org.spark.streaming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.scala.spark.streaming.StreamingExamples;
import scala.Tuple2;

import java.util.*;

/**
 * created by yqq 2020/3/2
 */
public class JavaStatefulNetworkWordCount {


    public static void main(String[] args) throws Exception{

        SparkConf sparkconf = new SparkConf().setAppName("JavaStatefulNetworkWordCount").setMaster("local[2]");

        //Logger.getLogger(JavaStatefulNetworkWordCount.class).setLevel(Level.OFF);
        //StreamingExamples.setStreamingLogLevels();
        Logger.getRootLogger().setLevel(Level.WARN);


        //定义状态更新函数
        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunc = new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

            private static final long serialVersionUID = 1359222521577679802L;

            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> currentValue) throws Exception {

                Integer newValue = 0;

                if (currentValue.isPresent()) {
                    newValue = currentValue.get();
                }

                for (Integer value : values) {
                    newValue += value;
                }
                return Optional.of(newValue);
            }
        };

        //获取流引擎
        JavaStreamingContext jssc = new JavaStreamingContext(sparkconf, Durations.seconds(3));

        // open checkpoint 容错
        jssc.checkpoint("hdfs://192.168.1.243:8020/test");

        //JavaReceiverInputDStream<String> linesDStream = jssc.socketTextStream("192.168.1.248", 9999);

        String topics = "topic_03";
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "ip239:9092,ip247:9092,ip248:9092");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-01");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map(ConsumerRecord::value);

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        JavaPairDStream<String, Integer> wordsAndOne = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairDStream<String, Integer> wordsCount = wordsAndOne.updateStateByKey(updateFunc);

        wordsCount.print();
        //开启实时计算
        jssc.start();
        //等待应用停止
        jssc.awaitTermination();
    }
}
