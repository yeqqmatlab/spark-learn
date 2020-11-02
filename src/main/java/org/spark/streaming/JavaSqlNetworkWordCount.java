//package org.spark.streaming;
//
//
//import java.util.Arrays;
//import java.util.regex.Pattern;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.api.java.StorageLevels;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.scala.spark.streaming.StreamingExamples;
//
//public final class JavaSqlNetworkWordCount {
//    private static final Pattern SPACE = Pattern.compile(" ");
//
//    public static void main(String[] args) throws Exception {
//
//
//        StreamingExamples.setStreamingLogLevels();
//
//        // Create the context with a 1 second batch size
//        SparkConf sparkConf = new SparkConf().setAppName("JavaSqlNetworkWordCount").setMaster("local[2]");
//        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));
//
//        // Create a JavaReceiverInputDStream on target ip:port and count the
//        // words in input stream of \n delimited text (eg. generated by 'nc')
//        // Note that no duplication in storage level only for running locally.
//        // Replication necessary in distributed scenario for fault tolerance.
//        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
//                "192.168.1.248", 9999, StorageLevels.MEMORY_AND_DISK_SER);
//        JavaDStream<String> words = (JavaDStream<String>)lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
//
//        // Convert RDDs of the words DStream to DataFrame and run SQL query
//        words.foreachRDD((rdd, time) -> {
//            SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
//
//            // Convert JavaRDD[String] to JavaRDD[bean class] to DataFrame
//            JavaRDD<JavaRecord> rowRDD = rdd.map(word -> {
//                JavaRecord record = new JavaRecord();
//                record.setWord(word);
//                return record;
//            });
//            Dataset<Row> wordsDataFrame = spark.createDataFrame(rowRDD, JavaRecord.class);
//
//            // Creates a temporary view using the DataFrame
//            wordsDataFrame.createOrReplaceTempView("words");
//
//            // Do word count on table using SQL and print it
//            Dataset<Row> wordCountsDataFrame =
//                    spark.sql("select word, count(*) as total from words group by word");
//            System.out.println("========= " + time + "=========");
//            wordCountsDataFrame.show();
//        });
//
//        ssc.start();
//        ssc.awaitTermination();
//    }
//}
//
///** Lazily instantiated singleton instance of SparkSession */
//class JavaSparkSessionSingleton {
//    private static transient SparkSession instance = null;
//    public static SparkSession getInstance(SparkConf sparkConf) {
//        if (instance == null) {
//            instance = SparkSession
//                    .builder()
//                    .config(sparkConf)
//                    .getOrCreate();
//        }
//        return instance;
//    }
//}
