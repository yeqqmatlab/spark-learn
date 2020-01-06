package org.spark.chap01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.spark.util.DataStructures;
import scala.Tuple2;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * created by yqq 2020-01-02
 */
public class SecondarySortUsingCombineByKey {
    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("SecondarySortUsingCombineByKey").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> linesRDD = sc.textFile("E:\\github\\spark-learn\\src\\main\\java\\org\\spark\\chap01\\sample_input.txt",1);

        JavaPairRDD<String, Tuple2<Integer, Integer>> javaPairRDD = linesRDD.mapToPair((String s) -> {
            String[] split = s.split(",");
            System.out.println(split[0] + "-->" + split[1] + " " + split[2]);
            Tuple2<Integer, Integer> timeValue = new Tuple2<>(Integer.parseInt(split[1]), Integer.parseInt(split[2]));
            return new Tuple2<String, Tuple2<Integer, Integer>>(split[0], timeValue);
        });

        List<Tuple2<String, Tuple2<Integer, Integer>>> collect = javaPairRDD.collect();

        for (Tuple2<String, Tuple2<Integer, Integer>> vo : collect) {
            Tuple2<Integer, Integer> timeValue = vo._2;
            System.out.println(vo._1 +"--->"+timeValue._1+"  "+timeValue._2);
        }

        //java8 定义spark一元函数
        Function<Tuple2<Integer, Integer>, SortedMap<Integer, Integer>> createCombiner = (Tuple2<Integer, Integer> x) -> {
            Integer time = x._1;
            Integer value = x._2;
            SortedMap<Integer, Integer> map = new TreeMap<>();
            map.put(time, value);
            return map;
        };

        //java8 定义spark二元函数
        Function2<SortedMap<Integer, Integer>, Tuple2<Integer, Integer>, SortedMap<Integer, Integer>> mergeValue = (SortedMap<Integer, Integer> map, Tuple2<Integer, Integer> x) -> {
            Integer time = x._1;
            Integer value = x._2;
            map.put(time, value);
            return map;
        };

        // function 3: merge two combiner data structures
        Function2<SortedMap<Integer, Integer>, SortedMap<Integer, Integer>, SortedMap<Integer, Integer>> mergeCombiners
                = (SortedMap<Integer, Integer> map1, SortedMap<Integer, Integer> map2) -> {
            if (map1.size() < map2.size()) {
                return DataStructures.merge(map1, map2);
            } else {
                return DataStructures.merge(map1, map2);
            }
        };

        // STEP-5: create sorted (time, value)
        JavaPairRDD<String, SortedMap<Integer, Integer>> combined = javaPairRDD.combineByKey(
                createCombiner,
                mergeValue,
                mergeCombiners);

        // STEP-7: validate STEP-6, we collect all values from JavaPairRDD<> and print it.
        System.out.println("===  DEBUG STEP-6 ===");
        List<Tuple2<String, SortedMap<Integer, Integer>>> output2 = combined.collect();
        for (Tuple2<String, SortedMap<Integer, Integer>> t : output2) {
            String name = t._1;
            SortedMap<Integer, Integer> map = t._2;
            System.out.println(name);
            System.out.println(map);
        }


        sc.stop();

    }
}
