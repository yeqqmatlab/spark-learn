package org.spark.algo.chapter08;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import java.util.*;

/**
 * description: 寻找公共的朋友
 * Created by yqq
 * 2022-07-18
 */
public class ChapterCommonFriend {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder().appName("ChapterCommonFriend").master("local[2]").getOrCreate();

        Dataset<String> lineDF = sparkSession.read().textFile("E:\\workspace\\spark-learn\\src\\main\\java\\org\\spark\\algo\\chapter08\\chapter8_kv.txt");

        List<Row> list = lineDF.toJavaRDD().map(r -> {
            String[] split = r.split(",");
            String person = split[0];
            String friends = split[1];
            String[] friendArr = friends.split("[ ]");
            Set set = new HashSet<>();
            Collections.addAll(set, friendArr);
            return RowFactory.create(person, set);
        }).collect();

        List resList = new ArrayList<String>();
        for (int i = 0; i < list.size() -1 ; i++) {
            for (int j = i+1; j < list.size(); j++) {

                Row rowA = list.get(i);
                Row rowB = list.get(j);
                String key = rowA.getString(0) + "-" + rowB.getString(0);
                HashSet<String> setA = (HashSet<String>)rowA.get(1);
                HashSet<String> setB = (HashSet<String>)rowB.get(1);

                setA.retainAll(setB);
                String setStr = setA.toString();
                String s = key + ":" + setStr;
                resList.add(s);
            }
        }

        System.out.println("resList = " + resList);

        sparkSession.stop();

    }

}
