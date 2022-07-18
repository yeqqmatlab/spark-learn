package org.spark.algo.chapter08;

import java.util.HashSet;

/**
 * description:
 * Created by yqq
 * 2022-07-18
 */
public class DemoSet {
    public static void main(String[] args) {



        HashSet<String> setA = new HashSet<>();
        setA.add("a");
        setA.add("b");
        HashSet<String> setB = new HashSet<>();
        setB.add("b");
        setB.add("c");
        HashSet<String> resSet = new HashSet<>();
        resSet.addAll(setA);
        resSet.retainAll(setB);

        System.out.println("resSet = " + resSet);

    }
}
