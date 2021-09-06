package org.spark.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import java.util.ArrayList;
import java.util.List;

/**
 * bulkPut demo
 * created by yqq 2020/5/6
 */
public class JavaHBaseBulkPutExample {

    public static void main(String args[]) {

        SparkConf sc = new SparkConf();
        sc.setAppName("JavaHBaseBulkPutExample").setMaster("local[8]");
        sc.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        JavaSparkContext jsc = new JavaSparkContext(sc);

        List<String> list = new ArrayList<>();


        for (int i = 0; i < 3000000; i++) {
            list.add("python_"+i+",demo"+",content,tom"+i);
            list.add("python_"+i+",demo"+",avg,avg"+i );
        }


        JavaRDD<String> rdd = jsc.parallelize(list);

        Configuration hconf = HBaseConfiguration.create();
        hconf.set("hbase.zookeeper.quorum", "ip239,ip243,ip244");

        JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, hconf);

        hbaseContext.bulkPut(rdd, TableName.valueOf("t_demo"), new PutFunction());



        jsc.stop();

    }

    public static class PutFunction implements Function<String, Put> {

        private static final long serialVersionUID = 1L;

        public Put call(String v) throws Exception {
            String[] cells = v.split(",");
            Put put = new Put(Bytes.toBytes(cells[0]));
            put.addColumn(Bytes.toBytes(cells[1]), Bytes.toBytes(cells[2]),Bytes.toBytes(cells[3]));
            return put;
        }
    }


}
