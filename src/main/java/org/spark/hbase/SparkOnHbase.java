package org.spark.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;


/**
 * created by yqq 2020/5/6
 */
public class SparkOnHbase {

    public static void main(String[] args) {
        SparkConf sc = new SparkConf().setAppName("spark on hbase").setMaster("local[2]");
        sc.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        Configuration hconf = HBaseConfiguration.create();
        hconf.set("hbase.zookeeper.quorum", "ip239,ip243,ip244");
        JavaSparkContext spark = new JavaSparkContext(sc);
        JavaHBaseContext jbc = new JavaHBaseContext(spark, hconf);
        Scan scan = new Scan();
        //  scan.setStartRow(Bytes.toBytes("1000"));
        //  scan.setStopRow(Bytes.toBytes("10000"));
        JavaRDD<Tuple2<ImmutableBytesWritable, Result>> pairRDD = jbc.hbaseRDD(TableName.valueOf("person"), scan);
        List<Tuple2<ImmutableBytesWritable, Result>> t = pairRDD.take(100);
        for (Tuple2 tuple2 : t) {
            ImmutableBytesWritable row = (ImmutableBytesWritable) tuple2._1;
            System.out.println("--->"+Bytes.toString(row.get()));
            Result result = (Result) tuple2._2();
            List<Cell> cells = result.listCells();
            System.out.println(Bytes.toString(CellUtil.cloneRow(cells.get(0))));
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }
}
