package org.spark.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;


/**
 * 教师端topic使用次数统计：
 *  1、本校使用次数
 *  2、教师使用次数
 *  3、总使用次数&难度系数
 *  4、详情列表
 * created by yqq 2020/4/16
 */
public class MethodRateCDHSpark {

    public static void main(String[] args) {


        SparkSession spark = SparkSession
                .builder()
                .appName("MethodRateCDHSpark")
                .master("local[2]")
                .config("spark.sql.parquet.writeLegacyFormat", "true")
                .getOrCreate();

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        Configuration hconf = HBaseConfiguration.create();
        hconf.set("hbase.zookeeper.quorum", "ip239,ip243,ip244");

        JavaHBaseContext hBaseContext = new JavaHBaseContext(jsc, hconf);

        /**
         * 获取学生考题得分表
         */
        Dataset<Row> topicScoreDF = spark.read().parquet("hdfs://192.168.1.243:8020/tmp/student_method_rate");
        topicScoreDF.createOrReplaceTempView("method_rate");

        Dataset<Row> resDF = spark.sql(" select concat_ws(\"_\", school_id, semester_id, student_id, method_id) as row_key, school_id, semester_id, student_id,method_id,rate from method_rate where school_id is not null and semester_id is not null and student_id is not null and method_id is not null");
        //resDF.printSchema();
        System.out.println("resDF--->"+resDF.count());

        /**
         * 批量写入
         */
        hBaseContext.bulkPut(resDF.toJavaRDD(),TableName.valueOf("student_method_rate"), new PutFunction());

        jsc.stop();
        spark.stop();
    }

    public static class PutFunction implements Function<Row, Put> {
        private static final long serialVersionUID = 1L;
        public Put call(Row r) throws Exception {
            Put put = new Put(Bytes.toBytes(r.getString(0)));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("school_id"), Bytes.toBytes(String.valueOf(r.getInt(1))));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("semester_id"), Bytes.toBytes(String.valueOf(r.getString(2))));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes(String.valueOf(r.getString(3))));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("method_id"), Bytes.toBytes(String.valueOf(r.getInt(4))));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rate"), Bytes.toBytes(String.valueOf(r.getDecimal(5))));
            return put;
        }
    }

    public static class GetFunction implements Function<Row, Get>{
        private static final long serialVersionUID = -336345323776387048L;

        public Get call(Row r) throws Exception {
            return new Get(Bytes.toBytes(r.getString(0)));
        }
    }

    public static class ResultFunction implements Function<Result, Tuple2<String,String>> {

        private static final long serialVersionUID = 1L;

        public Tuple2<String,String> call(Result result) throws Exception {

            //byte[] value = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("teacher_count"));

            List<Cell> cells = result.listCells();
            String key = Bytes.toString(CellUtil.cloneRow(cells.get(0)));
            String val = Bytes.toString(CellUtil.cloneValue(cells.get(0)));
            //System.out.println(key);
            //System.out.println(val);
            /*List<String> list = new ArrayList();
            for (Cell cell : cells) {
                //System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
                list.add(Bytes.toString(CellUtil.cloneValue(cell)));
            }*/
            //return Bytes.toString(value);
            return new Tuple2(key,val);
        }
    }
}
