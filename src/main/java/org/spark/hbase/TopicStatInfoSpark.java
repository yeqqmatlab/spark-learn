package org.spark.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.*;

/**
 * 教师端topic使用次数统计：
 *  1、本校使用次数
 *  2、教师使用次数
 *  3、总使用次数&难度系数
 *  4、详情列表
 * created by yqq 2020/4/16
 */
public class TopicStatInfoSpark {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("TopicStatInfoSpark")
                .master("local[2]")
                .config("spark.sql.parquet.writeLegacyFormat", "true")
                .getOrCreate();

        /**
         * 获取学生考题得分表
         */
        Dataset<Row> topicScoreDF = spark.read().parquet("hdfs://192.168.1.243:8020/zsy/warehouse/dws/student_method_score");
        topicScoreDF.createOrReplaceTempView("topic_score");

        Dataset<Row> topicTeacherDF = spark.sql(" SELECT topic_id,school_id,teacher_id,count(DISTINCT exam_group_id) as teacher_count FROM topic_score where mode = 0 and teacher_id is not null and teacher_id != '0' group by topic_id,school_id,teacher_id ");
        topicTeacherDF.persist();
        topicTeacherDF.foreachPartition(part -> {
            Connection connection = HBaseUtils.getConnection();
            Table topicInfoTable = connection.getTable(TableName.valueOf("topic_info"));
            while (part.hasNext()){
                Row next = part.next();
                long topic_id = next.getLong(0);
                int schoolId = next.getInt(1);
                String teacherId = next.getString(2);
                long count = next.getLong(3);
                String key = topic_id+"_"+schoolId+"_"+teacherId;
                Put put = new Put(Bytes.toBytes(key));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("teacher_count"), Bytes.toBytes(String.valueOf(count)));
                topicInfoTable.put(put);
            }
            topicInfoTable.close();
            connection.close();
        });

        spark.stop();
    }
}
