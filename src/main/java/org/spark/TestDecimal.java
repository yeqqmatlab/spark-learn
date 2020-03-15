package org.spark;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * created by yqq 2020/3/12
 */
public class TestDecimal {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("TestDecimal")
                .master("local[2]")
                .getOrCreate();

        Dataset<Row> dataset = sparkSession.read().option("delimiter", ",").csv("G:\\txt");
        Dataset<Row> union1 = dataset.union(dataset);
        Dataset<Row> union2 = union1.union(union1);

        JavaRDD<Row> javaRDD = union2.toJavaRDD().map(row -> {
            String f0 = row.getString(0);
            String f1 = row.getString(1);
            String f2 = row.getString(2);
            String f3 = row.getString(3);
            String f4 = row.getString(4);
            String f5 = row.getString(5);
            String f6 = row.getString(6);
            String f7 = row.getString(7);
            BigDecimal bd = new BigDecimal(f7);
            return RowFactory.create(f0, f1, f2, f3, f4, f5, f6, bd);
        });

        Dataset<Row> dataFrame = sparkSession.createDataFrame(javaRDD, getSchema());

        //writeDataToMysql(dataFrame,"test_decimal");

        dataFrame.repartition(1).write().mode(SaveMode.Overwrite).parquet("hdfs://192.168.1.243:8020/test/test_decimal");

        sparkSession.stop();
    }

    private static void writeDataToMysql(Dataset<Row> dataFrame,String tableName) {

        Config load = ConfigFactory.load();
        String url = load.getString("jdbc.url");
        String user = load.getString("jdbc.user");
        String password = load.getString("jdbc.password");
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("user", user);// 设置用户名
        connectionProperties.setProperty("password", password);// 设置密码

        try {
            dataFrame
                    .write()
                    .mode(SaveMode.Overwrite)
                    .option("truncate","true") //不删除表结构
                    .option("batchsize",10000) //批插入 默认1000
                    .jdbc(url,tableName,connectionProperties);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private static StructType getSchema(){

        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("school_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("sptr_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("student_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("paper_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("topic_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("type", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("is_right", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("handler_index", DataTypes.createDecimalType(20,1), true));

        return DataTypes.createStructType(structFields);
    }
}
