package org.spark.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by yqq on 2019/6/14.
 */
public class CheckAndPutExample {

    public static void main(String[] args) throws IOException {

        //get conn
        Connection connection = HBaseConnectionFactory.getConnection();
        if (connection == null){
            new Throwable("no getConnection!!!");
        }
        //get table
        Table table = connection.getTable(TableName.valueOf("t2"));

        //create put1
        Put put1 = new Put(Bytes.toBytes("java_row3"));
        put1.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("name"),Bytes.toBytes("spark"));

        boolean flag1 = table.checkAndPut(Bytes.toBytes("java_row3"),Bytes.toBytes("f1"), Bytes.toBytes("name"), null, put1);
        System.out.println(flag1); //ture

        boolean flag2 = table.checkAndPut(Bytes.toBytes("java_row3"),Bytes.toBytes("f1"), Bytes.toBytes("name"), null, put1);
        System.out.println(flag2); //false

        Put put2 = new Put(Bytes.toBytes("java_row3"));
        put1.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("age"),Bytes.toBytes("22"));

        boolean flag3 = table.checkAndPut(Bytes.toBytes("java_row3"),Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("spark"), put2);
        System.out.println(flag3); //true





    }



}
