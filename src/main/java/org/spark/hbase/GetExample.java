package org.spark.hbase;

import javafx.scene.text.TextAlignment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

/**
 * Created by yqq on 2019/6/14.
 */
public class GetExample {

    public static void main(String[] args) throws IOException {


        //get conn
        Connection connection = HBaseConnectionFactory.getConnection();
        if (connection == null){
            new Throwable("no getConnection!!!");
        }
        //get table
        Table table = connection.getTable(TableName.valueOf("student_method_rate"));
        byte[] rowkey = Bytes.toBytes("1452_17536408539789254473_17535877358481719982_691");
        byte[] f1 = Bytes.toBytes("info");
        byte[] rate = Bytes.toBytes("rate");

        long start = System.currentTimeMillis();

        Get get1 = new Get(rowkey);

        get1.addColumn(f1, rate);
        boolean exists = table.exists(get1);
        System.out.println(exists);
        Result result = table.get(get1);
        byte[] value = result.getValue(f1, rate);

        /*for (byte b : value) {
            System.out.println("-->"+b);
        }*/

        System.out.println("value--->"+Bytes.toString(value));

        System.out.println((System.currentTimeMillis() - start)/1000);

        table.close();
        connection.close();


    }

}
