package org.spark.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by yqq on 2019/6/17.
 */
public class DeleteExample {

    public static void main(String[] args) throws IOException {
        //get conn
        Connection connection = HBaseConnectionFactory.getConnection();
        if (connection == null){
            new Throwable("no getConnection!!!");
        }
        //get table
        Table table = connection.getTable(TableName.valueOf("t2"));


        Delete delete = new Delete(Bytes.toBytes("java_row1"));

        table.delete(delete);


        table.close();
        connection.close();
    }
}
