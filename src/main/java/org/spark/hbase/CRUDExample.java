package org.spark.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;

/**
 * Created by yqq on 2019/6/16.
 */
public class CRUDExample {

    public static void main(String[] args) throws IOException {
        //get conn
        Connection connection = HBaseConnectionFactory.getConnection();
        if (connection == null){
            new Throwable("no getConnection!!!");
        }
        // get table
        Table table = connection.getTable(TableName.valueOf("t2"));

        Put put = new Put(Bytes.toBytes("row1"));
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("qual1"),
                Bytes.toBytes("val1"));
        put.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("qual2"),
                Bytes.toBytes("val2"));
        table.put(put);

        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result2 : scanner) {
            while (result2.advance())
                System.out.println("Cell: " + result2.current());
        }

        Get get = new Get(Bytes.toBytes("row1"));
        get.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("qual1"));
        Result result = table.get(get);
        System.out.println("Get result: " + result);
        byte[] val = result.getValue(Bytes.toBytes("f1"),
                Bytes.toBytes("qual1"));
        System.out.println("Value only: " + Bytes.toString(val));

        Delete delete = new Delete(Bytes.toBytes("row1"));
        delete.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("qual1"));
        table.delete(delete);

        Scan scan2 = new Scan();
        ResultScanner scanner2 = table.getScanner(scan2);
        for (Result result2 : scanner2) {
            System.out.println("Scan: " + result2);
        }

        table.close();
        connection.close();
    }


}
