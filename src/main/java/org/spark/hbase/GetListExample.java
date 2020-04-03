package org.spark.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yqq on 2019/6/14.
 */
public class GetListExample {

    public static void main(String[] args) throws IOException {
        //get conn
        Connection connection = HBaseConnectionFactory.getConnection();
        if (connection == null){
            new Throwable("no getConnection!!!");
        }
        //get table
        Table table = connection.getTable(TableName.valueOf("person"));

        //
        byte[] f1 = Bytes.toBytes("info");
        byte[] name = Bytes.toBytes("name");
        byte[] age = Bytes.toBytes("age");
        byte[] row1 = Bytes.toBytes("1");
        byte[] row2 = Bytes.toBytes("2");

        List<Get> gets = new ArrayList<>();

        Get get1 = new Get(row1);
        get1.addColumn(f1,name);
        gets.add(get1);

        Get get11 = new Get(row1);
        get11.addColumn(f1,age);
        gets.add(get11);

        Get get2 = new Get(row2);
        get2.addColumn(f1,name);
        gets.add(get2);

        Get get3 = new Get(row2);
        get3.addColumn(f1,age);
        gets.add(get3);

        Result[] results = table.get(gets);

        System.out.println("1--------------------------------------------------------------> ");

        /*for (Result result : results) {
            String row = Bytes.toString(result.getRow());
            System.out.println("row--->"+row);
            if(result.containsColumn(f1,name)){
                byte[] value = result.getValue(f1, name);
                System.out.println("name--->"+Bytes.toString(value));
            }
            if(result.containsColumn(f1,age)){
                byte[] value = result.getValue(f1, age);
                System.out.println("age--->"+Bytes.toString(value));
            }

        }*/

        System.out.println("2------------------------------------------------------------>");

        for (Result result : results) {
            for (Cell cell : result.listCells()) {
                System.out.println("Row: " + Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
                System.out.println("Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        System.out.println("3-------------------------------------------------------------->");
        /*for (Result result : results) {
            System.out.println(result);
        }*/

        table.close();
        connection.close();


    }
}
