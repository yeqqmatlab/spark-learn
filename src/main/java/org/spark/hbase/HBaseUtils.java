package org.spark.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

/**
 * hbase config
 * created by yqq 2020/5/6
 */
public class HBaseUtils {



    /**
     * conn hbase
     * @return
     */
    public static Connection getConnection(){
        Connection connection = null;
        Configuration HBaseConf = HBaseConfiguration.create();
        try {
            HBaseConf.set(HConstants.ZOOKEEPER_QUORUM, "ip239,ip243,ip244");
            HBaseConf.set(HConstants.CLIENT_ZOOKEEPER_CLIENT_PORT, "2181");
            connection = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(HBaseConf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }

}
