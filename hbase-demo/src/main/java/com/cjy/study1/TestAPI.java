package com.cjy.study1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class TestAPI {
    private static Connection connection = null;
    private static Admin admin = null;

    static
    {
        try
        {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "hadoop202,hadoop203,hadoop204");

            connection = ConnectionFactory.createConnection(configuration);

            admin = connection.getAdmin();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
    public static boolean isTableExist(String tableName)
            throws IOException
    {
        boolean exists = admin.tableExists(TableName.valueOf(tableName));

        return exists;
    }
    public static void main(String[] args) throws IOException {
        System.out.println(admin);
        System.out.println(connection);
        System.out.println(isTableExist("stu"));
    }
}
