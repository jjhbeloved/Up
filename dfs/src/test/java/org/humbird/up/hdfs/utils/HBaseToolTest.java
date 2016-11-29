package org.humbird.up.hdfs.utils;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.humbird.up.hdfs.BaseTest;
import org.junit.Test;

/**
 * Created by david on 16/10/11.
 */
public class HBaseToolTest extends BaseTest {
    @Test
    public void infoClusterStatus() throws Exception {

    }

    @Test
    public void infoNamespaces() throws Exception {
        Connection connection = ConnectionFactory.createConnection();
        HBaseTool.infoNamespaces(connection.getAdmin());
    }

    @Test
    public void infoNamespace() throws Exception {
        Connection connection = ConnectionFactory.createConnection();
        HBaseTool.infoNamespace(connection.getAdmin(), "hbase");
    }

    @Test
    public void infoNamespaceTables() throws Exception {
        Connection connection = ConnectionFactory.createConnection();
        HBaseTool.infoNamespaceTables(connection.getAdmin(), "default");
    }

    @Test
    public void infoTables() throws Exception {

    }

}