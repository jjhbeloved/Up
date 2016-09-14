package org.humbird.up.hdfs.cores;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.humbird.up.hdfs.cores.action.IAction;
import org.humbird.up.hdfs.utils.HBaseTool;

/**
 * Created by david on 16/9/12.
 */
public class HBaseJoy implements IJoy {

    private final static Logger LOGGER = LogManager.getLogger(HBaseJoy.class);

    @Override
    public void make(IAction action) throws Exception {
        Connection connection = ConnectionFactory.createConnection();
        HBaseTool.infoClusterStatus(connection.getAdmin().getClusterStatus());
        action.Play(connection);
        HBaseTool.infoTables(connection.getAdmin());
        connection.close();
    }
}
