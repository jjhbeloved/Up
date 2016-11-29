package org.humbird.up.hdfs.cores;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.humbird.up.hdfs.cores.action.IAction;

/**
 * Created by david on 16/9/12.
 */
public class HBaseJoy implements IJoy {

    private final static Logger LOGGER = LogManager.getLogger(HBaseJoy.class);

    @Override
    public void make(IAction action) throws Exception {
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection();
//            HBaseTool.infoClusterStatus(connection.getAdmin().getClusterStatus());
            action.play(connection);
//            HBaseTool.infoTables(connection.getAdmin());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Override
    public Object happy(IAction action) throws Exception {
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection();
//            HBaseTool.infoClusterStatus(connection.getAdmin().getClusterStatus());
            return action.duang(connection);
//            HBaseTool.infoTables(connection.getAdmin());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
