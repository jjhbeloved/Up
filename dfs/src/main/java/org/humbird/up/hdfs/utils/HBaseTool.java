package org.humbird.up.hdfs.utils;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * Created by david on 16/9/12.
 */
public class HBaseTool {

    private final static Logger LOGGER = LogManager.getLogger(HBaseTool.class);

    public static void infoClusterStatus(ClusterStatus clusterStatus) {
        LOGGER.info("Master URL: " + clusterStatus.getMaster().getHostAndPort() + " -- ServerName: " + clusterStatus.getMaster().getServerName());
        clusterStatus.getBackupMasters().forEach(n -> {
            LOGGER.info("Backup URL: " + n.getHostAndPort() + " -- ServerName: " + n.getServerName());
        });
        LOGGER.info("BalanceOn: " + clusterStatus.getBalancerOn());
        LOGGER.info("RegionsCount: " + clusterStatus.getRegionsCount());
        clusterStatus.getServers().forEach(n -> {
            LOGGER.info("Server URL: " + n.getHostAndPort() + " -- ServerName: " + n.getServerName());
        });
    }

    public static void infoTables(Admin admin) throws IOException {
        TableName []tableNames = admin.listTableNames();
        for (int i = 0; i < tableNames.length; i++) {
            LOGGER.info(tableNames[i].getNameAsString());
        }
    }

}
