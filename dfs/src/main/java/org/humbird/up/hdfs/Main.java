package org.humbird.up.hdfs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;
import org.humbird.up.hdfs.utils.HATool;
import org.humbird.up.hdfs.utils.LogWatcher;

import java.io.IOException;

/**
 * Created by david on 16/9/8.
 */
public class Main {

    private final static Logger LOGGER = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        String zks = "172.16.50.22:2181,172.16.50.23:2181,172.16.50.24:2181";
        String dir = "/hadoop-ha/nsstargate";
        ZooKeeper zooKeeper;
        try {
            zooKeeper = new ZooKeeper(zks, 10000, new LogWatcher());
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
            return;
        }
        try {
            zooKeeper.getChildren(dir, false).forEach(LOGGER::info);
            LOGGER.info(HATool.getHdfsMasterAddress(zooKeeper, dir).getHostname());
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            HATool.closeZookeeper(zooKeeper);
            return;
        }
        HATool.closeZookeeper(zooKeeper);
    }
}
