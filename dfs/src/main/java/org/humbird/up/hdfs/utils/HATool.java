package org.humbird.up.hdfs.utils;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hdfs.server.namenode.ha.proto.HAZKInfoProtos;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Created by david on 16/9/8.
 */
public class HATool {

    private final static Logger LOGGER = LogManager.getLogger(HATool.class);

    /**
     *
     * @param zooKeeper
     * @param dir
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static HAZKInfoProtos.ActiveNodeInfo getHdfsMasterAddress(ZooKeeper zooKeeper, String dir) throws KeeperException, InterruptedException, InvalidProtocolBufferException {
        return SplitHAActive(getDataWithZK(zooKeeper, dir + "/ActiveStandbyElectorLock", new Stat()));
    }


    /**
     *
     * @param zooKeeper
     * @param dir
     * @param stat
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static HAZKInfoProtos.ActiveNodeInfo getHdfsMasterAddress(ZooKeeper zooKeeper, String dir, Stat stat) throws KeeperException, InterruptedException, InvalidProtocolBufferException {
        return SplitHAActive(getDataWithZK(zooKeeper, dir + "/ActiveStandbyElectorLock", stat));
    }

    /**
     *
     * @param zooKeeper
     * @param dir
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static byte[] getDataWithZK(ZooKeeper zooKeeper, String dir) throws KeeperException, InterruptedException {
        return zooKeeper.getData(dir, false, new Stat());
    }

    /**
     *
     * @param zooKeeper
     * @param dir
     * @param stat
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static byte[] getDataWithZK(ZooKeeper zooKeeper, String dir, Stat stat) throws KeeperException, InterruptedException {
        return zooKeeper.getData(dir, false, stat);
    }

    public static void closeZookeeper(ZooKeeper zooKeeper) {
        try {
            zooKeeper.close();
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
        }
    }

    /**
     *
     * @param data
     * @return
     */
    private static HAZKInfoProtos.ActiveNodeInfo SplitHAActive(byte[] data) throws InvalidProtocolBufferException {
        return HAZKInfoProtos.ActiveNodeInfo.parseFrom(data);
    }

}
