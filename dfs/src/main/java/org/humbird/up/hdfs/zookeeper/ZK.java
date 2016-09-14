package org.humbird.up.hdfs.zookeeper;

import org.apache.commons.httpclient.URI;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.hadoop.hdfs.server.namenode.ha.proto.HAZKInfoProtos;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.humbird.up.hdfs.commons.Constant;

import java.io.File;

import static org.humbird.up.hdfs.commons.Constant.hdfcConfiguration;

/**
 * Created by david on 16/9/8.
 */
public class ZK {

    private final static Logger LOGGER = LogManager.getLogger(ZK.class);

    private static ZK zk = null;

    private CuratorFramework client = null;

    private HAZKInfoProtos.ActiveNodeInfo activeNodeInfo = null;

    public ZK() {
    }

    public ZK(CuratorFramework curatorFramework) {
        this.client = curatorFramework;
        this.client.start();
    }

    // static factory method
    public static synchronized ZK instance() {
        if (zk == null) {
            synchronized (ZK.class) {
                CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(
                        hdfcConfiguration.get(Constant.HDFS_ZOOKEEPER_QUORUM),
                        30000,
                        30000,
                        new ExponentialBackoffRetry(1000, Integer.MAX_VALUE)
                );
                zk = new ZK(curatorFramework);
            }
        }
        return zk;
    }

    public HAZKInfoProtos.ActiveNodeInfo getHDFSActiveNode() throws Exception {
        return getHDFSActiveNode(false);
    }

    /**
     *
     * @param force 出错的时候选择为 true 可以强制刷新 active
     * @return
     * @throws Exception
     */
    public HAZKInfoProtos.ActiveNodeInfo getHDFSActiveNode(boolean force) throws Exception {
        if (activeNodeInfo == null || force) {
            URI uri = new URI(hdfcConfiguration.get(Constant.CORE_FS), false);
            if (uri.getPort() == -1) {
                StringBuilder dir = new StringBuilder()
                        .append(hdfcConfiguration.get(Constant.HDFS_ZOOKEEPER_PARENT))
                        .append(File.separator)
                        .append(hdfcConfiguration.get(Constant.HDFS_NAMESERVICES))
                        .append(File.separator)
                        .append(Constant.HDFS_ACTIVE_LOCK);
                this.activeNodeInfo = HAZKInfoProtos.ActiveNodeInfo.parseFrom(client.getData().forPath(dir.toString()));
            } else {
                this.activeNodeInfo = HAZKInfoProtos.ActiveNodeInfo.newBuilder()
                        .setHostname(uri.getHost())
                        .setPort(uri.getPort())
                        .setNameserviceId(uri.getPath())
                        .setNamenodeId(uri.getScheme())
                        .setZkfcPort(-1)
                        .build();
            }
        }
        return this.activeNodeInfo;
    }

    public void close() {
        CloseableUtils.closeQuietly(client);
    }
}
