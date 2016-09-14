package org.humbird.up.hdfs.commons;

import org.apache.hadoop.conf.Configuration;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistry;

import java.util.Properties;

/**
 * Created by david on 16/9/8.
 */
public class Constant {

    public final static Configuration hdfcConfiguration = new Configuration();
    public final static Properties properties = new Properties();
    public static StandardServiceRegistry hbRegistry = null;
    public static SessionFactory hbFactory = null;


    public final static String[] DEFAULT_HADOOP_CONFIG = {"hdfs-site.xml", "core-site.xml", "mapred-site.xml", "yarn-site.xml", "hbase-site.xml"};
    public final static String DEFAULT_UP_NAME = "/up.properties";
    public final static String DEFAULT_UP_HIBERNATE = "hibernate.cfg.xml";

    /**
     * hadoop commons
     */
    public final static String HADOOP_USER_NAME = "HADOOP_USER_NAME";
    public final static String HDFS_FILE_TYPE = "HDFS_FILE_TYPE";   // 文件类型
    public final static String HDFS_DIR_TYPE = "HDFS_DIR_TYPE"; // 目录类型

    /**
     * hdfs-site.xml
     */
    public final static String HDFS_NAMESERVICES = "dfs.nameservices";
    public final static String HDFS_ZOOKEEPER_QUORUM = "ha.zookeeper.quorum";
    public final static String HDFS_ZOOKEEPER_PARENT = "ha.zookeeper.parent-znode";
    public final static String HDFS_ACTIVE_LOCK = "ActiveStandbyElectorLock";

    /**
     * core-site.xml
     */
    public final static String CORE_FS = "fs.defaultFS";

    /**
     * up.properties
     */
    public final static String UP_HDFS_SUPER_USER = "hdfs.super.user";
    public final static String UP_HDFS_HUMBIRD_USER = "hdfs.humbird.user";
    public final static String UP_RDB_STATUS = "rdb.status";

}
