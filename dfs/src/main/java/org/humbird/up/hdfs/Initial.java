package org.humbird.up.hdfs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.humbird.up.hdfs.zookeeper.ZK;

import java.io.InputStream;
import java.util.Arrays;

import static org.humbird.up.hdfs.commons.Constant.*;

/**
 * Created by david on 16/9/8.
 */
public class Initial {

    private final static Logger LOGGER = LogManager.getLogger(Initial.class);

    public void init() throws Exception {
        Arrays.asList(DEFAULT_HADOOP_CONFIG).forEach(n ->
                hdfcConfiguration.addDefaultResource(n)
        );
        InputStream inputStream = null;
        try {
            inputStream = this.getClass().getResourceAsStream(DEFAULT_UP_NAME);
            properties.load(inputStream);
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
        initHibernate();
        initHdfs();
    }

    /////////////////////////////////////////////////
    // Hibernate
    /////////////////////////////////////////////////
    private void initHibernate() throws Exception {
        if ("on".equalsIgnoreCase(properties.getProperty(UP_RDB_STATUS))) {
            hbRegistry = new StandardServiceRegistryBuilder().configure(DEFAULT_UP_HIBERNATE).build();
            hbFactory = new MetadataSources(hbRegistry).buildMetadata().buildSessionFactory();
        }
    }

    /////////////////////////////////////////////////
    // HDFS
    /////////////////////////////////////////////////

    private void initHdfs() throws Exception {
//        Preconditions.checkArgument();
        System.getProperties().put(HADOOP_USER_NAME, properties.getProperty(UP_HDFS_SUPER_USER));
        ZK.instance().getHDFSActiveNode();
    }

    public void close() {
        ZK.instance().close();
        if ("on".equalsIgnoreCase(properties.getProperty(UP_RDB_STATUS))) {
            hbFactory.close();
        }
    }

}
