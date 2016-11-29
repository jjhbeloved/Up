package org.humbird.up.hdfs.cores.action;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by david on 16/9/18.
 */
public class DropHBaseAction implements IAction<Connection, List<HTableDescriptor>, HTableDescriptor> {

    private final static Logger LOGGER = LogManager.getLogger(DropHBaseAction.class);

    private List<HTableDescriptor> sources = new ArrayList();

    private HTableDescriptor dest;

    private String type;

    @Override
    public void play(Connection connection) throws Exception {
        Admin admin = connection.getAdmin();
        int size = sources.size();
        if (size > 0) {
            sources.forEach(ht -> {
                try {
                    drop(admin, ht);
                } catch (Exception e) {
                    LOGGER.error(e);
                }
            });
        } else {
            drop(admin, dest);
        }
        admin.close();
    }

    @Override
    public Object duang(Connection object) throws Exception {
        return null;
    }

    private void drop(Admin admin, HTableDescriptor htd) throws IOException {
        TableName tableName = htd.getTableName();
        if (admin.tableExists(tableName)) {
            if (admin.isTableEnabled(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                try {
                    admin.deleteNamespace(tableName.getNamespaceAsString());
                } catch (IOException e) {
                    LOGGER.warn(e.getMessage());
                }
            }
        }
    }

    @Override
    public HTableDescriptor getDest() {
        return dest;
    }

    @Override
    public void setDest(HTableDescriptor dest) {
    this.dest = dest;
    }

    @Override
    public List<HTableDescriptor> getSources() {
        return sources;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public void setType(String type) {
    this.type = type;
    }
}
