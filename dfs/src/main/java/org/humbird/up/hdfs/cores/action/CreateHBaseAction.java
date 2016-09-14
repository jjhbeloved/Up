package org.humbird.up.hdfs.cores.action;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by david on 16/9/12.
 */
public class CreateHBaseAction implements IAction<Connection, List<HTableDescriptor>, HTableDescriptor> {

    private final static Logger LOGGER = LogManager.getLogger(CreateHBaseAction.class);

    private List<HTableDescriptor> sources = new ArrayList();

    private HTableDescriptor dest;

    private String type;

    @Override
    public void Play(Connection connection) throws Exception {
        Admin admin = connection.getAdmin();
        int size = sources.size();
        if (size > 0) {
            sources.forEach(ht -> {
                try {
                    create(admin, ht);
                } catch (IOException e) {
                    LOGGER.error(e);
                }
            });
        } else {
            create(admin, dest);
        }
        admin.close();
    }

    private void create(Admin admin, HTableDescriptor hTableDescriptor) throws IOException {
        TableName tableName = hTableDescriptor.getTableName();
        if (admin.tableExists(tableName)) {
            if ("update".equalsIgnoreCase(type)) {
                admin.disableTable(tableName);
                admin.modifyNamespace(NamespaceDescriptor.create(tableName.getNamespaceAsString()).build());
                admin.modifyTable(tableName, hTableDescriptor);
                admin.enableTable(tableName);
            }
        } else {
            admin.createNamespace(NamespaceDescriptor.create(tableName.getNamespaceAsString()).build());
            admin.createTable(hTableDescriptor);
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
