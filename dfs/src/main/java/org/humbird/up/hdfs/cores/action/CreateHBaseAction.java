package org.humbird.up.hdfs.cores.action;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.humbird.up.hdfs.vo.OHTableDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by david on 16/9/12.
 */
public class CreateHBaseAction implements IAction<Connection, List<OHTableDescriptor>, OHTableDescriptor> {

    private final static Logger LOGGER = LogManager.getLogger(CreateHBaseAction.class);

    private List<OHTableDescriptor> sources = new ArrayList();

    private OHTableDescriptor dest;

    private String type;

    // 变更表结构, 会造成表数据丢失(变更表名, 但是列族不变, 不会有问题)
    @Override
    public void play(Connection connection) throws Exception {
        Admin admin = connection.getAdmin();
        int size = sources.size();
        if (size > 0) {
            sources.forEach(ht -> {
                try {
                    create(admin, ht);
                } catch (Exception e) {
                    LOGGER.error(e);
                }
            });
        } else {
            create(admin, dest);
        }
        admin.close();
    }

    @Override
    public Object duang(Connection object) throws Exception {
        return null;
    }

    private void create(Admin admin, OHTableDescriptor ohtd) throws IOException {
        TableName tableName = ohtd.getoHTableDescriptor().getTableName();
        if (admin.tableExists(tableName)) {
            if ("update".equalsIgnoreCase(type)) {
                TableName newTableName = ohtd.getnHTableDescriptor().getTableName();
                boolean flag = tableName.getNamespaceAsString().equals(newTableName.getNamespaceAsString());
                boolean enabled = tableName.getNameAsString().equals(newTableName.getNameAsString());
                try {
                    if (admin.isTableEnabled(tableName)) {
                        admin.disableTable(tableName);
                        if (!flag) {
                            try {
                                admin.createNamespace(NamespaceDescriptor.create(newTableName.getNamespaceAsString()).build());
                            } catch (IOException e) {
                                LOGGER.warn(e.getMessage());
                            }
                        }
                        // modifyTable 只能修改表结构, 不能改变表名
                        if (enabled) {
                            admin.modifyTable(tableName, ohtd.getnHTableDescriptor());
                        } else {
                            String snapName = tableName.getQualifierAsString() + "snapshot";
                            admin.snapshot(snapName, tableName);
                            admin.cloneSnapshot(snapName, newTableName);
                            admin.deleteSnapshot(snapName);
                            admin.deleteTable(tableName);
                            admin.modifyTable(newTableName, ohtd.getnHTableDescriptor());
                        }
                        if (!flag) {
                            try {
                                admin.deleteNamespace(tableName.getNamespaceAsString());
                            } catch (IOException e) {
                                LOGGER.warn(e);
                            }
                        }
                    }
                } finally {
                    if (enabled) {
                        try {
                            admin.enableTable(tableName);
                        } catch (IOException e) {
                            LOGGER.warn(e.getMessage());
                        }
                    }
                }

            }
        } else {
            try {
                admin.createNamespace(NamespaceDescriptor.create(tableName.getNamespaceAsString()).build());
            } catch (IOException e) {
                LOGGER.warn(e.getMessage());
            }
            admin.createTable(ohtd.getoHTableDescriptor(), ohtd.getSplits());
        }
    }

    @Override
    public OHTableDescriptor getDest() {
        return dest;
    }

    @Override
    public void setDest(OHTableDescriptor dest) {
        this.dest = dest;
    }

    @Override
    public List<OHTableDescriptor> getSources() {
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
