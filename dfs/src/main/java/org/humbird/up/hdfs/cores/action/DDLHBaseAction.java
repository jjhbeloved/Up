package org.humbird.up.hdfs.cores.action;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.humbird.up.hdfs.utils.HBaseTool;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by david on 16/9/12.
 */
public class DDLHBaseAction implements IAction<Connection, List, TableName> {

    private final static Logger LOGGER = LogManager.getLogger(DDLHBaseAction.class);

    private List sources = new ArrayList();

    private TableName dest;

    private String type = HBaseTool.PUT;

    // put的 rowkey 不要超过 16 byte
    @Override
    public void play(Connection connection) throws Exception {
        Table table = null;
        try {
            table = connection.getTable(dest);
            switch (type) {
                case HBaseTool.PUT:
//                    table.batch(sources, new Object[]{});
                    table.put((List<Put>) sources);
                    break;
                case HBaseTool.DELETE:
                    table.delete((List<Delete>) sources);
                    break;
                case HBaseTool.UPDATE:
                    break;
                default:
                    break;
            }

        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    @Override
    public Object duang(Connection connection) throws Exception {
        Table table = null;
        try {
            table = connection.getTable(dest);
            switch (type) {
                case HBaseTool.GET:
                    return table.get(sources);
                case HBaseTool.SCAN:
                    ResultScanner scanner = table.getScanner(new Scan());
                    List<Result> results = new ArrayList();
                    for (Result result : scanner) {
                        results.add(result);
                    }
                    return results;
                default:
                    break;
            }
        } finally {
            if (table != null) {
                table.close();
            }
        }
        return null;
    }

    @Override
    public TableName getDest() {
        return dest;
    }

    @Override
    public void setDest(TableName dest) {
        this.dest = dest;
    }

    @Override
    public List getSources() {
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
