package org.humbird.up.hdfs.cores.action;

import org.apache.hadoop.hbase.client.Connection;

import java.util.List;

/**
 * Created by david on 16/9/12.
 */
public class ImportHBaseAction implements IAction<Connection, List<String>, String> {

    @Override
    public void Play(Connection connection) throws Exception {
    }

    @Override
    public String getDest() {
        return null;
    }

    @Override
    public void setDest(String dest) {

    }

    @Override
    public List<String> getSources() {
        return null;
    }

    @Override
    public String getType() {
        return null;
    }

    @Override
    public void setType(String type) {

    }
}
