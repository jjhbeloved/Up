package org.humbird.up.hdfs.cores.action;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.humbird.up.hdfs.commons.Constant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.humbird.up.hdfs.commons.Constant.properties;

/**
 * Created by david on 16/9/10.
 */
public class DeleteHdfsAction implements IAction<FileSystem, List<Path>, Path> {

    private final static Logger LOGGER = LogManager.getLogger(DeleteHdfsAction.class);

    private List<Path> sources = new ArrayList();

    private Path dest;

    private String type;

    private String user = properties.getProperty(Constant.UP_HDFS_HUMBIRD_USER);

    public DeleteHdfsAction() {
    }

    public DeleteHdfsAction(Path dest, String type) {
        this.dest = dest;
        this.type = type;
    }

    @Override
    public void play(FileSystem fs) throws IOException {

        sources.forEach( p -> {
            try {
                fs.delete(p, true);
            } catch (IOException e) {
                LOGGER.error(e.getMessage());
            }
        });
    }

    @Override
    public Object duang(FileSystem object) throws Exception {
        return null;
    }

    public Path getDest() {
        return dest;
    }

    public void setDest(Path dest) {
        this.dest = dest;
    }

    public List<Path> getSources() {
        return this.sources;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
