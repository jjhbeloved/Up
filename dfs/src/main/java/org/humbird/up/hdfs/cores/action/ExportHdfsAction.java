package org.humbird.up.hdfs.cores.action;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.humbird.up.hdfs.commons.Constant;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.humbird.up.hdfs.commons.Constant.properties;

/**
 * Created by david on 16/9/10.
 */
public class ExportHdfsAction implements IAction<FileSystem, List<Path>, File> {

    private final static Logger LOGGER = LogManager.getLogger(ExportHdfsAction.class);

    private List<Path> sources = new ArrayList();

    private File dest;

    private String type;

    private String user = properties.getProperty(Constant.UP_HDFS_HUMBIRD_USER);

    public ExportHdfsAction() {
    }

    public ExportHdfsAction(File dest, String type) {
        this.dest = dest;
        this.type = type;
    }

    @Override
    public void play(FileSystem fs) throws IOException {

        final OutputStream[] os = new OutputStream[1];
        final InputStream[] is = new InputStream[1];

        switch (type) {
            case Constant.HDFS_FILE_TYPE:
                if (!dest.getParentFile().exists()) {
                    dest.getParentFile().mkdirs();
                }
                is[0] = fs.open(sources.get(0));
                os[0] = new FileOutputStream(dest);
                IOUtils.copyBytes(is[0], os[0], 8192, true);
                LOGGER.info(dest.getName());
                break;
            case Constant.HDFS_DIR_TYPE:
                if (!dest.exists()) {
                    dest.mkdirs();
                }
                sources.forEach(file -> {
                    try {
                        if (fs.isDirectory(file)) {
                            RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(file, false);
                            for (; iterator.hasNext(); ) {
                                LocatedFileStatus status = iterator.next();
                                File target = new File(dest, status.getPath().getName());
                                is[0] = fs.open(status.getPath());
                                os[0] = new FileOutputStream(target);
                                IOUtils.copyBytes(is[0], os[0], 8192, true);
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                File[] files = dest.listFiles();
                for (int i=0; i<files.length; i++) {
                    LOGGER.info(files[i].getAbsolutePath());
                }
                break;
            default:
                break;
        }
    }

    @Override
    public Object duang(FileSystem object) throws Exception {
        return null;
    }

    public File getDest() {
        return dest;
    }

    public void setDest(File dest) {
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
