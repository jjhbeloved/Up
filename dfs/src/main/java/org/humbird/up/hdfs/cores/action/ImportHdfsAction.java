package org.humbird.up.hdfs.cores.action;

import com.google.common.io.Files;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.humbird.up.hdfs.commons.Constant;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.humbird.up.hdfs.commons.Constant.properties;

/**
 * Created by david on 16/9/10.
 */
public class ImportHdfsAction implements IAction<FileSystem, List<File>, Path> {

    private final static Logger LOGGER = LogManager.getLogger(ImportHdfsAction.class);

    private List<File> sources = new ArrayList();

    private Path dest;

    private String type;

    private String user = properties.getProperty(Constant.UP_HDFS_HUMBIRD_USER);

    public ImportHdfsAction() {
    }

    public ImportHdfsAction(Path dest, String type) {
        this.dest = dest;
        this.type = type;
    }

    @Override
    public void play(FileSystem fs) throws IOException {

        final OutputStream[] os = new OutputStream[1];
        final InputStream[] is = new InputStream[1];

        switch (type) {
            case Constant.HDFS_FILE_TYPE:
                if (!fs.exists(dest.getParent())) {
                    fs.mkdirs(dest.getParent(), new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE, FsAction.READ_EXECUTE));
                    fs.setOwner(dest.getParent(), user, user);
                }
                os[0] = fs.create(dest, true);
                is[0] = Files.asByteSource(sources.get(0)).openBufferedStream();
                IOUtils.copyBytes(is[0], os[0], 8192, true);
                fs.setOwner(dest, user, user);
                Arrays.asList(fs.listStatus(dest.getParent())).forEach(
                        n -> LOGGER.info(n.getPath().getName() + " --- " + n.getOwner() + " --- " + n.getGroup() + " --- " + n.getPermission().toString()
                        )
                );
                break;
            case Constant.HDFS_DIR_TYPE:
                if (!fs.exists(dest)) {
                    fs.mkdirs(dest, new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE, FsAction.READ_EXECUTE));
                    fs.setOwner(dest, user, user);
                }
                sources.forEach(file -> {
                    if (file.isDirectory()) {
                        for (File f : file.listFiles()) {
                            Path target = new Path(dest, f.getName());
                            try {
                                os[0] = fs.create(target, true);
                                is[0] = Files.asByteSource(f).openBufferedStream();
                                IOUtils.copyBytes(is[0], os[0], 8192, true);
                                fs.setOwner(target, user, user);
                            } catch (IOException e) {
                                LOGGER.error(e);
                            }
                        }
                    }
                });
                Arrays.asList(fs.listStatus(dest)).forEach(
                        n -> LOGGER.info(n.getPath().getName() + " --- " + n.getOwner() + " --- " + n.getGroup() + " --- " + n.getPermission().toString()
                        )
                );
                break;
            default:
                break;
        }
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

    public List<File> getSources() {
        return this.sources;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
