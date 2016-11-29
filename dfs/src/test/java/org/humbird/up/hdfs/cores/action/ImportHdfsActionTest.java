package org.humbird.up.hdfs.cores.action;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.humbird.up.hdfs.Initial;
import org.humbird.up.hdfs.JoyTest;
import org.humbird.up.hdfs.commons.Constant;
import org.humbird.up.hdfs.cores.HdfsJoy;
import org.humbird.up.hdfs.cores.IJoy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

/**
 * Created by david on 16/9/27.
 */
public class ImportHdfsActionTest {

    private final static Logger LOGGER = LogManager.getLogger(JoyTest.class);

    Initial initial;
    IJoy joy;

    @Before
    public void init() throws Exception {
        initial = new Initial();
        initial.init();
    }

    @After
    public void close() {
        initial.close();
    }

    @Test
    public void play() throws Exception {
        joy = new HdfsJoy();
        File src = new File("/install_apps/api/cetc/Up/dfs/src/main/resources/analysis/1991_2016.txt");
        Path dest = new Path("/humbird/test/1991_2016.txt");
        IAction<FileSystem, List<File>, Path> iAction;
        iAction = new ImportHdfsAction();
        iAction.getSources().add(src);
        iAction.setDest(dest);
        iAction.setType(Constant.HDFS_FILE_TYPE);
        joy.make(iAction);
    }

}