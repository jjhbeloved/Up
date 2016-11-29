package org.humbird.up.hdfs.cores;

import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.humbird.up.hdfs.commons.Constant;
import org.humbird.up.hdfs.cores.action.IAction;

/**
 * Created by david on 16/9/10.
 */
public class HdfsJoy implements IJoy {

    private final static Logger LOGGER = LogManager.getLogger(HdfsJoy.class);

    @Override
    public void make(IAction iAction) throws Exception {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(Constant.hdfcConfiguration);
            iAction.play(fs);
        } finally {
            if (fs != null) {
                fs.close();
            }
        }
    }

    @Override
    public Object happy(IAction action) throws Exception {
        return null;
    }
}
