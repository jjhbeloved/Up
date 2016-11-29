package org.humbird.up.hdfs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.humbird.up.hdfs.cores.IJoy;

/**
 * Created by david on 16/9/8.
 */
public class Main {

    private final static Logger LOGGER = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        Initial initial = new Initial();
        IJoy joy;
        try {
            initial.init();
//            joy = new HdfsJoy();
//            joy = new RDBJoy();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage());
        } finally {
            initial.close();
        }
    }
}
