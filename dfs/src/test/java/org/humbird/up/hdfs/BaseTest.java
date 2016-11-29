package org.humbird.up.hdfs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.humbird.up.hdfs.cores.IJoy;
import org.junit.After;
import org.junit.Before;

/**
 * Created by david on 16/10/9.
 */
public class BaseTest {

    private final static Logger LOGGER = LogManager.getLogger(BaseTest.class);

    public Initial initial;
    public IJoy joy;

    @Before
    public void init() throws Exception {
        initial = new Initial();
        initial.init();
    }

    @After
    public void close() {
        initial.close();
    }
}
