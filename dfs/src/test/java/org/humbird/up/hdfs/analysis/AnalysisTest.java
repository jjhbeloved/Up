package org.humbird.up.hdfs.analysis;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.humbird.up.hdfs.BaseTest;
import org.junit.Test;

/**
 * Created by david on 16/10/9.
 */
public class AnalysisTest extends BaseTest {

    private final static Logger LOGGER = LogManager.getLogger(AnalysisTest.class);

    @Test
    public void easy() throws Exception {
        Analysis analysis = new Analysis();
        analysis.easy();
    }

}