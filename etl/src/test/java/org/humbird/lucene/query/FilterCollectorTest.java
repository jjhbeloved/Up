package org.humbird.lucene.query;

import org.apache.lucene.search.Collector;
import org.junit.Test;

/**
 * Created by david on 16/11/17.
 */
public class FilterCollectorTest {
    @Test
    public void getLeafCollector() throws Exception {
        String dir = "/Users/david/Desktop/CETC/railway/hbase_test/idx.tmp";
        Collector collector = new FilterCollector();
//        SearchUtil.search(query, dir);
    }

}