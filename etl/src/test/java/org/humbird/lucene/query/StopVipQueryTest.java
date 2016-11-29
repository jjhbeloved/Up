package org.humbird.lucene.query;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.humbird.lucene.util.SearchUtil;
import org.junit.Test;

/**
 * Created by david on 16/11/17.
 */
public class StopVipQueryTest {
    @Test
    public void search() throws Exception {
        String dir = "/Users/david/Desktop/CETC/railway/hbase_test/idx.tmp";
        QueryParser queryParser = new StopVipQuery("content", new StandardAnalyzer());
//        Query query = queryParser.parse("*hbase");
//        SearchUtil.search(query, dir);
//        Query query = queryParser.parse("hbase~0.8");
//        SearchUtil.search(query, dir);
        Query query = queryParser.parse("content:[hdfs TO sname]");
        SearchUtil.search(query, dir);
    }

}