package org.humbird.lucene.query;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.junit.Test;

/**
 * Created by david on 16/11/15.
 */
public class VipScoreQueryTest {

    @Test
    public void searchScore() throws Exception {
        VipScoreQuery vipScoreQuery = new VipScoreQuery();
        QueryParser queryParser = new QueryParser("content", new StandardAnalyzer());
        Query query = queryParser.parse("hbase");
        vipScoreQuery.searchScore(query);
    }

}