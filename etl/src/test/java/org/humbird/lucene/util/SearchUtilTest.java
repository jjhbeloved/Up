package org.humbird.lucene.util;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * Created by david on 16/11/19.
 */
public class SearchUtilTest {


    SearchUtil searchUtil;

    @Before
    public void init() throws IOException {
        Directory directory = FSDirectory.open(Paths.get("/Users/david/Desktop/CETC/railway/hbase_test/idx.tmp"));
        searchUtil = new SearchUtil(directory);
    }

    @Test
    public void commit() throws Exception {
        searchUtil.commit();
    }

    @Test
    public void close() throws Exception {
        searchUtil.close();
    }

    @Test
    public void search() throws Exception {

        searchUtil.query();
        System.out.println("===========================================");
        for (int i=0; i<5; i++) {
            searchUtil.search(new QueryParser("name", new StandardAnalyzer()).parse("h*"));
            searchUtil.query();
//            SearchUtil.search(new QueryParser("name", new StandardAnalyzer()).parse("h*"));
            System.out.println("===========================================");
            Thread.sleep(2000);
            if (i==2) {
                searchUtil.update();
            }
            searchUtil.delete(new QueryParser("name", new StandardAnalyzer()).parse("'hbase-site2.xml'"));
        }
//        searchUtil.commit();
//        searchUtil.close();
    }

    @Test
    public void index() throws Exception {
        searchUtil.index();
    }
}