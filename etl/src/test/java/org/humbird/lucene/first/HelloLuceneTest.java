package org.humbird.lucene.first;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.highlight.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by david on 16/11/3.
 */
public class HelloLuceneTest {

    HelloLucene helloLucene = null;

    @Test
    public void searchSort() throws Exception {
//        helloLucene.searchSort("path", "\\/*xml", null);
        helloLucene.searchSort("content", "hbase", null);
        System.out.println("=======================");
        // 设置了排序之后, 就没有评分了
        // Sort.INDEXORDER 通过 doc.id 来进行排序
//        helloLucene.searchSort("\\/*xml", Sort.INDEXORDER);
        // Sort.RELEVANCE
//        helloLucene.searchSort("\\/*xml", Sort.RELEVANCE);
        //
        helloLucene.searchSort("content", "hbase", new Sort(new SortField("size", SortField.Type.INT), SortField.FIELD_SCORE));
        System.out.println("=======================");
        helloLucene.searchSort("content", "hbase", new Sort(new SortField("size", SortField.Type.INT, true)));

    }

    @Test
    public void searchFilter() throws Exception {

    }

    @Test
    public void searchQuery() throws Exception {
        Query query = new WildcardQuery(new Term("content", "*hdfs"));
        helloLucene.searchQuery(query);
    }

    @Test
    public void highLight() throws IOException, InvalidTokenOffsetsException, ParseException {
        String txt = "i love baby one, baby is cute. cute love nice.";
//        Query query = new TermQuery(new Term("name", "baby"));  // 单个加粗
//        Query query = new QueryParser("name", new StandardAnalyzer()).parse("name:baby cute"); // 多词加粗
        Query query = new MultiFieldQueryParser(new String[]{"name", "content"}, new StandardAnalyzer()).parse("baby love");
        QueryScorer queryScorer = new QueryScorer(query);
        Fragmenter fragmenter = new SimpleSpanFragmenter(queryScorer);
        Formatter formatter = new SimpleHTMLFormatter("<span style='color:red'>", "</span>");
        Highlighter highlighter = new Highlighter(queryScorer);
        highlighter.setTextFragmenter(fragmenter);
        String ret = highlighter.getBestFragment(new StandardAnalyzer(), "name", txt);
        System.out.println(ret);
    }

    @Before
    public void setup() {
        helloLucene = new HelloLucene();
    }

    @Test
    public void count() throws Exception {
        helloLucene.count();
    }

    @Test
    public void delete() throws Exception {
        helloLucene.delete();
    }

    @Test
    public void undelete() throws Exception {

    }

    @Test
    public void search() throws Exception {
        helloLucene.search();
    }

    @Test
    public void index() throws Exception {
        helloLucene.index();
    }

}