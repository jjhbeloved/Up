package org.humbird.lucene.query;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.queries.CustomScoreProvider;
import org.apache.lucene.queries.CustomScoreQuery;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.humbird.lucene.util.SearchUtil;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * Created by david on 16/11/15.
 * <p>
 * 自定义 评分, 根据自定义 评分域 修改评分值
 */
public class VipScoreQuery {

    public void searchScore(Query query) {
        IndexReader indexReader = null;
        try {
            // 1. create Directory
            Directory directory = FSDirectory.open(Paths.get("/Users/david/Desktop/CETC/railway/hbase_test/idx.tmp"));
            // 2. create IndexReader
            IndexSearcher indexSearcher = SearchUtil.getSearch(directory);
            // 创建一个整形 评分[不是值score, 而是内部的评分] 查询域
            FunctionQuery scoreQuery = new FunctionQuery(new IntFieldSource("score"));
//            CustomScoreQuery customScoreQuery = new VipScoreQuerySub(query, scoreQuery);
            CustomScoreQuery customScoreQuery = new FilenameScoreQuery(query);
            TopDocs topDocs = indexSearcher.search(customScoreQuery, 50);
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                Document document = indexSearcher.doc(scoreDoc.doc);
                System.out.println(scoreDoc.doc + ":[" + document.getField("name").boost() + "]:(" + scoreDoc.score + ")(" + document.get("path") + ")(" + document.get("name") + ")(" + document.get("size") + ")");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (indexReader != null) {
                try {
                    indexReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class VipScoreQuerySub extends CustomScoreQuery {

        public VipScoreQuerySub(Query subQuery, FunctionQuery scoringQuery) {
            super(subQuery, scoringQuery);
        }

        @Override
        protected CustomScoreProvider getCustomScoreProvider(LeafReaderContext context) throws IOException {
            return super.getCustomScoreProvider(context);
//            return new VipScoreProvider(context);
        }
    }

    private class FilenameScoreQuery extends CustomScoreQuery {

        public FilenameScoreQuery(Query subQuery) {
            super(subQuery);
        }

        @Override
        protected CustomScoreProvider getCustomScoreProvider(LeafReaderContext context) throws IOException {
//            return super.getCustomScoreProvider(context);
            return new FilenameScoreProvider(context);
        }
    }

    private class VipScoreProvider extends CustomScoreProvider {

        public VipScoreProvider(LeafReaderContext context) {
            super(context);
        }

        /**
         * @param doc
         * @param subQueryScore 原始评分
         * @param valSrcScore   评分域提供的分值
         * @return
         * @throws IOException
         */
        @Override
        public float customScore(int doc, float subQueryScore, float valSrcScore) throws IOException {
//            return subQueryScore * valSrcScore;
            return subQueryScore / valSrcScore;
        }
    }

    private class FilenameScoreProvider extends CustomScoreProvider {

        SortedDocValues name;

        public FilenameScoreProvider(LeafReaderContext context) {
            super(context);
            try {
                name = context.reader().getSortedDocValues("name");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * @param doc
         * @param subQueryScore 原始评分
         * @param valSrcScore   评分域提供的分值
         * @return
         * @throws IOException
         */
        @Override
        public float customScore(int doc, float subQueryScore, float valSrcScore) throws IOException {
            BytesRef bytesRef = name.get(doc);
            String name = bytesRef.utf8ToString();
            System.out.println(name);
            if (StringUtils.endsWith(name, "xml")) {
                return subQueryScore * 3.5F;
            }
            return subQueryScore;
        }
    }
}
