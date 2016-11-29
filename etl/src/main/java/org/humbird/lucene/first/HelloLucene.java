package org.humbird.lucene.first;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.humbird.lucene.util.SearchUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Random;

/**
 * Created by david on 16/11/3.
 */
public class HelloLucene {

    public void addLongPoint(Document document, String name, long value) {
        Field field = new LongPoint(name, value);
        document.add(field);
        //要排序，必须添加一个同名的NumericDocValuesField
        field = new NumericDocValuesField(name, value);
        document.add(field);
        //要存储值，必须添加一个同名的StoredField
        field = new StoredField(name, value);
        document.add(field);
    }

    public void addIntPoint(Document document, String name, int value) {
        Field field = new IntPoint(name, value);
        document.add(field);
        //要排序，必须添加一个同名的NumericDocValuesField
        field = new NumericDocValuesField(name, value);
        document.add(field);
        //要存储值，必须添加一个同名的StoredField
        field = new StoredField(name, value);
        document.add(field);
    }

    /**
     * create index
     */
    public void index() {
        IndexWriter indexWriter = null;
        try {
            // 1. create Directory
            // - 内存型文档
            // - 本地类型
//            Directory directory = new RAMDirectory();
            // FSDirectory 会自动选择最好的
            Directory directory = FSDirectory.open(Paths.get("/Users/david/Desktop/CETC/railway/hbase_test/idx.tmp"));
            // 2. create IndexWriter
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
            indexWriter = new IndexWriter(directory, indexWriterConfig);
            // 3. create Document
            Document document = null;
            // 4. add Filed to Document
            File dir = new File("/Users/david/Desktop/CETC/railway/hbase_bak");
            Random Random = new Random();
            for (File file : dir.listFiles()) {
                if (!file.isFile()) break;
                document = new Document();
                int score = Random.nextInt(600);
                // StoredField 没有索引的 Field不支持评分
//                Field field = new StoredField("content", FileUtils.readFileToString(file, "UTF-8"));
                document.add(new TextField("content", FileUtils.readFileToString(file, "UTF-8"), Field.Store.YES));
//                document.add(new TextField("content", FileUtils.readFileToString(file, "UTF-8"), YES));
//                document.add(new Field("content", FileUtils.readFileToString(file, "UTF-8"), TextField.TYPE_NOT_STORED));
//                document.add(new TextField("name", file.getName(), YES));
                FieldType INDEX_INFO = new FieldType();
                INDEX_INFO.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
                INDEX_INFO.setTokenized(true);
                INDEX_INFO.setStored(true);
                TextField textField = new TextField("name", file.getName(), Field.Store.YES);
                textField.setBoost(2.0F);
                document.add(textField);
                document.add(new SortedDocValuesField("name", new BytesRef(file.getName().getBytes())));
                addLongPoint(document, "size", file.length());
                addIntPoint(document, "score", score);
                document.add(new Field("path", file.getAbsolutePath(), INDEX_INFO));

                System.out.println(file.getAbsolutePath());
                // 5. write index to RAM/DISK
                indexWriter.addDocument(document);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (indexWriter != null) {
                try {
                    indexWriter.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Search Index
     */
    public void search() {
        IndexReader indexReader = null;
        try {
            // 1. create Directory
            Directory directory = FSDirectory.open(Paths.get("/Users/david/Desktop/CETC/railway/hbase_test/idx.tmp"));
            // 2. create IndexReader
//            indexReader = DirectoryReader.open(directory);
            // 3. dependency IndexReader to create IndexSearcher
            IndexSearcher indexSearcher = SearchUtil.getSearch(directory);
            // 4. create Query
//            TermQuery query = new TermQuery(new Term("path", "hbase-site.xml"));  // 精确匹配查询
//            Query query = new TermRangeQuery(); // 范围查询, 无法查询内容为 数字存储的
//            Query query = LegacyNumericRangeQuery.newFloatRange(); // 范围查询, 无法查询内容为 数字存储的
//            Query query = IntPoint.newRangeQuery(); // 整形范围查询, 默认使用 id 进行, 也就是默认没有排序
//            Query query = FloatPoint.newRangeQuery(); // 浮点型范围查询
//            Query query = new PrefixQuery(new Term("path", "a")); // 前缀搜索, value值只匹配前缀, 会匹配文档内容里面每个单词的前缀
//            Query query = new WildcardQuery(new Term("path", "J*")); // 通配符搜索, ?表示匹配一个字符(可以输入任意多个), *表示匹配人一多字符
            // occur-> MUST SHOULD FILER NOT_MUST
//            Query query = new BooleanQuery.Builder().add(new BooleanClause(new TermQuery(new Term("name", "yangxuan")), BooleanClause.Occur.MUST)).add(new BooleanClause(new TermQuery(new Term("content", "name")), BooleanClause.Occur.SHOULD)).build(); // 多条件匹配查询
//            Query query = new PhraseQuery.Builder().setSlop(1).add(new Term("cotent", "who")).add(new Term("content", "you")).build(); // 短语查询, 在中文没什么用, 查询 who are you(输入who ... you 能匹配的), setSlop 设置...为1个跳数(跳过几个...), 尽量少用, cpu开销大
//            Query query = new FuzzyQuery(new Term("name", "make")); // 模糊查询, 和make相差一个有字符的单词(mike就能匹配), 少用, 需要计算
            QueryParser queryParser = new QueryParser("path", new StandardAnalyzer());
//            Query query = queryParser.parse(queryParser.escape("/Users/david/Desktop/CETC/railway/hbase_bak/hbase-site.xml"));
            Query query = queryParser.parse("\\/*c.txt");
//            Query query = queryParser.parse("\"I football\"~1");
//            SortField query = new SortField("content", SortField.Type.STRING_VAL, true);
            // 5. return TopDocs
            TopDocs topDocs = indexSearcher.search(query, 5); // 分页5
            // 6. dependency TopDocs get scoreDoc
            ScoreDoc[] scoreDocs = topDocs.scoreDocs;
            ScoreDoc[] tScoreDocs = indexSearcher.searchAfter(scoreDocs[2], query, 5).scoreDocs; // 从20位后面开始查询, 5个显示
            // 7. dependency search and scoreDoc to get Detail Document object
            for (ScoreDoc scoreDoc : scoreDocs) {
                Document indexableFields = indexSearcher.doc(scoreDoc.doc);
                System.out.println(indexableFields.get("path"));
                System.out.println(indexableFields.get("content"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } finally {
            if (indexReader != null) {
                try {
                    indexReader.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    public void count() {
        IndexReader indexReader = null;
        try {
            // 1. create Directory
            Directory directory = FSDirectory.open(Paths.get("/Users/david/Desktop/CETC/railway/hbase_test/idx.tmp"));
            // 2. create IndexReader
            indexReader = DirectoryReader.open(directory);
            System.out.println("max docs: " + indexReader.maxDoc());
            System.out.println("num docs: " + indexReader.numDocs());
            System.out.println("num delete docs: " + indexReader.numDeletedDocs());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (indexReader != null) {
                try {
                    indexReader.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    public void delete() {
        IndexWriter indexWriter = null;
        try {
            // 1. create Directory
            Directory directory = FSDirectory.open(Paths.get("/Users/david/Desktop/CETC/railway/hbase_test/idx.tmp"));
            // 2. create IndexWriter
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
            indexWriter = new IndexWriter(directory, indexWriterConfig);
            indexWriter.deleteDocuments(new Term("name", "c.txt"));
//            indexWriter.forceMergeDeletes();    // 强制删除回收站里的索引, 不可恢复
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (indexWriter != null) {
                try {
                    indexWriter.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    @SuppressWarnings("depracted")
    public void undelete() {
        IndexReader indexReader = null;
        try {
            // 1. create Directory
            Directory directory = FSDirectory.open(Paths.get("/Users/david/Desktop/CETC/railway/hbase_test/idx.tmp"));
            // 2. create IndexReader
            indexReader = DirectoryReader.open(directory);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (indexReader != null) {
                try {
                    indexReader.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    // 排序查询
    public void searchSort(String fieldName, String sql, Sort sort) {
        IndexReader indexReader = null;
        try {
            // 1. create Directory
            Directory directory = FSDirectory.open(Paths.get("/Users/david/Desktop/CETC/railway/hbase_test/idx.tmp"));
            // 2. create IndexReader
            IndexSearcher indexSearcher = SearchUtil.getSearch(directory);
            QueryParser queryParser = new QueryParser(fieldName, new StandardAnalyzer());
            Query query = queryParser.parse(sql);
            TopDocs topDocs;
            if (sort == null) {
                topDocs = indexSearcher.search(query, 50);
            } else {
                topDocs = indexSearcher.search(query, 50, sort);
            }
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                Document document = indexSearcher.doc(scoreDoc.doc);
                System.out.println(scoreDoc.doc + ":[" + document.getField("name").boost() + "]:(" + scoreDoc.score + ")(" + document.get("path") + ")(" + document.get("name") + ")(" + document.get("size") + "):(" + document.get("score") + ")");
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

    // 过滤查询
    public void searchFilter(String fieldName, String sql, FilterCollector collector) {
        IndexReader indexReader = null;
        try {
            // 1. create Directory
            Directory directory = FSDirectory.open(Paths.get("/Users/david/Desktop/CETC/railway/hbase_test/idx.tmp"));
            // 2. create IndexReader
            IndexSearcher indexSearcher = SearchUtil.getSearch(directory);
            QueryParser queryParser = new QueryParser(fieldName, new StandardAnalyzer());
            Query query = queryParser.parse(sql);
            TopDocs topDocs = null;
            if (collector == null) {
                topDocs = indexSearcher.search(query, 50);
            } else {
                indexSearcher.search(query, collector);
//                FilterLeafReader.FilterTerms
            }
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

    public void searchQuery(Query query) {
        IndexReader indexReader = null;
        try {
            // 1. create Directory
            Directory directory = FSDirectory.open(Paths.get("/Users/david/Desktop/CETC/railway/hbase_test/idx.tmp"));
            // 2. create IndexReader
            IndexSearcher indexSearcher = SearchUtil.getSearch(directory);
            TopDocs topDocs = indexSearcher.search(query, 50);
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
}
