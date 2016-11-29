package org.humbird.lucene.util;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

/**
 * Created by david on 16/11/13.
 */
public class SearchUtil {

    public static DirectoryReader directoryReader = null;
    private SearcherManager searcherManager;
    private ReaderManager readerManager;
    private IndexWriter indexWriter;
    private ControlledRealTimeReopenThread controlledRealTimeReopenThread;

    SearchUtil(Directory directory) {
        try {
            indexWriter = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()));
            searcherManager = new SearcherManager(indexWriter, new SearcherFactory());
            readerManager = new ReaderManager(indexWriter);
            // 最佳实践0.025~5.0
//            controlledRealTimeReopenThread = new ControlledRealTimeReopenThread(indexWriter, searcherManager, 5.0, 0.025);
            controlledRealTimeReopenThread = new ControlledRealTimeReopenThread(indexWriter, searcherManager, 2.0, 1.0);
            controlledRealTimeReopenThread.setDaemon(true);
            controlledRealTimeReopenThread.setName("ControlledRealTimeReopenThread");
            controlledRealTimeReopenThread.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static IndexSearcher getSearch(Directory directory) throws IOException {
        if (directoryReader == null) {
            directoryReader = DirectoryReader.open(directory);
        } else {
            DirectoryReader openIfChanged = DirectoryReader.openIfChanged(directoryReader);
            if (openIfChanged != null) {
                directoryReader.close();
                directoryReader = openIfChanged;
            }
        }
        return new IndexSearcher(directoryReader);
    }

    public static void search(Query query, String dir) {
        IndexReader indexReader = null;
        try {
            // 1. create Directory
            Directory directory = FSDirectory.open(Paths.get(dir));
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

    public static void search(Query query, Collector collector, String dir) {
        IndexReader indexReader = null;
        try {
            // 1. create Directory
            Directory directory = FSDirectory.open(Paths.get(dir));
            // 2. create IndexReader
            IndexSearcher indexSearcher = SearchUtil.getSearch(directory);
            indexSearcher.search(query, collector);
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

    /**
     * 使用单例获取IndexSearch
     **/
    public IndexSearcher getSearcher() {
        IndexSearcher is = null;
        try {
            if (is == null) {
                System.out.println(searcherManager.maybeRefresh());//刷新reMgr,获取最新的IndexSearcher
                is = searcherManager.acquire();

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (is == null) {
            throw new RuntimeException("indexSearcher is null!!!!");
        }
        return is;
    }

    public void index() {
        try {
            Document document;
            File dir = new File("/Users/david/Desktop/CETC/railway/hbase_bak");
            for (File file : dir.listFiles()) {
                if (!file.isFile()) break;
                document = new Document();
                document.add(new TextField("content", org.apache.commons.io.FileUtils.readFileToString(file, "UTF-8"), Field.Store.YES));
                FieldType INDEX_INFO = new FieldType();
                INDEX_INFO.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
                INDEX_INFO.setTokenized(true);
                INDEX_INFO.setStored(true);
                TextField textField = new TextField("name", file.getName(), Field.Store.YES);
                textField.setBoost(2.0F);
                document.add(textField);
                document.add(new SortedDocValuesField("name", new BytesRef(file.getName().getBytes())));
                document.add(new Field("path", file.getAbsolutePath(), INDEX_INFO));
                indexWriter.addDocument(document);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            commit();
        }
    }

    public void query() {
        IndexSearcher indexSearcher = getSearcher();
        try {
            System.out.println("max docs: " + indexSearcher.getIndexReader().maxDoc());
            System.out.println("num docs: " + indexSearcher.getIndexReader().numDocs());
            System.out.println("num delete docs: " + indexSearcher.getIndexReader().numDeletedDocs());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                searcherManager.release(indexSearcher);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void search(Query query) {
        IndexSearcher indexSearcher = getSearcher();
        try {
            TopDocs topDocs = indexSearcher.search(query, 50);
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                Document document = indexSearcher.doc(scoreDoc.doc);
                System.out.println(scoreDoc.doc + ":[" + document.getField("name").boost() + "]:(" + scoreDoc.score + ")(" + document.get("path") + ")(" + document.get("name") + ")(" + document.get("size") + ")");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                searcherManager.release(indexSearcher);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void update() {
        try {
            Document document = new Document();
            document.add(new TextField("content", "miaomiaomiao", Field.Store.YES));
            FieldType INDEX_INFO = new FieldType();
            INDEX_INFO.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            INDEX_INFO.setTokenized(true);
            INDEX_INFO.setStored(true);
            TextField textField = new TextField("name", "hcb.xml", Field.Store.YES);
            textField.setBoost(2.0F);
            document.add(textField);
            document.add(new SortedDocValuesField("name", new BytesRef("hcb.xml".getBytes())));
            document.add(new Field("path", "/hcb.xml", INDEX_INFO));
            indexWriter.updateDocument(new Term("name", "hxxx.xml"), document);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void delete(Query query) {
        try {
            indexWriter.deleteDocuments(query);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void commit() {
        try {
            indexWriter.commit();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        controlledRealTimeReopenThread.interrupt();
        controlledRealTimeReopenThread.close();
        try {
            indexWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
