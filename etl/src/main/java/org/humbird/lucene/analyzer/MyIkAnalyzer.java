package org.humbird.lucene.analyzer;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.IOUtils;
import org.humbird.lucene.token.MyIKTokenizer;

import java.io.Reader;
import java.io.StringReader;

/**
 * Created by david on 16/11/14.
 */
public class MyIkAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String str) {
        Reader reader = null;
        try {
            reader = new StringReader(str);
            MyIKTokenizer it = new MyIKTokenizer(reader, null);
            return new Analyzer.TokenStreamComponents(it);
        } finally {
            IOUtils.closeWhileHandlingException(reader);
        }
    }
}
