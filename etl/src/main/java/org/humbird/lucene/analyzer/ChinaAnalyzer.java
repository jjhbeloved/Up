package org.humbird.lucene.analyzer;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.core.LetterTokenizer;
import org.apache.lucene.analysis.core.StopAnalyzer;

/**
 * Created by david on 16/11/14.
 */
public class ChinaAnalyzer extends Analyzer {

    private CharArraySet stops;

    private void setup() {
        stops.addAll(StopAnalyzer.ENGLISH_STOP_WORDS_SET);
    }

    public ChinaAnalyzer() {
        setup();
    }

    public ChinaAnalyzer(String[] strings) {
        stops = StopFilter.makeStopSet(strings, true);
        setup();
    }

    /**
     * 这里创建分词器 和 过滤器 chain[stopFilter]
     * @param fieldName
     * @return
     */
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        LetterTokenizer source = new LetterTokenizer();
        return new TokenStreamComponents(source, new StopFilter(source, stops));
    }

    /**
     * 这里创建 过滤器filter
     * @param fieldName
     * @param in
     * @return
     */
    protected TokenStream normalize(String fieldName, TokenStream in) {
        return new LowerCaseFilter(in);
    }

}
