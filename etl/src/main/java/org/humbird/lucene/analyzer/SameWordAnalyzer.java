package org.humbird.lucene.analyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.humbird.lucene.context.SameWordContext;
import org.humbird.lucene.filter.SameWordTokenFilter;

/**
 * Created by david on 16/11/14.
 */
public class SameWordAnalyzer extends Analyzer {

    SameWordContext sameContxt;

    public SameWordAnalyzer(SameWordContext sameContxt) {
        this.sameContxt = sameContxt;
    }

    @Override
    protected TokenStreamComponents createComponents(String s) {
        final StandardTokenizer standardTokenizer = new StandardTokenizer();
        SameWordTokenFilter sameWordTokenFilter = new SameWordTokenFilter(standardTokenizer, sameContxt);
        final StandardFilter standardFilter= new StandardFilter(sameWordTokenFilter);
        return new TokenStreamComponents(standardTokenizer, standardFilter);

    }

}
