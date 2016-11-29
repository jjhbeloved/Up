package org.humbird.lucene.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;

/**
 * Created by david on 16/11/17.
 */
public class StopVipQuery extends QueryParser {

    public StopVipQuery(String f, Analyzer a) {
        super(f, a);
    }

    @Override
    protected Query getWildcardQuery(String field, String termStr) throws ParseException {
        throw new ParseException("stop wildcard... plase change other query parser.");
//        return super.getWildcardQuery(field, termStr);
    }

    @Override
    protected Query getFuzzyQuery(String field, String termStr, float minSimilarity) throws ParseException {
        throw new ParseException("stop fuzzy... plase change other query parser.");
//        return super.getFuzzyQuery(field, termStr, minSimilarity);
    }

    @Override
    protected Query getRangeQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive) throws ParseException {
        if (field.equals("content")) {
            System.out.println(part1);
            System.out.println(part2);
//           return IntPoint.newRangeQuery(field, Integer.parseInt(part1), Integer.parseInt(part1));
        }

        return new TermRangeQuery(field, new BytesRef(part1.getBytes()), new BytesRef(part2.getBytes()), startInclusive, endInclusive);
//        return super.getRangeQuery(field, part1, part2, startInclusive, endInclusive);
    }
}
