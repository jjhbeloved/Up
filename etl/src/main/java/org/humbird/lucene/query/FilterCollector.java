package org.humbird.lucene.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;

import java.io.IOException;

/**
 * Created by david on 16/11/17.
 */
public class FilterCollector implements Collector {

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        context.reader().terms("name");
        return null;
    }

    @Override
    public boolean needsScores() {
        return false;
    }
}
