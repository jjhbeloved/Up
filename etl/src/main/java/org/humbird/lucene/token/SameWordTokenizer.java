package org.humbird.lucene.token;

import org.apache.lucene.analysis.Tokenizer;

import java.io.IOException;

/**
 * Created by david on 16/11/14.
 */
public class SameWordTokenizer extends Tokenizer {

    @Override
    public boolean incrementToken() throws IOException {
        return false;
    }

}