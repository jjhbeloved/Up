package org.humbird.lucene.analyzer;

import com.chenlb.mmseg4j.Dictionary;
import com.chenlb.mmseg4j.MaxWordSeg;
import com.chenlb.mmseg4j.Seg;
import org.apache.lucene.analysis.Analyzer;
import org.humbird.lucene.token.MyMMsegTokenizer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;

/**
 * Created by david on 16/11/14.
 */
public class MyMMsegAnalyzer extends Analyzer {
    protected Dictionary dic;
    public Reader reader;

    public MyMMsegAnalyzer() {
        this.dic = Dictionary.getInstance();
    }

    public MyMMsegAnalyzer(boolean b) {
        this.dic = Dictionary.getInstance();
        try {
            this.reader = new FileReader(dic.getDicPath());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected TokenStreamComponents createComponents(String s) {
        return new TokenStreamComponents(new MyMMsegTokenizer(this.newSeg(), this.reader));
    }


    public MyMMsegAnalyzer(String path) {
        this.dic = Dictionary.getInstance(path);
    }

    public MyMMsegAnalyzer(File path) {
        this.dic = Dictionary.getInstance(path);
    }

    public MyMMsegAnalyzer(Dictionary dic) {
        this.dic = dic;
    }

    protected Seg newSeg() {
        return new MaxWordSeg(this.dic);
    }

    public Dictionary getDict() {
        return this.dic;
    }

}
