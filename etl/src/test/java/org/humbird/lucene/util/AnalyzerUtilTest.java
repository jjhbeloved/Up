package org.humbird.lucene.util;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.humbird.lucene.analyzer.ChinaAnalyzer;
import org.humbird.lucene.analyzer.MyMMsegAnalyzer;
import org.humbird.lucene.analyzer.SameWordAnalyzer;
import org.humbird.lucene.context.SameWordContextImpl;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by david on 16/11/13.
 */
public class AnalyzerUtilTest {

    @Test
    public void displayDetail() throws Exception {
        SimpleAnalyzer simpleAnalyzer = new SimpleAnalyzer();
        StopAnalyzer stopAnalyzer = new StopAnalyzer();
        WhitespaceAnalyzer whitespaceAnalyzer = new WhitespaceAnalyzer();
        StandardAnalyzer standardAnalyzer = new StandardAnalyzer();
        String str = "Hi mingming, I'm come from China. My e-mail is ccn@cnm.com and telephone number is 1622202";
        AnalyzerUtil.displayDetail(simpleAnalyzer, str);
        AnalyzerUtil.displayDetail(stopAnalyzer, str);
        AnalyzerUtil.displayDetail(whitespaceAnalyzer, str);
        AnalyzerUtil.displayDetail(standardAnalyzer, str);
    }

    @Test
    public void display() throws Exception {
        SimpleAnalyzer simpleAnalyzer = new SimpleAnalyzer();
        StopAnalyzer stopAnalyzer = new StopAnalyzer();
        WhitespaceAnalyzer whitespaceAnalyzer = new WhitespaceAnalyzer();
        StandardAnalyzer standardAnalyzer = new StandardAnalyzer();
        String str = "Hi mingming, I'm come from China. My e-mail is ccn@cnm.com and telephone number is 1622202";
        AnalyzerUtil.display(simpleAnalyzer, str);
        AnalyzerUtil.display(stopAnalyzer, str);
        AnalyzerUtil.display(whitespaceAnalyzer, str);
        AnalyzerUtil.display(standardAnalyzer, str);
    }

    @Test
    public void owner() throws IOException {
        ChinaAnalyzer chinaAnalyzer  = new ChinaAnalyzer(new String[]{"I", "you", "hate"});
        String str = "how are you thank you i hate you";
        AnalyzerUtil.display(chinaAnalyzer, str);
    }

    @Test
    public void ownerChina() throws IOException {
        MyMMsegAnalyzer mmsegAnalyzer = new MyMMsegAnalyzer(true);
        String str = "我来自中国,我居住在福建省建瓯市";
        AnalyzerUtil.display(mmsegAnalyzer, str);
        mmsegAnalyzer.reader.close();
    }

    @Test
    public void sameWord() throws IOException {
        Analyzer analyzer = new SameWordAnalyzer(new SameWordContextImpl());
        String str = "I love baby one";
        AnalyzerUtil.display(analyzer, str);
    }

}