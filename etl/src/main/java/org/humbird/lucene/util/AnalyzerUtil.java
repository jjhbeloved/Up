package org.humbird.lucene.util;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

import java.io.IOException;
import java.io.StringReader;

/**
 * Created by david on 16/11/13.
 */
public class AnalyzerUtil {

    public static void display(Analyzer analyzer, String str) throws IOException {
        TokenStream tokenStream = analyzer.tokenStream("content", new StringReader(str));
        CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
        tokenStream.reset();
        while (tokenStream.incrementToken()) {
            System.out.print("[" + charTermAttribute + "]");
        }
        System.out.println("================");
    }

    public static void displayDetail(Analyzer analyzer, String str) throws IOException {
        TokenStream tokenStream = analyzer.tokenStream("content", new StringReader(str));
        // 字符串查看器
        CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
        // 位置查看器
        PositionIncrementAttribute positionIncrementAttribute = tokenStream.addAttribute(PositionIncrementAttribute.class);
        // 偏移量查看器
        OffsetAttribute offsetAttribute = tokenStream.addAttribute(OffsetAttribute.class);
        // 类型查看器
        TypeAttribute typeAttribute = tokenStream.addAttribute(TypeAttribute.class);
        tokenStream.reset();
        while (tokenStream.incrementToken()) {
            System.out.print(positionIncrementAttribute.getPositionIncrement() + ":");
            System.out.println(charTermAttribute + "[" + offsetAttribute.startOffset() + "-" + offsetAttribute.endOffset() + "]-->" + typeAttribute.type());
        }
        System.out.println();
        System.out.println("================");
    }

}
