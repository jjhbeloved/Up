package org.humbird.lucene.filter;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.AttributeSource;
import org.humbird.lucene.context.SameWordContext;

import java.io.IOException;
import java.util.Stack;

/**
 * Created by david on 16/11/14.
 */
public class SameWordTokenFilter extends TokenFilter {

    // 存储分词数据
    private CharTermAttribute cta = null;
    // 存储语汇单元的位置信息
    private PositionIncrementAttribute pia = null;
    // 添加是否有同义词的判断变量属性,保存当前元素的状态信息
    private AttributeSource.State current;
    // 栈存储
    private Stack<String> sames = null;
    private SameWordContext sameContxt;

    public SameWordTokenFilter(TokenStream input, SameWordContext sameContxt) {
        super(input);
        cta = this.addAttribute(CharTermAttribute.class);
        pia = this.addAttribute(PositionIncrementAttribute.class);
        sames = new Stack();
        this.sameContxt = sameContxt;
    }

    @Override
    public final boolean incrementToken() throws IOException {
        while (sames.size() > 0) {
            String str = sames.pop();
            restoreState(current);
            cta.setEmpty();
            cta.append(str);
            pia.setPositionIncrement(0);
            return true;
        }

        if (!this.input.incrementToken()) {
            return false;
        }
        if (addSame(cta.toString())) {
            current = captureState();
        }
        return true;
    }

    private boolean addSame(String name) {
        String[] sws = sameContxt.getSameWords(name);
        if (sws != null) {
            // 添加进栈中
            for (String str : sws) {
                sames.push(str);
            }
            return true;
        }
        return false;
    }
}
