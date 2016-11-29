package org.humbird.lucene.token;

import com.chenlb.mmseg4j.MMSeg;
import com.chenlb.mmseg4j.Seg;
import com.chenlb.mmseg4j.Word;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

import java.io.IOException;
import java.io.Reader;

/**
 * Created by david on 16/11/14.
 */
public class MyMMsegTokenizer extends Tokenizer {
    private MMSeg mmSeg;
    private CharTermAttribute termAtt;
    private OffsetAttribute offsetAtt;
    private TypeAttribute typeAtt;

    public MyMMsegTokenizer(Seg seg, Reader reader) {
//        super(input);
        this.mmSeg = new MMSeg(reader, seg);
        this.termAtt = this.addAttribute(CharTermAttribute.class);
        this.offsetAtt = this.addAttribute(OffsetAttribute.class);
        this.typeAtt = this.addAttribute(TypeAttribute.class);
    }

    public void reset() throws IOException {
        this.mmSeg.reset(this.input);
    }

    public final boolean incrementToken() throws IOException {
        this.clearAttributes();
        Word word = this.mmSeg.next();
        if(word != null) {
            this.termAtt.copyBuffer(word.getSen(), word.getWordOffset(), word.getLength());
            this.offsetAtt.setOffset(word.getStartOffset(), word.getEndOffset());
            this.typeAtt.setType(word.getType());
            return true;
        } else {
            this.end();
            return false;
        }
    }

}
