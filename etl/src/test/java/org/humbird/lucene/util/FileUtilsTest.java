package org.humbird.lucene.util;

import org.junit.Test;

import java.io.File;

/**
 * Created by david on 16/11/18.
 */
public class FileUtilsTest {

    @Test
    public void tikaFileToStr() throws Exception {
//        String str = FileUtils.tikaFileToStr(new File("/Users/david/ceph2_david.cer"));
        String str = FileUtils.tikaFileToStr(new File("/Users/david/Downloads/c.doc"));
        System.out.println(str);
    }

    @Test
    public void fileToStr() throws Exception {
        String str = FileUtils.fileToStr(new File("/Users/david/Downloads/c.doc"));
        System.out.println(str);
    }

}