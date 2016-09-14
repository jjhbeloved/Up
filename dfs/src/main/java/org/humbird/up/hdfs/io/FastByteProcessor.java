package org.humbird.up.hdfs.io;

import com.google.common.io.ByteProcessor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Created by david on 16/9/10.
 */
public class FastByteProcessor<T> implements ByteProcessor {

    @Override
    public boolean processBytes(byte[] bytes, int from, int to) throws IOException {
        System.out.println("length:" + bytes.length + ", begin: " + from + ", end: " + to);
        if (to != 8192) {
            byte[] tmp = Arrays.copyOf(bytes, to-1);
            System.out.println(new String(tmp, Charset.defaultCharset()));
        } else {
            System.out.println(new String(bytes, Charset.defaultCharset()));
        }
        System.out.println("==------==");
        return true;
    }

    @Override
    public T getResult() {
        return null;
    }
}
