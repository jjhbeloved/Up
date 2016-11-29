package org.humbird.up.hdfs.utils;

/**
 * Created by david on 16/9/18.
 */
public class StringTool {

    @Deprecated
    public static String reverse(String str) {
        return new StringBuffer(str).reverse().toString();
    }

    public static String reverseC(String str) {
        char[] s = str.toCharArray();
        int n = s.length - 1;
        int halfLength = n / 2;
        for (int i = 0; i <= halfLength; i++) {
            char temp = s[i];
            s[i] = s[n - i];
            s[n - i] = temp;
        }
        return new String(s);
    }

}
