package org.humbird.lucene.context;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by david on 16/11/15.
 */
public class SameWordContextImpl implements SameWordContext {

    /*
     * 实现同义词接口
     */
    Map<String, String[]> maps = new HashMap<String, String[]>();


    public SameWordContextImpl() {
        maps.put("I", new String[] { "We", "Our" });
        maps.put("baby", new String[] { "honey", "darling" });
    }

    @Override
    public String[] getSameWords(String name) {
        return maps.get(name);
    }
}
