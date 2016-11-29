package org.humbird.up.hdfs.vo;

import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * Created by david on 16/9/18.
 */
public class OHTableDescriptor {
    // old
    private HTableDescriptor oHTableDescriptor;
    // new
    private HTableDescriptor nHTableDescriptor;

    private byte[][] splits;

    public OHTableDescriptor() {
    }

    public OHTableDescriptor(HTableDescriptor oHTableDescriptor, HTableDescriptor nHTableDescriptor, byte[][] splits) {
        this.oHTableDescriptor = oHTableDescriptor;
        this.nHTableDescriptor = nHTableDescriptor;
        this.splits = splits;
    }

    public HTableDescriptor getoHTableDescriptor() {
        return oHTableDescriptor;
    }


    public HTableDescriptor getnHTableDescriptor() {
        return nHTableDescriptor;
    }


    public byte[][] getSplits() {
        return splits;
    }

}
