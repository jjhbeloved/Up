package org.humbird.etl;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.humbird.up.hdfs.cores.HBaseJoy;
import org.humbird.up.hdfs.cores.action.CreateHBaseAction;
import org.humbird.up.hdfs.cores.action.IAction;
import org.humbird.up.hdfs.utils.HBaseTool;
import org.humbird.up.hdfs.vo.OHTableDescriptor;
import org.junit.Test;

import java.io.File;
import java.util.List;

/**
 * Created by david on 16/10/28.
 */
public class SogouLogETLTest extends BaseTest {

    public void createHBaseTable(HTableDescriptor htd) throws Exception {
        joy = new HBaseJoy();
        IAction<Connection, List<OHTableDescriptor>, OHTableDescriptor> iAction;
        iAction = new CreateHBaseAction();
        iAction.setType("update");
        HBaseTool.easyHTB(htd);
        OHTableDescriptor ohtd = new OHTableDescriptor(htd, htd, HBaseTool.getDecTimeSplit(4));
        iAction.getSources().add(ohtd);
        joy.make(iAction);
    }

    @Test
    public void pushSimpleTest() throws Exception {
        File file = new File("/install_apps/intelliJ_bak/BDX/dfs/src/main/resources/SogouQ.sample");
        String nameSpace = "sogou";
        TableName tableName = TableName.valueOf(nameSpace, "sogouMini");
        HTableDescriptor htd = new HTableDescriptor(tableName);
        HColumnDescriptor hcd = new HColumnDescriptor("logQuery");
        hcd.setInMemory(false);
        hcd.setTimeToLive(5184000);
        hcd.setBlockCacheEnabled(true);
        hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
        hcd.setCompressionType(Compression.Algorithm.LZ4);
        htd.addFamily(hcd);

        createHBaseTable(htd);
        joy = new HBaseJoy();
        SogouLogETL.pushSimple(file, joy, htd);
    }

    @Test
    public void pushDifficultTest() throws Exception {
        File file = new File("/Users/david/Downloads/SogouQ/access_log.20060831.decode.filter");
        String nameSpace = "sogou";
        TableName tableName = TableName.valueOf(nameSpace, "test");
        HTableDescriptor htd = new HTableDescriptor(tableName);
        HColumnDescriptor hcd = new HColumnDescriptor("logQuery");
        hcd.setInMemory(false);
        hcd.setTimeToLive(5184000);
        hcd.setBlockCacheEnabled(true);
        hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
        hcd.setCompressionType(Compression.Algorithm.LZ4);
        htd.addFamily(hcd);

        createHBaseTable(htd);
        joy = new HBaseJoy();
        SogouLogETL.pushDifficult(file, joy, htd);
    }

    @Test
    public void showTest() throws Exception {
        String nameSpace = "sogou";
//        TableName tableName = TableName.valueOf(nameSpace, "sogouMini");
        TableName tableName = TableName.valueOf(nameSpace, "test");
        HTableDescriptor htd = new HTableDescriptor(tableName);
        HColumnDescriptor hcd = new HColumnDescriptor("logQuery");
        hcd.setInMemory(false);
        hcd.setTimeToLive(5184000);
        hcd.setBlockCacheEnabled(true);
        hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
        hcd.setCompressionType(Compression.Algorithm.LZ4);
        htd.addFamily(hcd);
        joy = new HBaseJoy();
        SogouLogETL.show(joy, htd);
    }
}