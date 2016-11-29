package org.humbird.up.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.humbird.up.hdfs.commons.Constant;
import org.humbird.up.hdfs.cores.HBaseJoy;
import org.humbird.up.hdfs.cores.HdfsJoy;
import org.humbird.up.hdfs.cores.RDBJoy;
import org.humbird.up.hdfs.cores.action.*;
import org.humbird.up.hdfs.utils.HBaseTool;
import org.humbird.up.hdfs.vo.OHTableDescriptor;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by david on 16/9/18.
 */
public class JoyTest extends BaseTest {

    private final static Logger LOGGER = LogManager.getLogger(JoyTest.class);

    @Test
    public void testHdfsJoyImport() throws Exception {
        joy = new HdfsJoy();
        File src = new File("/install_apps/api/cetc/Up/dfs/src/main/resources/analysis");
        Path dest = new Path("/humbird/test");
        IAction<FileSystem, List<File>, Path> iAction;
        iAction = new ImportHdfsAction();
        iAction.getSources().add(src);
        iAction.setDest(dest);
        iAction.setType(Constant.HDFS_DIR_TYPE);
        joy.make(iAction);
    }

    @Test
    public void testHdfsJoyMySQLExport() throws Exception {
        joy = new RDBJoy();
        Path src = new Path("/humbird/test");
        File dest = new File("/tmp/cccc");
        IAction<FileSystem, List<Path>, File> iAction;
        iAction = new ExportHdfsAction();
        iAction.getSources().add(src);
        iAction.setDest(dest);
        iAction.setType(Constant.HDFS_DIR_TYPE);
        joy.make(iAction);
    }

    @Test
    public void testHdfsJoyDelete() throws Exception {
        joy = new RDBJoy();
        IAction<FileSystem, List<Path>, Path> iAction;
        iAction = new DeleteHdfsAction();
        iAction.getSources().add(new Path("/humbird"));
        joy.make(iAction);
    }

    @Test
    public void testRDBJoyMySQLImport() throws Exception {
        String name = "dad";
        joy = new RDBJoy();
        IAction<Session, List<Object>, Object> iAction;
        iAction = new ImportRDBAction();
        int count = 1000;
        for (int i = 0; i < count; i++) {
            List params = new ArrayList();
            params.add(20);
            params.add("1990-01-23");
            params.add(name + " is over... the world is over....");
            params.add(name + "_" + i);
            params.add((byte) 1);
            iAction.getSources().add(params);
        }
        iAction.setDest("insert into t_people_1000 (age, birthday, comments, name, sex) values (?, ?, ?, ?, ?)");
        joy.make(iAction);
    }

    @Test
    public void testRDBJoyOracleImport() throws Exception {
        String name = "dad";
        joy = new RDBJoy();
        IAction<Session, List<Object>, Object> iAction;
        iAction = new ImportRDBAction();
        int count = 1000;
        for (int i = 0; i < count; i++) {
            List params = new ArrayList();
            params.add(20);
            params.add("1990-01-23");
            params.add(name + " is over... the world is over....");
            params.add(name + "_" + i);
            params.add(1);
            iAction.getSources().add(params);
        }
        iAction.setDest("insert into t_people_1000 (id, age, birthday, comments, name, sex) values (PEOPLESEQ.nextval, ?, ?, ?, ?, ?)");
//        OPeople oPeople = new OPeople();
//        oPeople.setName(name);
//        oPeople.setAge(20);
//        oPeople.setSex("1");
//        iAction.setDest(oPeople);
        joy.make(iAction);
    }

    @Test
    public void testHBaseJoyCreate() throws Exception {
        joy = new HBaseJoy();
        IAction<Connection, List<OHTableDescriptor>, OHTableDescriptor> iAction;
        iAction = new CreateHBaseAction();
        iAction.setType("update");
        String nameSpace = "xxx";
        for (int i = 0; i < 1; i++) {
            TableName tableName = TableName.valueOf(nameSpace, "test2hb" + i);
            HTableDescriptor htd = new HTableDescriptor(tableName);
            HColumnDescriptor hcd = new HColumnDescriptor("human");
            HBaseTool.easyHCD(hcd);
            htd.addFamily(hcd);
            HBaseTool.easyHTB(htd);

            TableName ntableName = TableName.valueOf("xxx", "test2hb" + i);
            HTableDescriptor nhtd = new HTableDescriptor(ntableName);
            HColumnDescriptor nhcd = new HColumnDescriptor("human");
            hcd.setInMemory(false);
            hcd.setTimeToLive(5184000);
            hcd.setBlockCacheEnabled(true);
            hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
            hcd.setCompressionType(Compression.Algorithm.LZ4);
            nhtd.addFamily(nhcd);
            HBaseTool.easyHTB(nhtd);
            OHTableDescriptor ohtd = new OHTableDescriptor(htd, nhtd, HBaseTool.getDecTimeSplit(4));
            iAction.getSources().add(ohtd);
        }
//        RegionSplitter.SplitAlgorithm splitAlgorithm  = RegionSplitter.newSplitAlgoInstance();
//        splitAlgorithm.split()
        joy.make(iAction);
    }

    @Test
    public void testHBaseJoyDrop() throws Exception {
        joy = new HBaseJoy();
        IAction<Connection, List<HTableDescriptor>, HTableDescriptor> iAction;
        iAction = new DropHBaseAction();
        String nameSpace = "xxx";
        TableName tableName = TableName.valueOf(nameSpace, "test2hb0");
        HTableDescriptor htd = new HTableDescriptor(tableName);
        iAction.setDest(htd);
        joy.make(iAction);
    }

    @Test
    public void testHBaseJoyImport() throws Exception {
        joy = new HBaseJoy();
        IAction<Connection, List, TableName> iAction;
        iAction = new DDLHBaseAction();
        TableName tableName = TableName.valueOf("xxx", "test2hb0");
        List<Put> puts = (List<Put>) iAction.getSources();
        for (int i = 0; i < 1000; i++) {
            Put put = new Put(HBaseTool.getRowKeyByTime("abc"));
            put.addColumn("god".getBytes(), "name".getBytes(), "tester".getBytes());
            put.addColumn("god".getBytes(), "sex".getBytes(), "L".getBytes());
            put.addColumn("god".getBytes(), "age".getBytes(), "20".getBytes());
            puts.add(put);
        }
        iAction.setDest(tableName);
        iAction.setType(HBaseTool.PUT);
        long begin = System.currentTimeMillis();
        joy.make(iAction);
        LOGGER.info("datetime => " + (System.currentTimeMillis() - begin));
    }

    @Test
    public void testQuickHBase() throws Exception {
        testHBaseJoyDrop();
        testHBaseJoyCreate();
        testHBaseJoyImport();
    }

}
