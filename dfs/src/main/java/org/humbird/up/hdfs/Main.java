package org.humbird.up.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.humbird.up.hdfs.commons.Constant;
import org.humbird.up.hdfs.cores.HBaseJoy;
import org.humbird.up.hdfs.cores.IJoy;
import org.humbird.up.hdfs.cores.action.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by david on 16/9/8.
 */
public class Main {

    private final static Logger LOGGER = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        Initial initial = new Initial();
        IJoy joy;
        try {
            initial.init();
//            joy = new HdfsJoy();
//            joy = new RDBJoy();
//            long begin = System.currentTimeMillis();
//            joy.make(testOracleImport("dad"));
//            LOGGER.info("cost time: " + (System.currentTimeMillis() - begin) + " ms");
            joy = new HBaseJoy();
            joy.make(testHBaseCreate());
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage());
            initial.close();
            return;
        }
        initial.close();
    }

    private static IAction testImport() {
        File src = new File("/Users/david/Desktop/CETC/stargate/code/srv/pillar/servers");
        Path dest = new Path("/humbird/test");
        IAction<FileSystem, List<File>, Path> iAction;
        iAction = new ImportHdfsAction();
        iAction.getSources().add(src);
        iAction.setDest(dest);
        iAction.setType(Constant.HDFS_DIR_TYPE);
        return iAction;
    }

    private static IAction testExport() {
        Path src = new Path("/humbird/test");
        File dest = new File("/tmp/cccc");
        IAction<FileSystem, List<Path>, File> iAction;
        iAction = new ExportHdfsAction();
        iAction.getSources().add(src);
        iAction.setDest(dest);
        iAction.setType(Constant.HDFS_DIR_TYPE);
        return iAction;
    }

    private static IAction testDelete() {
        IAction<FileSystem, List<Path>, Path> iAction;
        iAction = new DeleteHdfsAction();
        iAction.getSources().add(new Path("/humbird"));
        return iAction;
    }

    private static IAction testMysqlImport(String name) {
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
        return iAction;
    }

    private static IAction testOracleImport(String name) {
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
        return iAction;
    }

    private static IAction testHBaseCreate() throws IOException {
        IAction<Connection, List<HTableDescriptor>, HTableDescriptor> iAction;
        iAction = new CreateHBaseAction();
//        iAction.setType("update");
        String nameSpace = "testH";
        for (int i = 0; i < 1; i++) {
            TableName tableName = TableName.valueOf(nameSpace, "testhb" + i);
            HTableDescriptor htd = new HTableDescriptor(tableName);
            HColumnDescriptor hcd = new HColumnDescriptor("people");
//            hcd.setInMemory(true);
            hcd.setTimeToLive(5184000);
            htd.addFamily(hcd);
            htd.setDurability(Durability.SYNC_WAL);
            iAction.getSources().add(htd);
        }
        return iAction;
    }
}
