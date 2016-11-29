package org.humbird.etl;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.humbird.up.hdfs.cores.IJoy;
import org.humbird.up.hdfs.cores.action.DDLHBaseAction;
import org.humbird.up.hdfs.cores.action.IAction;
import org.humbird.up.hdfs.utils.HBaseTool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.List;
import java.util.NavigableMap;

/**
 * Created by david on 16/10/28.
 */
public class SogouLogETL {

    static class UserQueryLog {
        public Date date;
        public String uid;
        public String word;
        public long top;
        public int sort;
        public String url;
    }

    public static void pushSimple(File file, IJoy joy, HTableDescriptor htd) throws Exception {
//        DateFormat format = new SimpleDateFormat("HH:mm:ss");
//        userQueryLog.date = format.parse(words[0]);

        String encoding = "GB18030";
        IAction<Connection, List, TableName> iAction;
        iAction = new DDLHBaseAction();
        List<Put> puts = (List<Put>) iAction.getSources();
        byte[] col = htd.getColumnFamilies()[0].getName();

        // 1. read file
        InputStreamReader read = null;
        try {
            if (file.isFile() && file.exists()) { //判断文件是否存在
                read = new InputStreamReader(
                        new FileInputStream(file), encoding);//考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    String[] words = line.split("\t");
                    if (words.length == 5) {
//                        UserQueryLog userQueryLog = new UserQueryLog();
                        String[] topSort = words[3].split(" ");
                        Put put = new Put(HBaseTool.getRowKeyByTime("random"));
                        put.addColumn(col, "date".getBytes(), words[0].getBytes());
                        put.addColumn(col, "uid".getBytes(), words[1].getBytes());
                        put.addColumn(col, "word".getBytes(), words[2].getBytes());
                        put.addColumn(col, "top".getBytes(), topSort[0].getBytes());
                        put.addColumn(col, "sort".getBytes(), topSort[1].getBytes());
                        put.addColumn(col, "url".getBytes(), words[4].getBytes());
                        puts.add(put);
                    }
                }
                iAction.setDest(htd.getTableName());
                iAction.setType(HBaseTool.PUT);
                joy.make(iAction);
            }
        } finally {
            if (read != null) {
                read.close();
            }
        }
    }

    public static void pushDifficult(File file, IJoy joy, HTableDescriptor htd) throws Exception {
        String encoding = "GB18030";
        IAction<Connection, List, TableName> iAction;
        iAction = new DDLHBaseAction();
        List<Put> puts = (List<Put>) iAction.getSources();
        byte[] col = htd.getColumnFamilies()[0].getName();

        // 1. read file
        InputStreamReader read = null;
        try {
            if (file.isFile() && file.exists()) { //判断文件是否存在
                read = new InputStreamReader(
                        new FileInputStream(file), encoding);//考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String line;
                int i = 0;
                while ((line = bufferedReader.readLine()) != null) {
                    String[] words = line.split("\t");
                    if (words.length == 4) {
                        String[] topSort = words[2].split(" ");
                        Put put = new Put(HBaseTool.getRowKeyByTime("random", i));
                        String[] names = file.getName().split("\\.");
                        put.addColumn(col, "date".getBytes(), names[1].getBytes());
                        put.addColumn(col, "uid".getBytes(), words[1].getBytes());
                        put.addColumn(col, "word".getBytes(), words[2].getBytes());
                        put.addColumn(col, "top".getBytes(), topSort[0].getBytes());
                        put.addColumn(col, "sort".getBytes(), topSort[1].getBytes());
                        put.addColumn(col, "url".getBytes(), words[3].getBytes());
                        puts.add(put);
                        i++;
                    } else {
                        System.out.println(line);
                    }
                }
                iAction.setDest(htd.getTableName());
                iAction.setType(HBaseTool.PUT);
                joy.make(iAction);
                System.out.println(i);
            }
        } finally {
            if (read != null) {
                read.close();
            }
        }
    }

    public static void show(IJoy joy, HTableDescriptor htd) throws Exception {
        IAction<Connection, List, TableName> iAction;
        iAction = new DDLHBaseAction();
        List<Get> gets = (List<Get>) iAction.getSources();
        iAction.setDest(htd.getTableName());
        iAction.setType(HBaseTool.SCAN);
        List<Result> results = (List<Result>) joy.happy(iAction);
        int i = 0;
        for (Result result : results) {
            System.out.println(new String(result.getRow()));
            byte[] col = htd.getColumnFamilies()[0].getName();
            NavigableMap<byte[], byte[]> navigableMap = result.getFamilyMap(col);
            navigableMap.forEach((k, v) -> {
                System.out.println("\t(key:" + new String(k) + ", val:" + new String(v) + ")");
            });
            i++;
        }
        System.out.println(i);
    }
}