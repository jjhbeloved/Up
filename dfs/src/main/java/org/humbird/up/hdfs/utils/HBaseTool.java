package org.humbird.up.hdfs.utils;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.List;

/**
 * Created by david on 16/9/12.
 */
public class HBaseTool {

    private final static Logger LOGGER = LogManager.getLogger(HBaseTool.class);

    public final static String UPDATE = "update";
    public final static String PUT = "put";
    public final static String DELETE = "delete";
    public final static String GET = "get";
    public final static String SCAN = "scan";

    public static void infoClusterStatus(ClusterStatus clusterStatus) {
        LOGGER.info("Master URL: " + clusterStatus.getMaster().getHostAndPort() + " -- ServerName: " + clusterStatus.getMaster().getServerName());
        clusterStatus.getBackupMasters().forEach(n -> {
            LOGGER.info("Backup URL: " + n.getHostAndPort() + " -- ServerName: " + n.getServerName());
        });
        LOGGER.info("BalanceOn: " + clusterStatus.getBalancerOn());
        LOGGER.info("RegionsCount: " + clusterStatus.getRegionsCount());
        clusterStatus.getServers().forEach(n -> {
            LOGGER.info("Server URL: " + n.getHostAndPort() + " -- ServerName: " + n.getServerName());
        });
    }

    public static void infoNamespaces(Admin admin) throws IOException {
        NamespaceDescriptor[] nds = admin.listNamespaceDescriptors();
        LOGGER.info("Namespaces: ========================");
        int i = 0;
        for (NamespaceDescriptor nd : nds) {
            LOGGER.info("# " + i + " # name: " + nd.getName());
            nd.getConfiguration().forEach((a, b) -> {
                LOGGER.info("key: " + a + ", value: " + b);
            });
            i++;
        }
    }

    public static void infoNamespace(Admin admin, String namespace) throws IOException {
        LOGGER.info("name: " + namespace);
        NamespaceDescriptor nd = admin.getNamespaceDescriptor(namespace);
        nd.getConfiguration().forEach((a, b) -> {
            LOGGER.info("key: " + a + ", value: " + b);
        });
    }

    public static void infoNamespaceTables(Admin admin, String namespace) throws IOException {
        LOGGER.info("name: " + namespace);
        HTableDescriptor[] htds = admin.listTableDescriptorsByNamespace(namespace);
        for (HTableDescriptor htd : htds) {
            LOGGER.info("table name: " + htd.getTableName().getQualifierAsString());
        }
    }

    public static void infoTables(Admin admin) throws IOException {
        TableName[] tableNames = admin.listTableNames();
        for (int i = 0; i < tableNames.length; i++) {
            LOGGER.info(tableNames[i].getNameAsString());
        }
    }

    public static Calendar getTime(int year, int month, int date, int hourOfDay, int minute,
                                   int second) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(year, month, date, hourOfDay, minute, second);
        return calendar;
    }

    /**
     * 16进制转换
     *
     * @param startKey   高位
     * @param endKey     低位
     * @param numRegions 分区
     * @return
     */
    public static byte[][] getHexSplits(String startKey, String endKey, int numRegions) {
        byte[][] splits = new byte[numRegions - 1][];
        BigInteger lowestKey = new BigInteger(startKey, 16);
        BigInteger highestKey = new BigInteger(endKey, 16);
        BigInteger range = highestKey.subtract(lowestKey);
        BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
        lowestKey = lowestKey.add(regionIncrement);
        for (int i = 0; i < numRegions - 1; i++) {
            BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
            byte[] b = String.format("%016x", key).getBytes();
            splits[i] = b;
        }
        return splits;
    }

    /**
     * 10进制转换
     *
     * @param startKey   高位
     * @param endKey     低位
     * @param numRegions 分区
     * @return
     */
    public static byte[][] getDecSplits(String prefix, String startKey, String endKey, int numRegions) {
        byte[][] splits = new byte[numRegions - 1][];
        BigInteger lowestKey = new BigInteger(startKey, 10);
        BigInteger highestKey = new BigInteger(endKey, 10);
        BigInteger range = highestKey.subtract(lowestKey);
        BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
        lowestKey = lowestKey.add(regionIncrement);
        for (int i = 0; i < numRegions - 1; i++) {
            BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
            byte[] b = (prefix + key.toString()).getBytes();
            splits[i] = b;
        }
        return splits;
    }

    /**
     * @param regionStartKeyList 高位
     * @return
     */
    public static byte[][] getHexSplits(List<String> regionStartKeyList) {
        if (regionStartKeyList == null || regionStartKeyList.size() == 0) {
            return getHexSplits("0", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);
        } else {
            byte[][] splits = new byte[regionStartKeyList.size()][];
            for (int i = 0; i < regionStartKeyList.size(); i++) {
                BigInteger key = new BigInteger(regionStartKeyList.get(i), 16);
                byte[] b = String.format("%016x", key).getBytes();
                splits[i] = b;
            }
            return splits;
        }
    }

    /**
     * 十进制时间rowkey
     *
     * @param regionNum 分区数
     * @return
     */
    public static byte[][] getDecTimeSplit(int regionNum) {
        String startKey = String.valueOf(System.currentTimeMillis());
        Calendar calendar = getTime(2020, 12, 31, 23, 59, 59);
        String endKey = String.valueOf(calendar.getTimeInMillis());

//        return getDecSplits("aaaa", startKey, "zzzz" + endKey, regionNum);
        return null;
    }

    public static byte[] getRowKeyByTime(String key) {
        return String.valueOf(System.currentTimeMillis()).getBytes();
    }

    public static byte[] getRowKeyByTime(String key, int count) {
        return (String.valueOf(System.currentTimeMillis()) + count).getBytes();
    }

    /**
     * 快速优化 HCD
     *
     * @param hcd
     */
    public static void easyHCD(HColumnDescriptor hcd) {
        hcd.setInMemory(true);
        hcd.setTimeToLive(5184000);
        hcd.setBlockCacheEnabled(true);
        hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
        hcd.setCompressionType(Compression.Algorithm.SNAPPY);
    }

    /**
     * 快速优化 HTD
     *
     * @param htd
     */
    public static void easyHTB(HTableDescriptor htd) {
        htd.setDurability(Durability.SYNC_WAL);
        htd.setRegionSplitPolicyClassName("org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy");
        htd.setRegionReplication(8);
    }
}
