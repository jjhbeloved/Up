package org.humbird.railway;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.poi.hssf.usermodel.*;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.humbird.up.hdfs.utils.HBaseTool;

import java.io.*;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;

/**
 * Created by david on 16/11/2.
 */
public class RailwayBulk {
    public final static Configuration SOURCE_HDFS_CONF = new Configuration();
    public final static Configuration DEST_HDFS_CONF = new Configuration();

    public static void setup() throws FileNotFoundException {
        String path = "/Users/david/Desktop/CETC/railway/hbase_test/";
//        String path = "/Users/david/Desktop/CETC/railway/hbase_test/";
        InputStream inputStream2 = new FileInputStream(path + "source_hbase-site.xml");
        SOURCE_HDFS_CONF.clear();
        SOURCE_HDFS_CONF.addResource(inputStream2);
        InputStream inputStream3 = new FileInputStream(path + "dest_hbase-site.xml");
        DEST_HDFS_CONF.clear();
        DEST_HDFS_CONF.addResource(inputStream3);
        System.getProperties().put("HADOOP_USER_NAME", "hbase");
    }

    public static void main(String[] args) throws IOException {
        setup();
//        imported2();
//        readXls();
//        read();
        chouqu();
    }

    public static void imported() throws IOException {
        File file = new File("/Users/david/Desktop/CETC/railway/hbase_bak/tables/alert-definition.txt");
        BufferedWriter out = new BufferedWriter(new FileWriter(file, false));
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(SOURCE_HDFS_CONF);
            HBaseTool.infoNamespaceTables(connection.getAdmin(), "default");
            HBaseTool.infoNamespaceTables(connection.getAdmin(), "hbase");

            String nameSpace = "default";
            TableName tableName = TableName.valueOf(nameSpace, "alert-definition");
            HTableDescriptor htd = new HTableDescriptor(tableName);
            HColumnDescriptor hcd = new HColumnDescriptor("meta");
            hcd.setInMemory(false);
            hcd.setTimeToLive(5184000);
            hcd.setBlockCacheEnabled(true);
            htd.addFamily(hcd);

            TableName dest = htd.getTableName();
            Table table = connection.getTable(dest);
            ResultScanner scanner = table.getScanner(new Scan());
            StringBuilder stringBuilder = new StringBuilder();
            int i = 1;
            for (Result result : scanner) {
                NavigableMap<byte[], byte[]> navigableMap = result.getFamilyMap("meta".getBytes());
                navigableMap.forEach((k, v) -> {
                    stringBuilder.append(new String(k) + ":###:" + new String(v) + "#:::#");
                });
                String o = stringBuilder.toString();
                System.out.println(o);
                out.write(stringBuilder.toString());
                out.newLine();
                if (i % 10000 == 0) {
                    out.flush();
                    System.out.println(" -- flush -- ");
                }
            }
            out.flush();
        } finally {
            if (connection != null) {
                connection.close();
            }
            out.close();
        }
    }

    public static void imported2() throws IOException {

        String tName = "tsdb";
        String path = "/Users/david/Desktop/CETC/railway/hbase_bak/tables/";
        List<String> sheets = new LinkedList();
        List<String> names = new LinkedList();

        Connection connection = null;
        int z = 0;
        int sum = 0;
        try {
            connection = ConnectionFactory.createConnection(SOURCE_HDFS_CONF);
            String nameSpace = "default";
            TableName tableName = TableName.valueOf(nameSpace, tName);
            HTableDescriptor htd = new HTableDescriptor(tableName);

            TableName dest = htd.getTableName();
            Table table = connection.getTable(dest);
            HColumnDescriptor[] hColumnDescriptors = table.getTableDescriptor().getColumnFamilies();
            for (HColumnDescriptor hcd : hColumnDescriptors) {
                sheets.add(hcd.getNameAsString());
            }
            HSSFWorkbook wb = createXls(sheets, names);
            ResultScanner scanner = table.getScanner(new Scan());
            int i = 1;
            for (Result result : scanner) {
                for (String sheetName : sheets) {
                    HSSFSheet sheet = wb.getSheet(sheetName);
                    final HSSFRow row = sheet.createRow(i);
                    NavigableMap<byte[], byte[]> navigableMap = result.getFamilyMap(sheetName.getBytes());
                    final int[] j = {0};
                    navigableMap.forEach((k, v) -> {
                        row.createCell(j[0]++).setCellValue(new String(k));
                        row.createCell(j[0]++).setCellValue(new String(v));
                    });
                }
                i++;
                if (i % 1000 == 0) {
                    File file = new File(path + tName + (z++) + ".xls");
                    OutputStream os = new FileOutputStream(file, false);
                    wb.write(os);
                    os.flush();
                    os.close();
                    System.out.println(" -- flush -- ");
                    wb = createXls(sheets, names);
                    i = 1;
                }
                sum++;
                if (sum > 10000) {
                    break;
                }
            }
            if (i != 1) {
                File file = new File(path + tName + (z++) + ".xls");
                OutputStream os = new FileOutputStream(file, false);
                wb.write(os);
                os.flush();
                os.close();
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    public static void read() throws IOException {
        File file = new File("/Users/david/Desktop/CETC/railway/hbase_bak/tables/alert-definition.txt");
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            System.out.println("===> " + line);
        }
    }

    public static HSSFWorkbook createXls(List<String> sheets, List<String> cellNames) {
        // 声明一个工作薄
        HSSFWorkbook wb = new HSSFWorkbook();

        for (String sheetName : sheets) {
            //声明一个单子并命名
            HSSFSheet sheet = wb.createSheet(sheetName);
            //给单子名称一个长度
            sheet.setDefaultColumnWidth((short) 15);
            //创建第一行（也可以称为表头）
            HSSFRow row = sheet.createRow(0);
            // 生成一个样式
            HSSFCellStyle style = wb.createCellStyle();
            //样式字体居中
            style.setAlignment(HSSFCellStyle.ALIGN_CENTER);
            //给表头第一行一次创建单元格
//            int i = 0;
//            for (String name : cellNames) {
//                HSSFCell cell = row.createCell(i++);
//                cell.setCellValue(sheetName);
//                cell.setCellStyle(style);
//            }
        }
        return wb;
    }

    public static void readXls() throws IOException {
        HSSFWorkbook wb = new HSSFWorkbook(new FileInputStream("/Users/david/Desktop/CETC/railway/hbase_bak/tables/alert-status.xls"));
        HSSFSheet sheet = wb.getSheet("alert-status");
        //迭代行
        for (Iterator<Row> iter = sheet.rowIterator(); iter.hasNext(); ) {
            Row row = iter.next();
            //迭代列
            for (Iterator<Cell> iter2 = row.cellIterator(); iter2.hasNext(); ) {
                Cell cell = iter2.next();
                //用于测试的文件就2列，第一列为数字，第二列为字符串
                //对于数字cell.getCellType的值为HSSFCell.CELL_TYPE_NUMERIC，为0
                //对于字符串cell.getCellType的值为HSSFCell.CELL_TYPE_STRING，为1
                //完整的类型列表请查看api
                String content = cell.getCellType() == HSSFCell.CELL_TYPE_NUMERIC ? cell.getNumericCellValue() + "" : cell.getStringCellValue();
                System.out.println(content);
            }
        }
    }

    public static HTableDescriptor[] listTablesByNamespace(Admin admin, String namespace) throws IOException {
        return admin.listTableDescriptorsByNamespace(namespace);
    }

    public static void createNamepsace(Admin admin, String namespace) {
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (IOException e) {
            System.out.println("WARN: " + e.getMessage());
        }
    }

    public static void createTableModel(HTableDescriptor[] hTableDescriptors, Connection destConn, String destNamespace) throws IOException {
        Admin admin = destConn.getAdmin();
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            TableName tableName = hTableDescriptor.getTableName();
            TableName destTableName = TableName.valueOf(destNamespace, tableName.getNameAsString());
            if (!admin.tableExists(destTableName)) {
                hTableDescriptor.setName(destTableName);
                admin.createTable(hTableDescriptor);
            }
        }
    }

//    public static void importSource

    public static void chouqu() {
//        String tName = "tsdb";
        Connection connection = null;
        Connection destConn = null;
        try {

            connection = ConnectionFactory.createConnection(SOURCE_HDFS_CONF);
            destConn = ConnectionFactory.createConnection(DEST_HDFS_CONF);
            String nameSpace = "default";
            String destNameSpace = "partner";

            // 创建 目标命名空间
            createNamepsace(destConn.getAdmin(), destNameSpace);
            // 列出 源库指定命名空间的表
            HTableDescriptor[] hTableDescriptors = listTablesByNamespace(connection.getAdmin(), nameSpace);
            // 创建 数据模型
            createTableModel(hTableDescriptors, destConn, destNameSpace);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
