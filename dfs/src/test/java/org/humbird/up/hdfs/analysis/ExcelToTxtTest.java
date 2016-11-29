package org.humbird.up.hdfs.analysis;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

/**
 * Created by david on 16/9/27.
 */
public class ExcelToTxtTest {
    @Test
    public void trans() throws Exception {
        File source = new File("/install_apps/api/cetc/Up/dfs/src/main/resources/analysis/1991_2016.xls");
        File dest = new File("/install_apps/api/cetc/Up/dfs/src/main/resources/analysis/1991_2016.txt");
        HSSFSheet rows = ExcelToTxt.readExcel(source, "easy");
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(dest));
        int colNum = 8;
        int rowNum = 2;
        int commits = 1000;

        StringBuilder allRows = new StringBuilder();
        for (int i = rowNum; i < rows.getLastRowNum(); i++) {
            StringBuilder rowStr = new StringBuilder();
            for (int j = 0; j < colNum; j++) {
                HSSFCell cell = rows.getRow(i).getCell(j);
                String val;
                if (cell == null) {
                    val = "###";
                } else {
                    val = ExcelToTxt.getValue(cell);
//                    val = cell.toString();
                    if (val.startsWith("#")) {
                        rowStr.append("###");
                        val = "###";
                    }
                }
                rowStr.append(val).append("\t");
                if (j == colNum - 1) {
                    rowStr.append("\n");
                }
            }
            allRows.append(rowStr);
            if (i + 1 % commits == 0) {
                bufferedWriter.write(allRows.toString());
                allRows.setLength(0);
            }
        }
        if (allRows.length() > 0) {
            bufferedWriter.write(allRows.toString());
            allRows.setLength(0);
        }
    }

}