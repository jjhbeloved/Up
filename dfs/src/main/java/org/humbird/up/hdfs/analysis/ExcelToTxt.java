package org.humbird.up.hdfs.analysis;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;

/**
 * Created by david on 16/9/27.
 */
public class ExcelToTxt {

    public static HSSFSheet readExcel(File source, String sheetName) throws IOException {
        InputStream is = new FileInputStream(source);
        HSSFWorkbook hssfWorkbook = new HSSFWorkbook(is);
        return hssfWorkbook.getSheet(sheetName);
    }

    public static String getValue(HSSFCell hssfCell) {
        if (hssfCell.getCellType() == hssfCell.CELL_TYPE_BOOLEAN) {
            // 返回布尔类型的值
            return String.valueOf(hssfCell.getBooleanCellValue());
        } else if (hssfCell.getCellType() == hssfCell.CELL_TYPE_NUMERIC) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            if(HSSFDateUtil.isCellDateFormatted(hssfCell)) {
                return sdf.format(HSSFDateUtil.getJavaDate(hssfCell.getNumericCellValue())).toString();
            }
            // 返回数值类型的值
            return String.valueOf(hssfCell.getNumericCellValue());
        } else if (hssfCell.getCellType() == hssfCell.CELL_TYPE_FORMULA) {
            String strCell;
            try {
                strCell = String.valueOf(hssfCell.getStringCellValue());
            } catch (IllegalStateException e) {
                strCell = String.valueOf(hssfCell.getNumericCellValue());
            }
            return strCell;
        } else {
            // 返回字符串类型的值
            return String.valueOf(hssfCell.getStringCellValue());
        }
    }

}
