package eventDetector.drawing;

import java.io.IOException;

import java.io.FileOutputStream;
import java.util.Date;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import topologyBuilder.Constants;

/**
 * A very simple program that writes some data to an Excel file
 * using the Apache POI library.
 * @author www.codejava.net
 *
 */
public class ExcelWriter {

    private static int[][] times;
    private static Date startTime = new Date();
    private static long startRound = 0;
    private static String fileNum="12345";
    private static int lastInd ;
    private static int rowNum = 7200;
    private static int columnNum = 200;
    private static int numOfBolts = 13;
    private static boolean createChart = false;

    public static void putStartDate(Date date, String filenum, long round) {
        times = new int[rowNum][columnNum];
        lastInd = rowNum -1;
        startTime = date;
        startRound = round;
        for(int i = 0; i< rowNum; i++) {
            times[i][0] = i;
            for (int j = 1; j < columnNum; j++)
                times[i][j] = 0;
        }

        fileNum = filenum +"/";
    }

    public static void putData(int id, Date boltStartTime, Date boltEndTime, String boltName, String country, long round) {

        long timeStart = (boltStartTime.getTime()-startTime.getTime())/1000;
        long duration = (boltEndTime.getTime()-boltStartTime.getTime())/1000;

        if(duration==0) duration = 1;

        while (duration-->0)
            times[(int) timeStart++][id + numOfBolts * ((int) ((round - startRound)/2) % 10) ] = id;


//        System.out.println( "Key: " + id + ", name: " + boltName + ", country: " + country );
    }

    public static void createTimeChart () throws IOException {

        if(!createChart) {
            createChart = true;
            System.out.println("Excel creation started");
            writeExcel();
        }
    }


    public static void clean () {
        for(int i = rowNum -1; i>=0; i--) {
            for(int j = 1; j<columnNum; j++) {
                if(times[i][j] != 0) {
                    lastInd = i+1;
                    return;
                }
            }
        }
    }

    public static void writeExcel() throws IOException {
        SXSSFWorkbook workbook = new SXSSFWorkbook();
        Sheet sheet = workbook.createSheet();
        clean();

        for (int i=0;i<lastInd;i++) {
            Row row = sheet.createRow(i);
            for (int j = 0; j<columnNum; j++) {
                Cell cell = row.createCell(j);
                if(times[i][j] == 0)
                    cell.setCellValue("");
                else
                    cell.setCellValue(times[i][j]);
            }
        }

        try (FileOutputStream outputStream = new FileOutputStream(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + "timechart.xlsx")) {
            workbook.write(outputStream);
        }
        workbook.close();
    }

    public static void main(String[] args) throws IOException {
        times = new int[rowNum][columnNum];
        times[0][1] = 3;
        times[9][7] = 3;
//        times[rowNum/8][5] = 5;
//        times[rowNum/4][136] = 36;
//        times[rowNum/2][190] = 10;
//        times[rowNum-10][12] = 11;
        writeExcel();
    }

}