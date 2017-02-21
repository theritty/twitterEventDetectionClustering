package eventDetector.drawing;

import java.io.IOException;

import java.io.FileOutputStream;
import java.util.Date;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
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
    private static int rowNum = 21600;
    private static int columnNum = 200;
    private static int numOfBolts = 11;
    private static int createChart = 0;

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

    public static void putData(int id, Date boltStartTime, Date boltEndTime, long round) {

        long timeStart = (boltStartTime.getTime()-startTime.getTime())/1000;
        long duration = (boltEndTime.getTime()-boltStartTime.getTime())/1000;

        if(duration==0) duration = 1;

        while (duration-->0)
            times[(int) timeStart++][id + numOfBolts * ((int) ((round - startRound)) % 10) ] = id;

    }

    public static void createTimeChart () throws IOException {
        createChart++;
        System.out.println("trol " + createChart);
        if(createChart==2) {
            System.out.println(new Date() + " Excel creation started");
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
        writeExcel();
    }

}