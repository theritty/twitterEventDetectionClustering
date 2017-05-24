package drawing;

import cassandraConnector.CassandraDao;
import cassandraConnector.CassandraDaoHybrid;
import cassandraConnector.CassandraDaoKeyBased;
import com.datastax.driver.core.ResultSet;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.usermodel.charts.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFChart;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTLineSer;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTMarker;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTMarkerStyle;
import org.openxmlformats.schemas.drawingml.x2006.chart.CTPlotArea;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

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
    private static String fileNum="keybasedsleep";
    private static int lastInd ;
    private static int rowNum = 3000000;
    private static int columnNum = 250;
    private static int numOfBolts = 25;
    private static int createChart = 0;

    public static void setNumOfBolts(int numOfBoltsX){
        numOfBolts = numOfBoltsX;
    }

    public static void putStartDate(Date date, String filenum, long round) {
        startTime = date;
        startRound = round;
        fileNum = filenum +"/";
    }

    public static void putData(int id, Date boltStartTime, Date boltEndTime, long round, CassandraDao cassandraDao) {

        long timeStart = (boltStartTime.getTime()-startTime.getTime())/1000;
        long duration = (boltEndTime.getTime()-boltStartTime.getTime())/1000;

        if(duration==0) duration = 1;

        while (duration-->0) {
            int col = (id + numOfBolts * ((int) ((round - startRound)) % 10));
//            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "HEEEEEEELPXXXXXXXX : " + timeStart + " " + col + " " + id);
            List<Object> values = new ArrayList<>();
            values.add((int) timeStart++);
            values.add(col);
            values.add(id);
            try {
                cassandraDao.insertIntoProcessTimes(values.toArray());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void putData(int id, Date boltStartTime, Date boltEndTime, long round, CassandraDaoKeyBased cassandraDao) {

        long timeStart = (boltStartTime.getTime()-startTime.getTime())/1000;
        long duration = (boltEndTime.getTime()-boltStartTime.getTime())/1000;

        if(duration==0) duration = 1;

        while (duration-->0) {
            int col = (id + numOfBolts * ((int) ((round - startRound)) % 10));
            int row = (int) timeStart++;
//            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "HEEEEEEELP : " + row + " " + col + " " + id);
            List<Object> values = new ArrayList<>();
            if(row < 0 ) TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "vadaaaa");
            values.add(row);
            values.add(col);
            values.add(id);
            try {
                cassandraDao.insertIntoProcessTimes(values.toArray());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    public static void putData(int id, Date boltStartTime, Date boltEndTime, long round, CassandraDaoHybrid cassandraDao) {

        long timeStart = (boltStartTime.getTime()-startTime.getTime())/1000;
        long duration = (boltEndTime.getTime()-boltStartTime.getTime())/1000;

        if(duration==0) duration = 1;

        while (duration-->0) {
            int col = (id + numOfBolts * ((int) ((round - startRound)) % 10));
            int row = (int) timeStart++;
//            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "HEEEEEEELP : " + row + " " + col + " " + id);
            List<Object> values = new ArrayList<>();
            if(row < 0 ) TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "vadaaaa");
            values.add(row);
            values.add(col);
            values.add(id);
            try {
                cassandraDao.insertIntoProcessTimes(values.toArray());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public static void cassandraTableToList(CassandraDao cassandraDao) {

        lastInd = rowNum -1;
        times = new int[rowNum][columnNum];

        for(int i = 0; i< rowNum; i++) {
            times[i][0] = i;
            for (int j = 1; j < columnNum; j++)
                times[i][j] = 0;
        }

        try {
            ResultSet resultSet;
            resultSet = cassandraDao.getProcessTimes();
            Iterator<com.datastax.driver.core.Row> iterator = resultSet.iterator();
            while (iterator.hasNext()) {
                com.datastax.driver.core.Row row = iterator.next();
                int process_row = row.getInt("row");
                int process_col = row.getInt("column");
                int process_id = row.getInt("id");

                if(process_row<0 ||process_col<0 ) continue;
                //!!!!!!!!!!!!!!!!!!!!!!!!!!!
                times[process_row][process_col-1]=process_id;
                //!!!!!!!!!!!!!!!!!!!!!!!!!!!
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void createTimeChart (CassandraDao cassandraDao) throws IOException {
        createChart++;
        if(createChart==2) {
            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", new Date() + " Excel creation started");
            System.out.println("Cass to table");
            cassandraTableToList(cassandraDao);
            System.out.println("writeToExcel");
            writeExcel();
        }
    }


    public static void clean () {
        //!!!!!!!!!!!!!!!!!!!!!!!!!!!
        rowNum = 96000;
        //!!!!!!!!!!!!!!!!!!!!!!!!!!!
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

        System.out.println("here " + lastInd);
        for (int i=0;i<lastInd;i++) {
            Row row = sheet.createRow(i);
            for (int j = 0; j<columnNum; j++) {
                Cell cell = row.createCell(j);
                cell.setCellValue(times[i][j]);
            }
        }
        System.out.println("here 2");
        try (FileOutputStream outputStream = new FileOutputStream(Constants.EXPERIMENT_FILE + fileNum + ".xlsx")) {
            workbook.write(outputStream);
        }
        workbook.close();
        System.out.println("DONE");
    }

    public static void main(String[] args) throws Exception {
        Workbook wb = new XSSFWorkbook();
        Sheet dataSheet = wb.createSheet("linechart");

        final int NUM_OF_ROWS = 10;
        final int NUM_OF_COLUMNS = 4;

        Row row;
        Cell cell;
        for (int rowIndex = 0; rowIndex < NUM_OF_ROWS; rowIndex++) {
            row = dataSheet.createRow((short) rowIndex);
            for (int colIndex = 0; colIndex < NUM_OF_COLUMNS; colIndex++) {
                cell = row.createCell((short) colIndex);
                if(colIndex==0) cell.setCellValue(rowIndex);
                else {
                    int val = (int) (Math.random()*10);
                    if(val == 0) cell.setCellValue("");
                    else cell.setCellValue(rowIndex * ((colIndex + 1) + ((int) (Math.random() * 10))));
                }
            }
        }

        Drawing drawing = dataSheet.createDrawingPatriarch();
        ClientAnchor anchor = drawing.createAnchor(0, 0, 0, 0, NUM_OF_COLUMNS + 2, 3, NUM_OF_COLUMNS + 15, 20);

        Chart chart = drawing.createChart(anchor);
        ChartLegend legend = chart.getOrCreateLegend();
        legend.setPosition(LegendPosition.RIGHT);

        LineChartData data = chart.getChartDataFactory().createLineChartData();

        ChartAxis bottomAxis = chart.getChartAxisFactory().createCategoryAxis(AxisPosition.BOTTOM);
        ValueAxis leftAxis = chart.getChartAxisFactory().createValueAxis(AxisPosition.LEFT);
        leftAxis.setCrosses(AxisCrosses.AUTO_ZERO);

        ChartDataSource<Number> xs = DataSources.fromNumericCellRange(dataSheet, new CellRangeAddress(0, NUM_OF_ROWS - 1, 0, 0));
        ChartDataSource<Number> ys1 = DataSources.fromNumericCellRange(dataSheet, new CellRangeAddress(0, NUM_OF_ROWS - 1, 1, 1));
        ChartDataSource<Number> ys2 = DataSources.fromNumericCellRange(dataSheet, new CellRangeAddress(0, NUM_OF_ROWS - 1, 2, 2));
        ChartDataSource<Number> ys3 = DataSources.fromNumericCellRange(dataSheet, new CellRangeAddress(0, NUM_OF_ROWS - 1, 3, 3));

        LineChartSeries series1 = data.addSeries(xs, ys1);
        series1.setTitle("one");
        LineChartSeries series2 = data.addSeries(xs, ys2);
        series2.setTitle("two");
        LineChartSeries series3 = data.addSeries(xs, ys3);
        series3.setTitle("three");

        chart.plot(data, bottomAxis, leftAxis);

        XSSFChart xssfChart = (XSSFChart) chart;
        CTPlotArea plotArea = xssfChart.getCTChart().getPlotArea();
        CTMarker ctMarker = CTMarker.Factory.newInstance();
        ctMarker.setSymbol(CTMarkerStyle.Factory.newInstance());
        for (CTLineSer ser : plotArea.getLineChartArray()[0].getSerArray()) {
            ser.setMarker(ctMarker);
        }

        FileOutputStream fileOut = new FileOutputStream(Constants.EXPERIMENT_FILE + "testchart.xlsx");
        wb.write(fileOut);
        fileOut.close();
    }

//    public static void main(String[] args) throws Exception {
////        times = new int[rowNum][columnNum];
////        times[0][1] = 3;
////        times[9][7] = 3;
////        writeExcel();
//
//        TopologyHelper topologyHelper = new TopologyHelper();
//        Properties properties = topologyHelper.loadProperties( "config.properties" );
//
//        String TWEETS_TABLE = properties.getProperty("clustering.tweets.table");
//        String EVENTS_TABLE = properties.getProperty("clustering.events.table");
//        String EVENTS_WORDBASED_TABLE = properties.getProperty("clustering.events_wordbased.table");
//        String CLUSTER_TABLE = properties.getProperty("clustering.clusters.table");
//        String CLUSTERANDTWEET_TABLE = properties.getProperty("clustering.clusterandtweets.table");
//        String PROCESSEDTWEET_TABLE = properties.getProperty("clustering.processed_tweets.table");
////        String PROCESSTIMES_TABLE = properties.getProperty("clustering.processtimes.table");
////        String PROCESSTIMES_TABLE = properties.getProperty("keybased.processtimes.table");
//        String PROCESSTIMES_TABLE = properties.getProperty("keybasedsleep.processtimes.table");
//        CassandraDao cassandraDao = new CassandraDao(TWEETS_TABLE, CLUSTER_TABLE, CLUSTERANDTWEET_TABLE, EVENTS_TABLE, EVENTS_WORDBASED_TABLE, PROCESSEDTWEET_TABLE, PROCESSTIMES_TABLE);
//
//        createChart = 1;
//        createTimeChart(cassandraDao);
//        System.out.println("DONE");
//        return;
//    }
}