package eventDetector.drawing;

import java.io.IOException;

import java.io.FileOutputStream;
import java.util.*;

import cassandraConnector.CassandraDao;
import com.datastax.driver.core.ResultSet;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

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
    private static String fileNum="12345xx";
    private static int lastInd ;
    private static int rowNum = 300000;
    private static int columnNum = 250;
    private static int numOfBolts = 25;
    private static int createChart = 0;

    public static void putStartDate(Date date, String filenum, long round) {
//        times = new int[rowNum][columnNum];
//        lastInd = rowNum -1;
        startTime = date;
        startRound = round;
//        for(int i = 0; i< rowNum; i++) {
//            times[i][0] = i;
//            for (int j = 1; j < columnNum; j++)
//                times[i][j] = 0;
//        }

        fileNum = filenum +"/";
    }

    public static void putData(int id, Date boltStartTime, Date boltEndTime, long round, CassandraDao cassandraDao) {

        long timeStart = (boltStartTime.getTime()-startTime.getTime())/1000;
        long duration = (boltEndTime.getTime()-boltStartTime.getTime())/1000;

        if(duration==0) duration = 1;

        while (duration-->0) {
            List<Object> values = new ArrayList<>();
            values.add((int) timeStart++);
            values.add(id + numOfBolts * ((int) ((round - startRound)) % 10));
            values.add(id);
            try {
                cassandraDao.insertIntoProcessTimes(values.toArray());
            } catch (Exception e) {
                e.printStackTrace();
            }
//            if(id + numOfBolts * ((int) ((round - startRound)) % 10) > 250)
//                TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt","VOVOVOVOVOVOOVOVOVOVOOV " + id + numOfBolts * ((int) ((round - startRound)) % 10) + " " + id + " " + round + " " + startRound);
//            times[(int) timeStart++][id + numOfBolts * ((int) ((round - startRound)) % 10)] = id;
        }

    }



    public static void cassandraTableToXYSeries(CassandraDao cassandraDao) {

        lastInd = rowNum -1;
        times = new int[rowNum][columnNum];

        XYSeriesCollection result = new XYSeriesCollection();
        XYSeries series = new XYSeries("1");

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

                if(process_row<96000)
                    //!!!!!!!!!!!!!!!!!!!!!!!!!!!
                    times[process_row][process_col-1]=process_id;
                    //!!!!!!!!!!!!!!!!!!!!!!!!!!!
            }
            clean();
            System.out.println("Last index is " + lastInd + " column num " + columnNum);
            for(int i=0; i< columnNum; i++) {
//            for(int i=0; i< 15; i++) {
                System.out.println("Col " + i);
                boolean added = false;
//                for(int j=0; j<15; j++) {
                for(int j=0; j<lastInd; j++) {
                    if(times[j][i]!=0) {
                        series.add(j,times[j][i]);
                        added=true;
                    }
                    else if(added){
                        result.addSeries(series);
                        series = new XYSeries(Integer.toString(j)+"and"+ Integer.toString(i));
                        added=false;
                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        result.addSeries(series);
        try {
            LineChart.drawScatterChart(result, Constants.IMAGES_FILE_PATH+"testxx");
        } catch (IOException e) {
            e.printStackTrace();
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
        try (FileOutputStream outputStream = new FileOutputStream(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + "timechart-processtimes4.xlsx")) {
            workbook.write(outputStream);
        }
        workbook.close();
        System.out.println("DONE");
    }

    public static void main(String[] args) throws Exception {
//        times = new int[rowNum][columnNum];
//        times[0][1] = 3;
//        times[9][7] = 3;
//        writeExcel();

        TopologyHelper topologyHelper = new TopologyHelper();
        Properties properties = topologyHelper.loadProperties( "config.properties" );

        String TWEETS_TABLE = properties.getProperty("tweets.table");
        String EVENTS_TABLE = properties.getProperty("events.table");
        String EVENTS_WORDBASED_TABLE = properties.getProperty("events_wordbased.table");
        String CLUSTER_TABLE = properties.getProperty("clusters.table");
        String CLUSTERINFO_TABLE = properties.getProperty("clusterinfo.table");
        String CLUSTERANDTWEET_TABLE = properties.getProperty("clusterandtweets.table");
        String PROCESSEDTWEET_TABLE = properties.getProperty("processed_tweets.table");
        String PROCESSTIMES_TABLE = properties.getProperty("processtimes.table");
        CassandraDao cassandraDao = new CassandraDao(TWEETS_TABLE, CLUSTER_TABLE, CLUSTERINFO_TABLE, CLUSTERANDTWEET_TABLE, EVENTS_TABLE, EVENTS_WORDBASED_TABLE, PROCESSEDTWEET_TABLE, PROCESSTIMES_TABLE);

//        cassandraTableToXYSeries(cassandraDao);
        createChart = 1;
        createTimeChart(cassandraDao);
        System.out.println("DONE");
        return;


    }

}