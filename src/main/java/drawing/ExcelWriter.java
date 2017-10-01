package drawing;

import cassandraConnector.CassandraDao;
import cassandraConnector.CassandraDaoHybrid;
import cassandraConnector.CassandraDaoKeyBased;
import com.datastax.driver.core.ResultSet;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
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
    private static String fileNum="experiment-4-keybased-hybrid-xxx";
    private static int lastInd ;
    private static int rowNum = 300000;
    private static int columnNum = 250;
    private static int numOfBolts = 15;
    private static int createChart = 0;

    public static void setNumOfBolts(int numOfBoltsX){
        //numOfBolts = numOfBoltsX;
    }

    public static void putStartDate(Date date, String filenum, long round) {
//        startTime = date;
//        startRound = round;
        fileNum = filenum +"/";
    }

    public static void putData(int id, Date boltStartTime, Date boltEndTime, long round, CassandraDao cassandraDao) {

        long timeStart = (boltStartTime.getTime()-startTime.getTime())/1000;
        long duration = (boltEndTime.getTime()-boltStartTime.getTime())/1000;

        if(duration==0) duration = 1;

        while (duration-->0) {
            int col = (id + numOfBolts * ((int) (round - startRound) % 10));

//            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "HEEEEEEELPXXXXXXXX : " + timeStart + " " + col + " " + id);
            if(col%15!=id)
                TopologyHelper.writeToFile(Constants.EXPERIMENT_FILE + " test-sout.txt", col + " " +  id + " " + numOfBolts + " " + startRound + " " + round);
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
            if(col%15!=id)
                TopologyHelper.writeToFile(Constants.EXPERIMENT_FILE + " test-sout.txt", col + " " +  id + " " + numOfBolts + " " + startRound + " " + round);

            System.out.println(id + " " + numOfBolts + " " + startRound + " " + round);
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

                TopologyHelper.writeToFile(Constants.EXPERIMENT_FILE + " test-sout.txt", startTime.toString());
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
//        times = new int[rowNum][15];

        for(int i = 0; i< rowNum; i++) {
            times[i][0] = i;
//            for (int j = 1; j < 15; j++)
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
                int process_col = row.getInt("id");
//                int process_col = row.getInt("column");
                int process_id = row.getInt("id");

                if(process_row<0 ||process_col<0 ) {
                    System.out.println("?");
                    continue;
                }
//                if(process_col%11!=process_id || )
//                {
//                    System.out.println("wow " + process_col + " " + process_id);;
//                }
                //!!!!!!!!!!!!!!!!!!!!!!!!!!!
                times[process_row][process_col]=process_id;
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
        try (FileOutputStream outputStream = new FileOutputStream(Constants.EXPERIMENT_FILE + fileNum + ".xlsx")) {
            workbook.write(outputStream);
        }
        workbook.close();
        System.out.println("DONE exc");
    }


    public static void run(String filename, String tablename) throws Exception {
        TopologyHelper topologyHelper = new TopologyHelper();
        Properties properties = topologyHelper.loadProperties( "config.properties" );

        fileNum = filename;

        String TWEETS_TABLE = properties.getProperty("clustering.tweets.table");
        String EVENTS_TABLE = properties.getProperty("clustering.events.table");
        String EVENTS_WORDBASED_TABLE = properties.getProperty("clustering.events_wordbased.table");
        String CLUSTER_TABLE = properties.getProperty("clustering.clusters.table");
        String PROCESSEDTWEET_TABLE = properties.getProperty("clustering.processed_tweets.table");
        String PROCESSTIMES_TABLE = properties.getProperty(tablename);
        CassandraDao cassandraDao = new CassandraDao(TWEETS_TABLE, CLUSTER_TABLE, EVENTS_TABLE, EVENTS_WORDBASED_TABLE, PROCESSEDTWEET_TABLE, PROCESSTIMES_TABLE);

        createChart = 1;
        createTimeChart(cassandraDao);
        System.out.println("DONE run");
        return;
    }


    public static void main(String[] args) throws Exception {
//        times = new int[rowNum][columnNum];
//        times[0][1] = 3;
//        times[9][7] = 3;
//        writeExcel();

        run("experiment-4-method-keybased-no", "keybased.processtimes.table");
        run("experiment-4-method-keybasedSleep-no", "keybasedsleep.processtimes.table");
        run("experiment-4-method-clustering-no", "clustering.processtimes.table");
//        run("experiment-4-method-hybrid-no", "hybrid.processtimes.table");

        System.out.println("DONEXX");
//
//
//        TopologyHelper topologyHelper = new TopologyHelper();
//        Properties properties = topologyHelper.loadProperties( "config.properties" );
//
//        String TWEETS_TABLE = properties.getProperty("clustering.tweets.table");
//        String EVENTS_TABLE = properties.getProperty("clustering.events.table");
//        String EVENTS_WORDBASED_TABLE = properties.getProperty("clustering.events_wordbased.table");
//        String CLUSTER_TABLE = properties.getProperty("clustering.clusters.table");
//        String PROCESSEDTWEET_TABLE = properties.getProperty("clustering.processed_tweets.table");
////        String PROCESSTIMES_TABLE = properties.getProperty("clustering.processtimes.table");
////        String PROCESSTIMES_TABLE = properties.getProperty("keybased.processtimes.table");
////        String PROCESSTIMES_TABLE = properties.getProperty("keybasedsleep.processtimes.table");
//        String PROCESSTIMES_TABLE = properties.getProperty("hybrid.processtimes.table");
//        CassandraDao cassandraDao = new CassandraDao(TWEETS_TABLE, CLUSTER_TABLE, EVENTS_TABLE, EVENTS_WORDBASED_TABLE, PROCESSEDTWEET_TABLE, PROCESSTIMES_TABLE);
//
//        createChart = 1;
//        createTimeChart(cassandraDao);
//        System.out.println("DONE");
//        return;
    }
}