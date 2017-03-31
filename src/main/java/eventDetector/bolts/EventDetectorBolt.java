package eventDetector.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import cassandraConnector.CassandraDao;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import eventDetector.drawing.ExcelWriter;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.io.IOException;
import java.util.*;

public class EventDetectorBolt extends BaseRichBolt {

    private OutputCollector collector;
    private long currentRound = 0;
    private long ignoredCount = 0;
    private int componentId;
    private String fileNum;
    private Date lastDate = new Date();
    private Date startDate = new Date();
    private String country;

    private CassandraDao cassandraDao;


    public EventDetectorBolt(String filenum, CassandraDao cassandraDao, String country)
    {
        this.fileNum = filenum + "/";
        this.cassandraDao = cassandraDao;
        this.country = country;
    }
    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.componentId = context.getThisTaskId()-1;
        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "eventdet : " + componentId + " " + country );
    }

    @Override
    public void execute(Tuple tuple) {
        long round = tuple.getLongByField("round");
        String country = tuple.getStringByField("country");

        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", country + " event detection " + round);
        if(round==0L) {
            try {
                ExcelWriter.createTimeChart(cassandraDao);
            } catch (IOException e) {
                e.printStackTrace();
            }
            collector.ack(tuple);
            return;
        }
        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "workhistory.txt", new Date() + " Event Detector " + componentId + " working "  + round);

        Date nowDate = new Date();
        if(round > currentRound)
        {
            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
                    "Event Detector "+ componentId + " end for round " + currentRound + " at " + lastDate);

            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
                    "Event Detector "+ componentId + " time taken for round" + currentRound + " is " +
                            (lastDate.getTime()-startDate.getTime())/1000);

            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "round " + round + " end of.");
//            if ( currentRound!=0)
//                ExcelWriter.putData(componentId,startDate,lastDate, "eventdetector",tuple.getSourceStreamId(), currentRound);


            startDate = new Date();
            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + round + ".txt",
                    "Event Detector "+ componentId + " starting for round " + round + " at " + startDate );

            currentRound = round;
        }
        else if(round < currentRound) {
            ignoredCount++;
            if(ignoredCount%1000==0)
                TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + "ignoreCount.txt",
                        "Event Detector ignored count " + componentId + ": " + ignoredCount );
            collector.ack(tuple);
            return;
        }

        ResultSet resultSet ;
        try {
            resultSet = cassandraDao.getClusters(country);
            Iterator<Row> iterator = resultSet.iterator();
            while (iterator.hasNext()) {
                Row row = iterator.next();
                UUID clusterid = row.getUUID("id");
                int prevnumtweets = row.getInt("prevnumtweets");
                int currentnumtweets = row.getInt("currentnumtweets");
                int numtweets = prevnumtweets+currentnumtweets;
                long lastround = row.getLong("lastround");


                //Total number of tweets is below the threshold. Remove cluster.
                if(currentnumtweets<50) {
                    cassandraDao.deleteFromClusters(country, clusterid);
                    continue;
                }
                else {
                    HashMap<String, Double> cosinevector = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);
                    Iterator<Map.Entry<String, Double>> it = cosinevector.entrySet().iterator();
                    while(it.hasNext()) {
                        Map.Entry<String, Double> entry = it.next();
                        double value = entry.getValue();
                        if(value < 0.01) it.remove();
                    }

                    List<Object> values2 = new ArrayList<>();
                    values2.add(clusterid);
                    values2.add(country);
                    values2.add(cosinevector);
                    values2.add(numtweets);
                    values2.add(0);
                    values2.add(round);
                    cassandraDao.insertIntoClusters(values2.toArray());

                    if( ((double) currentnumtweets)/((double) numtweets) > 0.5){
                        //add as event
                        List<Object> values_event = new ArrayList<>();
                        values_event.add(round);
                        values_event.add(clusterid);
                        values_event.add(country);
                        values_event.add(cosinevector);
                        values_event.add(((double) currentnumtweets)/((double) numtweets) );
                        values_event.add(numtweets );
                        cassandraDao.insertIntoEvents(values_event.toArray());
                    }
                }
            }

            List<Object> values3 = new ArrayList<>();
            values3.add(round);
            values3.add(componentId);
            values3.add(0L);
            values3.add(0L);
            values3.add(true);
            values3.add(country);
            cassandraDao.insertIntoProcessed(values3.toArray());

        } catch (Exception e) {
            e.printStackTrace();
        }
        lastDate = new Date();



        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "round " + round + " put excel " + componentId);
        ExcelWriter.putData(componentId, nowDate, lastDate, currentRound,cassandraDao);
        collector.ack(tuple);


    }


//    public void cleanupclusters(long round) {
//        System.out.println("Clean up for round " + round);
//        ResultSet resultSet ;
//        Constants.lock.lock();
//        try {
//            resultSet = cassandraDao.getClusters(country);
//            Iterator<Row> iterator = resultSet.iterator();
//            while (iterator.hasNext()) {
//                Row row = iterator.next();
//                long lastround = row.getLong("lastround");
//                int numberoftweets = row.getInt("numberoftweets");
//                if(round - lastround>= 2 || numberoftweets<30) {
//                    UUID clusterid = row.getUUID("id");
//                    cassandraDao.deleteFromClusters(country, clusterid);
//                }
//                else {
//                    HashMap<String, Double> cosinevector = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);
//                    int numTweets = row.getInt("numberoftweets");
//                    Iterator<Map.Entry<String, Double>> it = cosinevector.entrySet().iterator();
//                    while(it.hasNext()) {
//                        Map.Entry<String, Double> entry = it.next();
//                        double value = entry.getValue();
//                        if(value < 0.01) {
////                            cosinevector.remove(entry.getKey());
//                            it.remove();
//                        }
//                    }
//                    List<Object> values = new ArrayList<>();
//                    values.add(row.getUUID("id"));
//                    values.add(country);
//                    values.add(cosinevector);
//                    values.add(numTweets);
//                    values.add(round);
//                    cassandraDao.insertIntoClusters(values.toArray());
//                }
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//            Constants.lock.unlock();
//        }
//
//        Constants.lock.unlock();
//    }

//    @Override
//    public void execute(Tuple tuple) {
//        long round = tuple.getLongByField("round");
//        String country = tuple.getStringByField("country");
//
//        System.out.println(country + " event detection " + round);
//        if(round==0L) {
//            try {
//                System.out.println("trololo");
//                ExcelWriter.createTimeChart();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            return;
//        }
//        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "workhistory.txt", new Date() + " Event Detector " + componentId + " working "  + round);
//
//        Date nowDate = new Date();
//        if(round > currentRound)
//        {
//            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
//                    "Event Detector "+ componentId + " end for round " + currentRound + " at " + lastDate);
//
//            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
//                    "Event Detector "+ componentId + " time taken for round" + currentRound + " is " +
//                            (lastDate.getTime()-startDate.getTime())/1000);
//
//            System.out.println("round " + round + " end of.");
////            if ( currentRound!=0)
////                ExcelWriter.putData(componentId,startDate,lastDate, "eventdetector",tuple.getSourceStreamId(), currentRound);
//
//
//            startDate = new Date();
//            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + round + ".txt",
//                    "Event Detector "+ componentId + " starting for round " + round + " at " + startDate );
//
//            currentRound = round;
//        }
//        else if(round < currentRound) {
//            ignoredCount++;
//            if(ignoredCount%1000==0)
//                TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + "ignoreCount.txt",
//                        "Event Detector ignored count " + componentId + ": " + ignoredCount );
//            return;
//        }
//
//        ResultSet resultSet ;
//        try {
//            resultSet = cassandraDao.getClusterinfoByRound(round, country);
//            Iterator<Row> iterator = resultSet.iterator();
//            while (iterator.hasNext()) {
//                Row row = iterator.next();
//                UUID clusterid = row.getUUID("id");
//                int numtweets = row.getInt("numberoftweets");
//
//                if(numtweets<100) continue;
//                ResultSet resultSet2 ;
//                resultSet2 = cassandraDao.getClusterinfoByRoundAndId(round-1, country, clusterid);
//                Iterator<Row> iterator2 = resultSet2.iterator();
//
//                if(!iterator2.hasNext()) {
//                    int numtweetsPrev = 50;
//
//                    if( ((double) numtweets - (double) numtweetsPrev)/((double) numtweets) > 0.5){
//                        addEvent(clusterid,round, ((double) numtweets - (double) numtweetsPrev)/((double) numtweets),country, numtweets);
//                    }
//                }
//                else {
//                    Row row2 = iterator2.next();
//                    int numtweetsPrev = row2.getInt("numberoftweets");
//
//                    System.out.println(clusterid + " " + numtweets + " " + numtweetsPrev);
//                    if( ((double) numtweets - (double) numtweetsPrev)/((double) numtweets) > 0.5){
//                        addEvent(clusterid,round, ((double) numtweets - (double) numtweetsPrev)/((double) numtweets),country, numtweets);
//                    }
//                }
//            }
//
//            cleanupclusters(round);
//
//            List<Object> values = new ArrayList<>();
//            values.add(round);
//            values.add(componentId);
//            values.add(0L);
//            values.add(0L);
//            values.add(true);
//            values.add(country);
//            cassandraDao.insertIntoProcessed(values.toArray());
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        lastDate = new Date();
//
//
//
//        System.out.println("round " + round + " put excel");
//        ExcelWriter.putData(componentId, nowDate, lastDate, currentRound);
//
//
//    }

    public  void addEvent(UUID clusterid, long round, double incrementrate, String country, int numtweet) {
        ResultSet resultSet ;
        try {
            resultSet = cassandraDao.getClustersById(country, clusterid);
            Iterator<Row> iterator = resultSet.iterator();

            if(!iterator.hasNext()) {
                TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "Errorrrrrrrr");
            }
            else {
                while (iterator.hasNext()) {
                    Row row3 = iterator.next();
                    HashMap<String, Double> cosinevector = (HashMap<String, Double>) row3.getMap("cosinevector", String.class, Double.class);
                    List<Object> values = new ArrayList<>();
                    values.add(round);
                    values.add(clusterid);
                    values.add(country);
                    values.add(cosinevector);
                    values.add(incrementrate );
                    values.add(numtweet );
                    cassandraDao.insertIntoEvents(values.toArray());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields( "round", "country"));
    }

}