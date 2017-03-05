package eventDetector.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
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
        System.out.println("eventdet : " + componentId + " " + country );
    }

    @Override
    public void execute(Tuple tuple) {
        long round = tuple.getLongByField("round");
        String country = tuple.getStringByField("country");

        if(round==0L) {
            try {
                System.out.println("trololo");
                ExcelWriter.createTimeChart();
            } catch (IOException e) {
                e.printStackTrace();
            }
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

            System.out.println("round " + round + " end of.");
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
            return;
        }

        ResultSet resultSet ;
        try {
            resultSet = cassandraDao.getClusterinfoByRound(round);
            Iterator<Row> iterator = resultSet.iterator();
            while (iterator.hasNext()) {
                Row row = iterator.next();
                UUID clusterid = row.getUUID("id");
                int numtweets = row.getInt("numberoftweets");

                if(numtweets<60) continue;
                ResultSet resultSet2 ;
                resultSet2 = cassandraDao.getClusterinfoByRoundAndId(round-1, clusterid);
                Iterator<Row> iterator2 = resultSet2.iterator();

                if(!iterator2.hasNext()) {
                    int numtweetsPrev = 1;

                    if( ((double) numtweets - (double) numtweetsPrev)/((double) numtweets) > 0.5){
                        addEvent(clusterid,round, ((double) numtweets - (double) numtweetsPrev)/((double) numtweets),country, numtweets);
                    }
                }
                else {
                    Row row2 = iterator2.next();
                    int numtweetsPrev = row2.getInt("numberoftweets");

                    if( ((double) numtweets - (double) numtweetsPrev)/((double) numtweets) > 0.5){
                        addEvent(clusterid,round, ((double) numtweets - (double) numtweetsPrev)/((double) numtweets),country, numtweets);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        lastDate = new Date();

        System.out.println("round " + round + " put excel");
        ExcelWriter.putData(componentId, nowDate, lastDate, currentRound);


    }

    public  void addEvent(UUID clusterid, long round, double incrementrate, String country, int numtweet) {
        ResultSet resultSet ;
        try {
            resultSet = cassandraDao.getClustersById(clusterid);
            Iterator<Row> iterator = resultSet.iterator();

            if(!iterator.hasNext()) {
                System.out.println("Errorrrrrrrr");
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