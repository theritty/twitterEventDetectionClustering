package eventDetectionHybrid;

import cassandraConnector.CassandraDaoHybrid;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import drawing.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.util.*;

public class CassandraSpoutHybrid extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private CassandraDaoHybrid cassandraDao;
    private ArrayList<Long> roundlist;
    private int componentId;
    private Iterator<Row> iterator = null;
    private long current_round;
    private long count_tweets = 0;
    private String fileNum;
    private Date startDate = new Date();
    private Date lastDate = new Date();
    private long startRound = 0;

    private int USATaskNumber ;
    private int CANTaskNumber ;

    private int numWorkers ;
    private int numFinder ;

    private int USATask = 0;
    private int CANTask = 0;

    private int numOfBolts;
    private boolean chartCreated= false;

    private HashMap<String, Integer> USAwordMap = new HashMap<>();
    private HashMap<String, Integer> CANwordMap = new HashMap<>();


    public CassandraSpoutHybrid(CassandraDaoHybrid cassandraDao, String filenum, int USATaskNumber, int CANTaskNumber, int numWorkers, int numFinder, int numOfBolts) throws Exception {
        this.cassandraDao = cassandraDao;
        this.roundlist = new ArrayList<>();
        this.fileNum = filenum + "/";
        this.USATaskNumber = USATaskNumber;
        this.CANTaskNumber = CANTaskNumber;
        this.numWorkers = numWorkers;
        this.current_round=0;
        this.numOfBolts = numOfBolts;
        this.numFinder = numFinder;

    }
    @Override
    public void ack(Object msgId) {}
    @Override
    public void close() {}

    @Override
    public void fail(Object msgId) {}

    /**
     * The only thing that the methods will do It is emit each
     * file line
     */
    @Override
    public void nextTuple() {
        /**
         * The nextuple it is called forever, so if we have been readed the file
         * we will wait and then return
         */

        Date nowDate = new Date();
        if(iterator == null || !iterator.hasNext())
        {
            if(roundlist.size()==0)
            {
                streamEnd();
                return;
            }
            else {
                streamNewRound();
            }
        }
        Row row = iterator.next();
        String tweet = row.getString("tweet");
        String country = row.getString("country");

        if(tweet == null || tweet.length() == 0) return;

        if(iterator.hasNext()) {
            splitAndEmit(tweet, current_round, country);
        }
        else {
            splitAndEmit(tweet, current_round, country);
            putProcessedBoltInfoToCassandra();
            sendBlockEndInfoAndWait();

            USAwordMap.clear();
            CANwordMap.clear();
            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "workhistory.txt", new Date() + " Cass wake up " + current_round);
        }
        lastDate = new Date();
        ExcelWriter.putData(componentId,nowDate,lastDate, current_round, cassandraDao);
    }


    private void streamEnd() {
        //          getEventInfo.report();
        System.out.println("Number of tweets: " + count_tweets);
        try {
            System.out.println("sleeeeeeeep");
            Thread.sleep(120000);
            if(!chartCreated) {
//                ExcelWriter.createTimeChart(cassandraDao);
                chartCreated = true;
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void streamNewRound( ) {
        if(current_round!=0)
            ExcelWriter.putData(componentId,startDate,lastDate, current_round, cassandraDao);

        current_round = roundlist.remove(0);
        TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + current_round + ".txt",
                new Date() + ": Round submission from cass spout =>" + current_round );

        startDate = new Date();

        ResultSet resultSet = getDataFromCassandra(current_round);
        iterator = resultSet.iterator();
    }

    private void putProcessedBoltInfoToCassandra() {
        for(int k=2+numWorkers;k<CANTaskNumber+USATaskNumber+2+numWorkers+3+numFinder;k++) {
            try {
                List<Object> values = new ArrayList<>();
                values.add(current_round);
                values.add(k-1);
                values.add(false);
                cassandraDao.insertIntoProcessed(values.toArray());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    private void sendBlockEndInfoAndWait() {
        System.out.println("Sending blockend for " + current_round);
        for(int k=numWorkers+4;k<CANTaskNumber+USATaskNumber+numWorkers+4;k++) {
            System.out.println("blockend for wc " + k + " round " + current_round);
            collector.emitDirect(k, new Values("BLOCKEND", current_round, true));
        }

        TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + current_round + ".txt",
                new Date() + ": Round end from cass spout =>" + current_round );

        int sleepCnt = 0;
        try {
            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "workhistory.txt", new Date() + " Cass sleeping " + current_round);
            while (true) {
                Iterator<Row> iteratorProcessed = cassandraDao.getAllProcessed(current_round).iterator();
                boolean fin = true;
                while (iteratorProcessed.hasNext()) {
                    if (!iteratorProcessed.next().getBool("finished")) fin = false;
                }
                if (fin) break;
                else Thread.sleep(2000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void sendToNextTaskNum(String country, String tweet, long round) {
        if(country.equals("USA")) {
            if(USAwordMap.containsKey(tweet)) {
                this.collector.emitDirect(USAwordMap.get(tweet), new Values(tweet, round, false));
            }
            else {
                this.collector.emitDirect(USATask, new Values(tweet, round, false));
                USAwordMap.put(tweet, USATask++);
                if ((USATask < 4 + numWorkers) || (USATask >= USATaskNumber + 4 + numWorkers))
                    USATask = 4 + numWorkers;
            }
        }
        else {
            if(CANwordMap.containsKey(tweet)) {
                this.collector.emitDirect(CANwordMap.get(tweet), new Values(tweet, round, false));
            }
            else {
                this.collector.emitDirect(CANTask, new Values(tweet, round, false));
                CANwordMap.put(tweet, CANTask++);
                if ((CANTask < USATaskNumber + 4 + numWorkers) || (CANTask >= CANTaskNumber + USATaskNumber + 4 + numWorkers))
                    CANTask = USATaskNumber + 4 + numWorkers;
            }
        }
    }

    public void splitAndEmit(String tweetSentence, long round, String country) {
        List<String> tweets = Arrays.asList(tweetSentence.split(" "));
        for (String tweet : tweets) {
            if (!tweet.equals("") && tweet.length() > 2 && !tweet.equals("hiring") && !tweet.equals("careerarc")) {
                sendToNextTaskNum(country, tweet, round);
            }
        }
    }

    public void getRoundListFromCassandra(){
        ResultSet resultSet;
        try {
            resultSet = cassandraDao.getRounds();
            roundlist = new ArrayList<>();

            Iterator<Row> iterator = resultSet.iterator();
            while(iterator.hasNext())
            {
                Row row = iterator.next();
                roundlist.add(row.getLong("round"));
            }
            Collections.sort(roundlist, new Comparator<Long>() {
                public int compare(Long m1, Long m2) {
                    return m1.compareTo(m2);
                }
            });



            while (roundlist.get(0)<4068480)
//            while (roundlist.get(0)<4069540)
                roundlist.remove(0);
            while (roundlist.get(roundlist.size()-1)>=4070160)
                roundlist.remove(roundlist.size()-1);

            int i = 0;
            while(2>i++)
                roundlist.remove(0);

            System.out.println(roundlist);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public ResultSet getDataFromCassandra(long round) {
        ResultSet resultSet = null;
        try {
            resultSet = cassandraDao.getTweetsByRound(round);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return resultSet;
    }

    /**
     * We will create the file and get the collector object
     */
    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        getRoundListFromCassandra();
        this.collector = collector;
        this.componentId = context.getThisTaskId()-1;
        System.out.println("cass: " + componentId);
        this.startRound = roundlist.get(0);
        ExcelWriter.setNumOfBolts(numOfBolts);
        ExcelWriter.putStartDate(new Date(), fileNum, this.startRound);
    }

    /**
     * Declare the output field "word"
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "round", "blockEnd"));
    }

}