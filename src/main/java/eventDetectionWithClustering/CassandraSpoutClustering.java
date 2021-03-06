package eventDetectionWithClustering;

import cassandraConnector.CassandraDao;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

public class CassandraSpoutClustering extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private CassandraDao cassandraDao;
    private ArrayList<Long> roundlist;
    private int componentId;
    private Iterator<Row> iterator = null;
    private long current_round;
    private long count_tweets = 0;
    private String fileNum;
    private boolean start = true;
    private Date lastDate = new Date();
    private long numTweetRound = 0;
    private int USATask=0;
    private int CANTask=0;

    private int CANTaskNumber = 0;
    private int USATaskNumber = 0;
    private int numWorkers = 0;

    private HashMap<Integer, Long> counts = new HashMap<>();

    private long startRound;
    private long endRound;
    private HashMap<String, Integer> vectorMap;
    private boolean finished = false;

    private int numOfBolts;


    public CassandraSpoutClustering(CassandraDao cassandraDao, String filenum, long start, long end, int canTaskNum, int usaTaskNum, int numWorkers, int numOfBolts) throws Exception {
        this.cassandraDao = cassandraDao;
        roundlist = new ArrayList<>();
        this.fileNum = filenum + "/";
        this.startRound = start;
        this.endRound = end;
        this.CANTaskNumber = canTaskNum;
        this.USATaskNumber = usaTaskNum;
        this.numWorkers = numWorkers;
        this.numOfBolts = numOfBolts;
    }
    @Override
    public void ack(Object msgId) {
    }
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

        if( (USATask<2+numWorkers) || (USATask>= USATaskNumber+2+numWorkers))
            USATask = 2+numWorkers;
        if( (CANTask < USATaskNumber+2+numWorkers) || (CANTask>=CANTaskNumber+USATaskNumber+2+numWorkers))
            CANTask = USATaskNumber+2+numWorkers;


        Date nowDate = new Date();
        if(iterator == null || !iterator.hasNext())
        {
            if(finished) {
                try {
                    Thread.sleep(1200000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return;
            }

            if(roundlist.size()==0)
            {
                try {
                    for(int k=2+numWorkers;k<CANTaskNumber+USATaskNumber+2+numWorkers;k++)
                        collector.emitDirect(k, new Values(new ArrayList<>(), 0L, current_round+1, true, true));

                    try {
                        Thread.sleep(120000);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    finished = true;

                    TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", new Date() + " Number of tweets: " + count_tweets);
                    Thread.sleep(10000000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return;
            }

            current_round = roundlist.remove(0);
            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + current_round + ".txt",
                    new Date() + ": Round submission from cass spout =>" + current_round );

            if(start) start = false;

            ResultSet resultSet = getDataFromCassandra(current_round);
            iterator = resultSet.iterator();
        }


        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "workhistory.txt", new Date() + " Cass goooooo " + current_round);


        Row row = iterator.next();
        String tweet = row.getString("tweet");
        String country = row.getString("country");

        if(iterator.hasNext()) {
            vectorizeAndEmit(tweet, row.getLong("id"), current_round, country);
            numTweetRound ++;
        }
        else {
            vectorizeAndEmit(tweet, row.getLong("id"), current_round, country);


            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "Round " + current_round + " number of tweets " + ++numTweetRound);
            numTweetRound=0;
            try {
                for(int k=2+numWorkers;k<CANTaskNumber+USATaskNumber+2+numWorkers;k++) {
                    List<Object> values = new ArrayList<>();
                    values.add(current_round);
                    values.add(k-1);
                    values.add(counts.get(k));
                    values.add(0L);
                    values.add(false);
                    if(k<USATaskNumber+2+numWorkers) values.add("USA");
                    else values.add("CAN");
                    cassandraDao.insertIntoProcessed(values.toArray());
                }
                counts.clear();
                for(int k=2+numWorkers;k<CANTaskNumber+USATaskNumber+2+numWorkers;k++)
                    collector.emitDirect(k, new Values(new ArrayList<>(), 0L, current_round, true, false));


                List<Object> values = new ArrayList<>();
                values.add(current_round);
                values.add(CANTaskNumber+USATaskNumber+1+numWorkers);
                values.add(0L);
                values.add(0L);
                values.add(false);
                values.add("USA");
                cassandraDao.insertIntoProcessed(values.toArray());


                List<Object> values2 = new ArrayList<>();
                values2.add(current_round);
                values2.add(CANTaskNumber+USATaskNumber+2+numWorkers);
                values2.add(0L);
                values2.add(0L);
                values2.add(false);
                values2.add("CAN");
                cassandraDao.insertIntoProcessed(values2.toArray());


                TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "workhistory.txt", new Date() + " Cass sleeping " + current_round);
                while(true) {
                    Iterator<Row> iteratorProcessed = cassandraDao.getAllProcessed(current_round).iterator();
                    boolean fin = true;
                    while (iteratorProcessed.hasNext()) {
                        if(!iteratorProcessed.next().getBool("finished")) fin = false;
                    }
                    if(fin) break;
                    else Thread.sleep(2000);
                }
                TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "workhistory.txt", new Date() + " Cass wake up " + current_round);


                TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + current_round + ".txt",
                        new Date() + ": Round end from cass spout =>" + current_round );
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        lastDate = new Date();
        if(!start )
            ExcelWriter.putData(componentId,nowDate,lastDate, current_round, cassandraDao);

        count_tweets++;

    }

    public void vectorizeAndEmit(String tweetSentence, long id, long round, String country) {
        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "workhistory.txt", new Date() + " Cass goooooo " + current_round);
        List<String> tweets = Arrays.asList(tweetSentence.split(" "));
        ArrayList<String> tweetMap = new ArrayList<>();
        for (String tweet : tweets) {
//            tweet = tweet.replace("#", "");
            if(tweet.length()>=3 && vectorMap.get(tweet)!=null)
                tweetMap.add(tweet);
        }



        if(tweetMap.size()>1) {
            if(country.equals("USA")) {
                collector.emitDirect(USATask, new Values(tweetMap, id, round, false, false));
                if ( counts.get(USATask) != null)
                    counts.put(USATask, counts.get(USATask)+1);
                else
                    counts.put(USATask, 1L);
                USATask++;
//                TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", new Date() + " USA emit: " + (USATask-1));
            }
            else {
                collector.emitDirect(CANTask, new Values(tweetMap, id, round, false, false));
                if ( counts.get(CANTask) != null)
                    counts.put(CANTask, counts.get(CANTask)+1);
                else
                    counts.put(CANTask, 1L);
                CANTask++;
//                TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", new Date() + " CAN emit: " + (CANTask-1));
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

            while(roundlist.get(0)<startRound)
                roundlist.remove(0);
            while(roundlist.get(roundlist.size()-1)>endRound)
                roundlist.remove(roundlist.size()-1);

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

    public void createWordsMap() {
        vectorMap = new HashMap<>();
        try {
            int index = 0;
            ClassLoader classloader = Thread.currentThread().getContextClassLoader();
            InputStream is = classloader.getResourceAsStream("wordList.txt");
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                vectorMap.put(line.replace("#",""),index++);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
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
        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "cass" + " id: " + componentId);
        ExcelWriter.setNumOfBolts(numOfBolts);
        ExcelWriter.putStartDate(new Date(), fileNum, this.startRound);

        createWordsMap();
    }

    /**
     * Declare the output field "word"
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweetmap", "tweetid", "round", "blockEnd", "streamEnd"));

//        declarer.declareStream("USA", new Fields("tweetmap", "tweetid", "round", "blockEnd", "streamEnd"));
//        declarer.declareStream("CAN", new Fields("tweetmap", "tweetid", "round", "blockEnd", "streamEnd"));
    }

}