package eventDetector.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import cassandraConnector.CassandraDao;
import eventDetector.drawing.ExcelWriter;
import jnr.ffi.annotations.In;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CassandraSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private CassandraDao cassandraDao;
    private ArrayList<Long> roundlist;
    private int componentId;
    private Iterator<Row> iterator = null;
    private long current_round;
    private long count_tweets = 0;
    private String fileNum;
    private boolean start = true;
    private Date startDate = new Date();
    private Date lastDate = new Date();

    private long startRound = 2033719;
    private long endRound = 2033719;
    private HashMap<String, Integer> vectorMap;
    private int vectorMapLength = 0;


    public CassandraSpout(CassandraDao cassandraDao, String filenum, long start, long end) throws Exception {
        this.cassandraDao = cassandraDao;
        roundlist = new ArrayList<>();
        this.fileNum = filenum + "/";
        this.startRound = start;
        this.endRound = end;
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

        if(iterator == null || !iterator.hasNext())
        {
            if(roundlist.size()==0)
            {
                try {
                    collector.emit("CAN", new Values(new HashMap<>(), 0L,current_round+1, true));
                    try {
                            Thread.sleep(120000);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    collector.emit("CAN", new Values(new HashMap<>(), 0L,current_round+1, true));
                    ExcelWriter.createTimeChart();

                    System.out.println("Number of tweets: " + count_tweets);
                    Thread.sleep(10000000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return;
            }

            current_round = roundlist.remove(0);
            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + current_round + ".txt",
                    new Date() + ": Round submission from cass spout =>" + current_round );


                if(!start) {
                    ExcelWriter.putData(componentId,startDate,lastDate, "cassSpout", "both", current_round);
                }
                else start = false;

            startDate = new Date();

            ResultSet resultSet = getDataFromCassandra(current_round);
            iterator = resultSet.iterator();
        }
        Row row = iterator.next();
        String tweet = row.getString("tweet");
        String country = row.getString("country");

        if(tweet == null || tweet.length() == 0) return;

        if(iterator.hasNext()) {
            vectorizeAndEmit(tweet, row.getLong("id"), current_round, country);
        }
        else {
            vectorizeAndEmit(tweet, row.getLong("id"), current_round, country);
//            collector.emit("USA", new Values(new HashMap<>(), 0L, current_round, true, tmp_roundlist));
            try {
                    TopologyHelper.writeToFile(Constants.WORKHISTORY, new Date() + " Cass sleeping " + current_round);
                    Thread.sleep(15000);
                    TopologyHelper.writeToFile(Constants.WORKHISTORY, new Date() + " Cass wake up " + current_round);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }

            collector.emit("CAN", new Values(new HashMap<>(), 0L, current_round, true));
            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + current_round + ".txt",
                    new Date() + ": Round end from cass spout =>" + current_round );

        }
        lastDate = new Date();


        try {
//                TopologyHelper.writeToFile(Constants.WORKHISTORY, new Date() + " Cass sleeping " + current_round);
                Thread.sleep(5);
//                TopologyHelper.writeToFile(Constants.WORKHISTORY, new Date() + " Cass wake up " + current_round);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void vectorizeAndEmit(String tweetSentence, long id, long round, String country) {
        TopologyHelper.writeToFile(Constants.WORKHISTORY, new Date() + " Cass goooooo " + current_round);
        List<String> tweets = Arrays.asList(tweetSentence.split(" "));
        HashMap<String, Double> tweetMap = new HashMap<>();
        for (String tweet : tweets) {
            tweet = tweet.replace("#", "");
            if(tweet.length()>=3 && vectorMap.get(tweet)!=null)
                tweetMap.put(tweet,1.0);
        }

//        System.out.println("Tweet " + tweetSentence + " map " + tweetMap + " !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        if(tweetMap.size()>1)
            collector.emit(country, new Values(tweetMap, id, round, false));
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
        vectorMapLength = vectorMap.size();
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
        System.out.println("cass" + " id: " + componentId);
        ExcelWriter.putStartDate(new Date(), fileNum, this.startRound);

        createWordsMap();
    }

    /**
     * Declare the output field "word"
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declareStream("USA", new Fields("tweetmap", "tweetid", "round", "blockEnd"));
        declarer.declareStream("CAN", new Fields("tweetmap", "tweetid", "round", "blockEnd"));
    }

}