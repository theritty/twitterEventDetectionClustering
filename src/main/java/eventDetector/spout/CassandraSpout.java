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
    private ArrayList<Long> readRoundlist;
    private int compareSize;
    private int trainSize;
    private int testSize;
    private int componentId;
    private Iterator<Row> iterator = null;
    private long current_round;
    private long count_tweets = 0;
    private String fileNum;
    private boolean start = true;
    private Date startDate = new Date();
    private Date lastDate = new Date();

    private long startRound = 2033719;
    private HashMap<String, Integer> vectorMap;
    private int vectorMapLength = 0;


    public CassandraSpout(CassandraDao cassandraDao, int trainSize, int compareSize, int testSize, String filenum) throws Exception {
        this.cassandraDao = cassandraDao;
        this.compareSize = compareSize;
        this.trainSize = trainSize;
        this.testSize = testSize;
        roundlist = new ArrayList<>();
        readRoundlist = new ArrayList<>();
        this.fileNum = filenum + "/";
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
                    collector.emit("CAN", new Values("dummy", 0L,current_round+1, true, new ArrayList<Long>()));
                    try {
                            Thread.sleep(120000);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    collector.emit("CAN", new Values("dummyBLOCKdone", 0L,current_round+1, true, new ArrayList<Long>()));

                    System.out.println("Number of tweets: " + count_tweets);
                    Thread.sleep(10000000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return;
            }

            current_round = roundlist.remove(0);
            readRoundlist.add(current_round);
            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + current_round + ".txt",
                    new Date() + ": Round submission from cass spout =>" + current_round );

            if (readRoundlist.size() > compareSize) readRoundlist.remove(0);

//            try {
                if(!start) {
//                    TopologyHelper.writeToFile("/Users/ozlemcerensahin/Desktop/workhistory.txt", new Date() + " Cass sleeping " + current_round);
//                    Thread.sleep(120000);
//                    TopologyHelper.writeToFile("/Users/ozlemcerensahin/Desktop/workhistory.txt", new Date() + " Cass wake up " + current_round);
                    ExcelWriter.putData(componentId,startDate,lastDate, "cassSpout", "both", current_round);
                }
                else start = false;
//            }
//            catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            startDate = new Date();

            ResultSet resultSet = getDataFromCassandra(current_round);
            iterator = resultSet.iterator();
        }
        Row row = iterator.next();
        String tweet = row.getString("tweet");
        String country = row.getString("country");

        if(tweet == null || tweet.length() == 0) return;
        ArrayList<Long> tmp_roundlist = new ArrayList<>(readRoundlist);

        if(iterator.hasNext()) {
            vectorizeAndEmit(tweet, row.getLong("id"), current_round, tmp_roundlist, country);
        }
        else {
            vectorizeAndEmit(tweet, row.getLong("id"), current_round, tmp_roundlist, country);
            collector.emit("USA", new Values("BLOCKEND", 0L, current_round, true, tmp_roundlist));
            collector.emit("CAN", new Values("BLOCKEND", 0L, current_round, true, tmp_roundlist));
            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + current_round + ".txt",
                    new Date() + ": Round end from cass spout =>" + current_round );

        }
        lastDate = new Date();


        try {
//                TopologyHelper.writeToFile("/Users/ozlemcerensahin/Desktop/workhistory.txt", new Date() + " Cass sleeping " + current_round);
                Thread.sleep(5);
//                TopologyHelper.writeToFile("/Users/ozlemcerensahin/Desktop/workhistory.txt", new Date() + " Cass wake up " + current_round);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void vectorizeAndEmit(String tweetSentence, long id, long round, ArrayList<Long> roundlist, String country) {
        List<String> tweets = Arrays.asList(tweetSentence.split(" "));
        ArrayList<Integer> tweetVector = new ArrayList<>();
        for(int i= 0; i< vectorMapLength;i++) tweetVector.add(0);

        for (String tweet : tweets) {
            Integer ind = vectorMap.get(tweet);
            if(ind != null)
                tweetVector.set(ind,1);
        }
        collector.emit(country, new Values(tweetVector, id, round, false, roundlist));
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

            if(testSize!=Integer.MAX_VALUE) {
                while (roundlist.size() > testSize + trainSize)
                    roundlist.remove(roundlist.size() - 1);
            }

            int i = 0;
            while(trainSize>i++)
                readRoundlist.add(roundlist.remove(0));

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
                vectorMap.put(line,index++);
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
        declarer.declareStream("USA", new Fields("word", "tweetid", "round", "blockEnd", "dates"));
        declarer.declareStream("CAN", new Fields("word", "tweetid", "round", "blockEnd", "dates"));
    }

}