package eventDetector.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cassandraConnector.CassandraDao;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import eventDetector.algorithms.CosineSimilarity;
import eventDetector.drawing.ExcelWriter;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;
import com.datastax.driver.core.utils.UUIDs;

import java.util.*;

public class ClusteringBolt extends BaseRichBolt {

    private OutputCollector collector;
    private HashMap<String, Long> countsForRounds = null;
    private long currentRound = 0;
    private long ignoredCount = 0;
    private int componentId;
    private String fileNum;
    private Date lastDate = new Date();
    private Date startDate = new Date();
    private String country ;
    private HashMap<Long, Long> counts = new HashMap<>();

    private CassandraDao cassandraDao;


    public ClusteringBolt(String filenum, CassandraDao cassandraDao, String country)
    {
        this.fileNum = filenum + "/";
        this.cassandraDao = cassandraDao;
        this.country = country;
    }
    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.countsForRounds = new HashMap<>();
        this.componentId = context.getThisTaskId()-1;
        System.out.println("cluster : " + componentId + " " + country);
    }

    public void cleanupclusters(long round) {
        System.out.println("Clean up for round " + round);
        ResultSet resultSet ;
//        Constants.lock.lock();
        try {
            resultSet = cassandraDao.getClusters(country);
            Iterator<Row> iterator = resultSet.iterator();
            while (iterator.hasNext()) {
                Row row = iterator.next();
                long lastround = row.getLong("lastround");
                int numberoftweets = row.getInt("numberoftweets");
                if(round - lastround>= 2 || numberoftweets<30) {
                    UUID clusterid = row.getUUID("id");
                    cassandraDao.deleteFromClusters(country, clusterid);
                }
                else {
                    HashMap<String, Double> cosinevector = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);
                    int numTweets = row.getInt("numberoftweets");
                    Iterator<Map.Entry<String, Double>> it = cosinevector.entrySet().iterator();
                    while(it.hasNext()) {
                        Map.Entry<String, Double> entry = it.next();
                        double value = entry.getValue();
                        if(value < 0.01) {
                            it.remove();
                        }
                    }
                    List<Object> values = new ArrayList<>();
                    values.add(row.getUUID("id"));
                    values.add(country);
                    values.add(cosinevector);
                    values.add(numTweets);
                    values.add(round);
                    cassandraDao.insertIntoClusters(values.toArray());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
//            Constants.lock.unlock();
        }
//        Constants.lock.unlock();
    }

    @Override
    public void execute(Tuple tuple) {
        HashMap<String, Double> tweetmap = (HashMap<String, Double>) tuple.getValueByField("tweetmap");
        long round = tuple.getLongByField("round");
        long tweetid = tuple.getLongByField("tweetid");
        boolean streamEnd = tuple.getBooleanByField("streamEnd");
        boolean blockEnd = tuple.getBooleanByField("blockEnd");

        Date nowDate = new Date();

        if(streamEnd) {
            this.collector.emit(new Values( 0L, tuple.getSourceStreamId()));
            return;
        }
        if(blockEnd) {
            System.out.println( new Date() + " round end " + round + " for " + country + " for " + componentId);
            this.collector.emit(new Values( round, tuple.getSourceStreamId()));

            try {
                Iterator<Row> iteratorProcessed = cassandraDao.getProcessed(round, componentId).iterator();
                List<Object> values = new ArrayList<>();
                values.add(round);
                values.add(componentId);
                values.add(iteratorProcessed.next().getLong("spoutSent"));
                values.add(counts.get(round));
                values.add(true);
                values.add(country);
                cassandraDao.insertIntoProcessed(values.toArray());

                Iterator<Row> iteratorByCountry = cassandraDao.getProcessedByCountry(round, country).iterator();
                while (iteratorByCountry.hasNext()){
                    if(!iteratorByCountry.next().getBool("finished"))
                        return;
                }
                cleanupclusters(round);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if(counts.get(round)==null) counts.put(round,1L);
        else counts.put(round, counts.get(round)+1);

        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "workhistory.txt", new Date() + " Clustering " + componentId + " working "  + round);

        if(round > currentRound)
        {
//            System.out.println("Count " + counts.get(currentRound) + " for round " + currentRound + " and component " + componentId);
            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
                    "Clustering "+ componentId + " end for round " + currentRound + " at " + lastDate);

            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
                    "Clustering "+ componentId + " time taken for round" + currentRound + " is " +
                            (lastDate.getTime()-startDate.getTime())/1000);

            startDate = new Date();
            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + round + ".txt",
                    "Clustering "+ componentId + " starting for round " + round + " at " + startDate );

            countsForRounds.clear();
            currentRound = round;
        }
        else if(round < currentRound) {
            System.out.println("tweet ignored " + tweetid);
            ignoredCount++;
            if(ignoredCount%1000==0)
                TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + "ignoreCount.txt",
                        "Clustering ignored count " + componentId + ": " + ignoredCount );
            return;
        }

        ResultSet resultSet ;
//        Constants.lock.lock();
        try {
            resultSet = cassandraDao.getClusters(country);
            Iterator<Row> iterator = resultSet.iterator();

            if(!iterator.hasNext()) {
                addNewCluster(round, tweetmap, tweetid);
            }
            else {
                boolean similarclusterfound = false;
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    HashMap<String, Double> cosinevector = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);
                    if (cosinevector == null) continue;

                    double similarity = CosineSimilarity.cosineSimilarityFromMap(cosinevector, tweetmap);
                    if(similarity > 0.5) {
                        similarclusterfound = true;
                        updateCluster(row, tweetmap, tweetid, round);
                        break;
                    }
                }
                if(!similarclusterfound) {
                    addNewCluster(round, tweetmap, tweetid);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
//            Constants.lock.unlock();
        }
//        Constants.lock.unlock();
        lastDate = new Date();

        ExcelWriter.putData(componentId,nowDate,lastDate, currentRound);

    }

    public void updateCluster(Row row, HashMap<String, Double> tweetmap, long tweetid, long round) throws Exception {
        HashMap<String, Double> cosinevector = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);
        int numTweets = row.getInt("numberoftweets");
        for(Map.Entry<String, Double> entry : tweetmap.entrySet()) {
            String key = entry.getKey();
            double value = entry.getValue();
            if(cosinevector.get(key) != null)
                cosinevector.put(key, ((cosinevector.get(key)*numTweets ) + value) / numTweets);
            else
                cosinevector.put(key, value / numTweets);
        }

        Iterator<Map.Entry<String, Double>> it = cosinevector.entrySet().iterator();
        while(it.hasNext()) {
            Map.Entry<String, Double> entry = it.next();
            String key = entry.getKey();
            double value = entry.getValue();
            double newValue = value * numTweets / (numTweets+1);
//            if(newValue < 0.001) {
//                it.remove();
//            }
//            else {
                cosinevector.put(key, newValue);
//            }
        }
        List<Object> values = new ArrayList<>();
        values.add(row.getUUID("id"));
        values.add(country);
        values.add(cosinevector);
        values.add(numTweets+1);
        values.add(round);
        cassandraDao.insertIntoClusters(values.toArray());
        List<Object> values2 = new ArrayList<>();
        values2.add(row.getUUID("id"));
        values2.add(tweetid);
        cassandraDao.insertIntoClusterAndTweets(values2.toArray());
        values = new ArrayList<>();
        values.add(round);
        values.add(row.getUUID("id"));
        values.add(country);
        values.add(numTweets+1);
        cassandraDao.insertIntoClusterinfo(values.toArray());

    }

    public void addNewCluster(long round, HashMap<String, Double> tweet, long tweetid) throws Exception {
        UUID clusterid = UUIDs.timeBased();
        List<Object> values = new ArrayList<>();
        values.add(clusterid);
        values.add(country);
        values.add(tweet);
        values.add(1);
        values.add(round);
        cassandraDao.insertIntoClusters(values.toArray());
        values = new ArrayList<>();
        values.add(clusterid);
        values.add(tweetid);
        cassandraDao.insertIntoClusterAndTweets(values.toArray());
        values = new ArrayList<>();
        values.add(round);
        values.add(clusterid);
        values.add(country);
        values.add(1);
        cassandraDao.insertIntoClusterinfo(values.toArray());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields( "round", "country"));
    }

}