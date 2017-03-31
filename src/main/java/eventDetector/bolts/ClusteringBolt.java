package eventDetector.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
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
    private CosineSimilarity cosineSimilarity = new CosineSimilarity();

    private double cosine=0;
    private int cosineSize=0;
    private int tweetSize=0;
    private long cosineNum=0L;
    private double clusterUp=0;
    private long clusterUpNum=0L;

    private CassandraDao cassandraDao;
    private int CANTaskNumber = 0;
    private int USATaskNumber = 0;
    private int numWorkers = 0;


    public ClusteringBolt(String filenum, CassandraDao cassandraDao, String country, int canTaskNum, int usaTaskNum, int numWorkers)
    {
        this.fileNum = filenum + "/";
        this.cassandraDao = cassandraDao;
        this.country = country;
        this.CANTaskNumber = canTaskNum;
        this.USATaskNumber = usaTaskNum;
        this.numWorkers = numWorkers;
    }
    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.countsForRounds = new HashMap<>();
        this.componentId = context.getThisTaskId()-1;
        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "cluster : " + componentId + " " + country);
    }



    @Override
    public void execute(Tuple tuple) {
        ArrayList<HashMap<String, Double>> tweetmaplist = (ArrayList<HashMap<String, Double>>) tuple.getValueByField("tweetmap");
        long round = tuple.getLongByField("round");
        long tweetid = tuple.getLongByField("tweetid");
        boolean streamEnd = tuple.getBooleanByField("streamEnd");
        boolean blockEnd = tuple.getBooleanByField("blockEnd");

        Date nowDate = new Date();

        if(streamEnd) {
            this.collector.emit(new Values( 0L, country));
            collector.ack(tuple);
            return;
        }
        if(blockEnd) {
            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt",  new Date() + " round end " + round + " for " + country + " for " + componentId);

            try {

                TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", country + " comp id: "  + componentId + ". Cosine size: " + cosineSize + ", tweet size: " + tweetSize + ". Cosine: " +
                        cosine + " and total " + cosineNum*cosine + ", clusterUp: " + clusterUp + "and total " + clusterUp*clusterUpNum +
                        " and time taken is " + (new Date().getTime()-startDate.getTime()) + " at round " + round);
                cosine = 0L;
                cosineSize=0;
                tweetSize=0;
                cosineNum = 0L;
                clusterUp = 0L;
                clusterUpNum = 0L;
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
                    Row r = iteratorByCountry.next();
                    if(r.getInt("boltId")<CANTaskNumber+USATaskNumber+1+numWorkers && !r.getBool("finished")) {
                        collector.ack(tuple);
                        return;
                    }
                }
                TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "Round finished for " + country + " round " + round);
                this.collector.emit(new Values( round, country));
                counts.remove(round);

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
//            System.out.println("tweet ignored " + tweetid);
            ignoredCount++;
            if(ignoredCount%1000==0)
                TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + "ignoreCount.txt",
                        "Clustering ignored count " + componentId + ": " + ignoredCount );
            return;
        }

        for(HashMap<String, Double> tweetmap: tweetmaplist) {
            ResultSet resultSet;
//        Constants.lock.lock();
            try {
                if (tweetmap == null || tweetmap.size() < 3)
                    continue;

                resultSet = cassandraDao.getClusters(country);
                Iterator<Row> iterator = resultSet.iterator();

                if (!iterator.hasNext()) {
                    addNewCluster(round, tweetmap, tweetid);
                } else {
                    boolean similarclusterfound = false;

                    double magnitude2 = 0.0;
                    for (String key : tweetmap.keySet()) {
                        magnitude2 += Math.exp(Math.log(tweetmap.get(key)) * 2);
                    }
                    magnitude2 = Math.sqrt(magnitude2);//sqrt(a^2)

                    while (iterator.hasNext()) {
                        Row row = iterator.next();
                        HashMap<String, Double> cosinevector = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);
                        if (cosinevector == null) continue;

                        long n = new Date().getTime();
                        cosineSize += cosinevector.size();
                        tweetSize += tweetmap.size();
                        double similarity = cosineSimilarity.cosineSimilarityFromMap(cosinevector, tweetmap, magnitude2);
                        cosine = (cosine * (double) cosineNum + (double) (new Date().getTime() - n)) / (double) ++cosineNum;
                        cosinevector.clear();

                        if (similarity > 0.5) {
                            similarclusterfound = true;
                            n = new Date().getTime();
                            updateCluster(row, tweetmap, tweetid, round);
                            clusterUp = (clusterUp * (double) clusterUpNum + (double) (new Date().getTime() - n)) / (double) ++clusterUpNum;
                            break;
                        }
                    }
                    if (!similarclusterfound) {
                        addNewCluster(round, tweetmap, tweetid);
                    }
                }
                tweetmap.clear();

            } catch (Exception e) {
                e.printStackTrace();
//            Constants.lock.unlock();
            }
//        Constants.lock.unlock();
        }
        lastDate = new Date();

        ExcelWriter.putData(componentId,nowDate,lastDate, currentRound,cassandraDao);
        collector.ack(tuple);

    }

    public void updateCluster(Row row, HashMap<String, Double> tweetmap, long tweetid, long round) throws Exception {
        HashMap<String, Double> cosinevector = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);
        int numTweets = row.getInt("prevnumtweets")+row.getInt("currentnumtweets");
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
        values.add(row.getInt("prevnumtweets"));
        values.add(row.getInt("currentnumtweets")+1);
        values.add(row.getLong("lastround"));
        cassandraDao.insertIntoClusters(values.toArray());
//        List<Object> values2 = new ArrayList<>();
//        values2.add(row.getUUID("id"));
//        values2.add(tweetid);
//        cassandraDao.insertIntoClusterAndTweets(values2.toArray());
//        List<Object> values3 = new ArrayList<>();
//        values3.add(round);
//        values3.add(row.getUUID("id"));
//        values3.add(country);
//        values3.add(numTweets+1);
//        cassandraDao.insertIntoClusterinfo(values3.toArray());

    }

    public void addNewCluster(long round, HashMap<String, Double> tweet, long tweetid) throws Exception {
        UUID clusterid = UUIDs.timeBased();
        List<Object> values = new ArrayList<>();
        values.add(clusterid);
        values.add(country);
        values.add(tweet);
        values.add(0);
        values.add(1);
        values.add(0L);
        cassandraDao.insertIntoClusters(values.toArray());
//        List<Object> values2 = new ArrayList<>();
//        values2.add(clusterid);
//        values2.add(tweetid);
//        cassandraDao.insertIntoClusterAndTweets(values2.toArray());
//        List<Object> values3 = new ArrayList<>();
//        values3.add(round);
//        values3.add(clusterid);
//        values3.add(country);
//        values3.add(1);
//        cassandraDao.insertIntoClusterinfo(values3.toArray());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields( "round", "country"));
    }

}