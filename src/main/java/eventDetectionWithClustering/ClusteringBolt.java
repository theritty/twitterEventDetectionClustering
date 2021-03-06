package eventDetectionWithClustering;

import cassandraConnector.CassandraDao;
import com.datastax.driver.core.Row;
import algorithms.*;
import drawing.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.util.*;

public class ClusteringBolt extends BaseRichBolt {

    private OutputCollector collector;
    private int componentId;
    private String fileNum;
    private Date lastDate = new Date();
    private Date startDate = new Date();
    private String country ;
    private HashMap<Long, Long> counts = new HashMap<>();
    private CosineSimilarity cosineSimilarity = new CosineSimilarity();
    private ArrayList<Cluster> clusters = new ArrayList<>();

    private double cosine=0;
    private int cosineSize=0;
    private int tweetSize=0;
    private long cosineNum=0L;
    private double clusterUp=0;
    private long clusterUpNum=0L;

    private int updateCntCond = 60;
    private double updateCntPer = 0.02;
    private double similarityThreshold = 0.6;
    private int totCntThre = 120;
    private double newCntPer = 0.06;


//    private int updateCntCond = 50;
//    private double updateCntPer = 0.01;
//    private double similarityThreshold = 0.5;
//    private int totCntThre = 120;
//    private double newCntPer = 0.05;
////
//    private int updateCntCond = 40;
//    private double updateCntPer = 0.01;
//    private double similarityThreshold = 0.4;
//    private int totCntThre = 150;
//    private double newCntPer = 0.04;

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
        this.componentId = context.getThisTaskId()-1;
        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "cluster : " + componentId + " " + country);
    }



    @Override
    public void execute(Tuple tuple) {
        ArrayList<String> tweetmap = (ArrayList<String>) tuple.getValueByField("tweetmap");
        long round = tuple.getLongByField("round");
        long tweetid = tuple.getLongByField("tweetid");
        boolean streamEnd = tuple.getBooleanByField("streamEnd");
        boolean blockEnd = tuple.getBooleanByField("blockEnd");

        Date nowDate = new Date();
//
        if(streamEnd) {
            this.collector.emit(new Values( 0L, country, new ArrayList<>()));
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

                TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "Round finished for " + country + " round " + round + "from bolt " + componentId +  "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

                ArrayList<Cluster> clustersCopy = new ArrayList<>(clusters);

                this.collector.emit(new Values( round, country, clustersCopy));

                clusters.clear();
                counts.remove(round);
                return;

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if(counts.get(round)==null) counts.put(round,1L);
        else counts.put(round, counts.get(round)+1);
        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "workhistory.txt", new Date() + " Clustering " + componentId + " working "  + round);

        try {
            if (tweetmap == null || tweetmap.size() < 3)
                return;

            if (clusters.size() == 0) {
                addNewCluster(tweetmap, tweetid, round);
            } else {
                boolean similarclusterfound = false;

                double magnitude2 = (double) tweetmap.size();
                magnitude2 = Math.sqrt(magnitude2);//sqrt(a^2)

                double maxSim = 0.0;
                for(int i=0; i<clusters.size(); i++) {
                    HashMap<String, Double> clustermap = clusters.get(i).cosinevector;
                    if (clustermap == null) continue;

                    double similarity = cosineSimilarity.cosineSimilarityFromMap(clustermap, tweetmap, magnitude2);
                    if(maxSim<similarity) maxSim=similarity;
                    if (similarity > similarityThreshold) {
                        similarclusterfound = true;
                        updateCluster(clustermap, tweetmap, i, tweetid, round);
                        break;
                    }
                }
                if (!similarclusterfound) {
                    addNewCluster(tweetmap, tweetid, round);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        lastDate = new Date();

        ExcelWriter.putData(componentId,nowDate,lastDate, round,cassandraDao);
        collector.ack(tuple);

    }

    public void updateCluster(HashMap<String, Double> clustermap, ArrayList<String> tweetmap, int index, long tweetid, long round) throws Exception {
        double numTweets = clusters.get(index).currentnumtweets;
        for(String key : tweetmap) {
            double value = 1.0;
            if(clustermap.get(key) != null)
                clustermap.put(key, ((clustermap.get(key)*numTweets ) + value) / numTweets);
            else
                clustermap.put(key, value / numTweets);
        }
        Iterator<Map.Entry<String, Double>> it = clustermap.entrySet().iterator();
        while(it.hasNext()) {
            Map.Entry<String, Double> entry = it.next();
            String key = entry.getKey();
            double value = entry.getValue();
            double newValue = value * numTweets / (numTweets+1.0);
            clustermap.put(key, newValue);
        }

        clusters.get(index).cosinevector = clustermap;
        clusters.get(index).currentnumtweets++;
        clusters.get(index).tweetList.add(tweetid);

    }

    public void addNewCluster(ArrayList<String> tweetmap, long tweetid, long round) throws Exception {
        HashMap<String, Double> tweetMap = new HashMap<>();
        for(String key : tweetmap) {
            tweetMap.put(key, 1.0);
        }
        Cluster newCluster = new Cluster(country, null, tweetMap, 1, round, 0);
        newCluster.tweetList.add(tweetid);
        clusters.add(newCluster);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields( "round", "country", "clusters"));
    }

}