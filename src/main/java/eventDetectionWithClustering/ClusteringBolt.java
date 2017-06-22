package eventDetectionWithClustering;

import cassandraConnector.CassandraDao;
import com.datastax.driver.core.Row;
import algorithms.*;
import drawing.*;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
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
    private ArrayList<HashMap<String, Double>> clusters = new ArrayList<>();

    private double cosine=0;
    private int cosineSize=0;
    private int tweetSize=0;
    private long cosineNum=0L;
    private double clusterUp=0;
    private long clusterUpNum=0L;

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

                ArrayList<HashMap<String, Double>> clustersCopy = new ArrayList<>(clusters);
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
                    addNewCluster(tweetmap);
                } else {
                    boolean similarclusterfound = false;

                    double magnitude2 = (double) tweetmap.size();
                    magnitude2 = Math.sqrt(magnitude2);//sqrt(a^2)

                    double maxSim = 0.0;
                    for(int i=0; i<clusters.size(); i++) {
                        HashMap<String, Double> clustermap = clusters.get(i);
                        if (clustermap == null) continue;

                        double similarity = cosineSimilarity.cosineSimilarityFromMap(clustermap, tweetmap, magnitude2);
                        if(maxSim<similarity) maxSim=similarity;
                        if (similarity > 0.5) {
                            similarclusterfound = true;
                            updateCluster(clustermap, tweetmap, i);
                            break;
                        }
                    }
                    if (!similarclusterfound) {
                        addNewCluster(tweetmap);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        lastDate = new Date();

        ExcelWriter.putData(componentId,nowDate,lastDate, round,cassandraDao);
        collector.ack(tuple);

    }

    public void updateCluster(HashMap<String, Double> clustermap, ArrayList<String> tweetmap, int index) throws Exception {
        double numTweets = clustermap.get("numTweets");
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

        clustermap.put("numTweets", numTweets+1.0);
        clusters.remove(index);
        clusters.add(clustermap);

    }

    public void addNewCluster(ArrayList<String> tweetmap) throws Exception {
        HashMap<String, Double> tweetMap = new HashMap<>();
        for(String key : tweetmap) {
            tweetMap.put(key, 1.0);
        }
        tweetMap.put("numTweets",1.0);

        clusters.add(tweetMap);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields( "round", "country", "clusters"));
    }

}