package eventDetectionHybrid;

import algorithms.*;
import cassandraConnector.CassandraDaoHybrid;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

public class ClusteringBoltHybrid extends BaseRichBolt {

    private OutputCollector collector;
    private CassandraDaoHybrid cassandraDao;
    private int componentId;
    private String fileNum;
    private ArrayList<String> words;
    private int numWordCountBolts;
    private ArrayList<Integer> numWordCountBoltsForRound;
    private HashMap<String, Integer> vectorMap;
    private String country;
    private long finalRound = 0;
    private CosineSimilarity cosineSimilarity = new CosineSimilarity();
    private ArrayList<HashMap<String, Double>> clusters = new ArrayList<>();

    public ClusteringBoltHybrid(CassandraDaoHybrid cassandraDao, String filePath, String fileNum, double tfidfEventRate, int compareSize, String country, int numWordCountBolts )
    {
        this.cassandraDao = cassandraDao;
        this.fileNum = fileNum + "/";
        this.words = new ArrayList<>();
        this.country = country;
        this.numWordCountBolts = numWordCountBolts;
        this.numWordCountBoltsForRound = new ArrayList<>();
        createWordsMap();
    }

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;

        this.componentId = context.getThisTaskId()-1;
        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "clustering : " + componentId  );
        System.out.println( "clustering : " + componentId  );
    }

    @Override
    public void execute(Tuple tuple) {
        String wrd = tuple.getStringByField("key");
        long round = tuple.getLongByField("round");
        boolean blockend = tuple.getBooleanByField("blockEnd");
        int comingCompId = tuple.getIntegerByField("compId");

        Date nowDate = new Date();
        if(!blockend) {
            if(!words.contains(wrd))
                words.add(wrd);
            collector.ack(tuple);

            ExcelWriter.putData(componentId,nowDate,new Date(), round, cassandraDao);
            return;
        }

        if(!numWordCountBoltsForRound.contains(comingCompId) && finalRound<round) {
            numWordCountBoltsForRound.add(comingCompId);
            System.out.println("blockend came for " + componentId + " from " + comingCompId);
        }

        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "Detector " + componentId + " num: " + numWordCountBoltsForRound + " " + numWordCountBolts);
        if(numWordCountBolts == numWordCountBoltsForRound.size()) {

            clusterAssignment(round,country, words);
            mergeClusters();
            markComponentAsFinishedInCassandra(round);
            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "clustering end for round " + round + ": " + componentId + " num of words: " + words.size() + " number of clusters: " + clusters.size() );
            System.out.println("clustering end : " + componentId + " num of words: " + words.size() + " number of clusters: " + clusters.size() );
            System.out.println(clusters);

            endOfRoundOperations(round);

            collector.ack(tuple);
            words.clear();
            numWordCountBoltsForRound.clear();
        }
        else{
            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "clustering not end : " + componentId + " " + numWordCountBolts + " " +numWordCountBoltsForRound.size()  );
        }
        ExcelWriter.putData(componentId,nowDate,new Date(), round, cassandraDao);

    }

    private void clusterAssignment(long round, String country, ArrayList<String> eventCandidates) {

        ResultSet resultSet ;
        try {
            resultSet = cassandraDao.getTweetsByRoundAndCountry(round, country);
            Iterator<Row> iterator = resultSet.iterator();
            int numTweets = 0;
            while(iterator.hasNext()) {
                Row row = iterator.next();
                String tweet = row.getString("tweet");
                if (eventCandidates.parallelStream().anyMatch(tweet::contains)) {
                    numTweets++;
                    List<String> tweets = Arrays.asList(tweet.split(" "));
                    ArrayList<String> tweetmap = new ArrayList<>();
                    for (String t : tweets) {
                        if (t.length() >= 3 && vectorMap.get(t) != null)
                            tweetmap.add(t);
                    }
                    if ( tweetmap.size() < 3 ) continue;
                    if ( clusters.size() == 0 ) {
                        addNewCluster(tweetmap);
                    } else {
                        int indexOfSimilarCluster = getIndexOfSimilarCluster(tweetmap);
                        if(indexOfSimilarCluster == -1) addNewCluster(tweetmap);
                        else updateCluster(clusters.get(indexOfSimilarCluster), tweetmap, indexOfSimilarCluster);
                    }
                }
            }
            double maxNum=0.0;
            for(HashMap<String,Double> m:clusters) {
                if(m.get("numTweets")>maxNum) maxNum=m.get("numTweets");
            }
            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "clustering end putting :::: " + componentId  + " num tweets for selection " + numTweets  + " max::::: " + maxNum);
            System.out.println("clustering end putting " + round + " :::: " + componentId  + " num tweets for selection " + numTweets  + " max::::: " + maxNum);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void updateCluster(HashMap<String, Double> cluster1, HashMap<String, Double> cluster2) {
        double numTweets1 = cluster1.get("numTweets");
        double numTweets2 = cluster2.get("numTweets");

        Iterator<Map.Entry<String, Double>> it = cluster1.entrySet().iterator();
        while(it.hasNext()) {
            Map.Entry<String, Double> entry = it.next();
            String key = entry.getKey();
            if(cluster2.containsKey(key)){
                double value1 = entry.getValue();
                double value2 = cluster2.get(key);
                double newValue = (value1 * numTweets1 + value2 * numTweets2) / (numTweets1+numTweets2);
                if(numTweets1+numTweets2>50 && newValue<0.03) {
                    it.remove();
                }
                else {
                    cluster1.put(key, newValue);
                    cluster1.put("numTweets", numTweets1+numTweets2);
                }
                cluster2.remove(key);
            }
        }
        Iterator<Map.Entry<String, Double>> it2 = cluster2.entrySet().iterator();
        while(it2.hasNext()) {
            Map.Entry<String, Double> entry = it2.next();
            String key = entry.getKey();
            double value = entry.getValue();
            double newValue = (value * numTweets2) / (numTweets1+numTweets2);
            if(numTweets1+numTweets2<50 || newValue>0.03) {
                cluster1.put(key, newValue);
            }
        }
    }

    public void mergeClusters() {
        for(int i=0;i<clusters.size()-1;i++) {
            for(int j=i+1; j< clusters.size();){
                double similarity = cosineSimilarity.cosineSimilarityFromMap(clusters.get(j), clusters.get(i));
                if(similarity>0.5) {
                    updateCluster(clusters.get(i), clusters.get(j));
                    clusters.remove(j);
                }
                else j++;
            }
        }
    }


    private int getIndexOfSimilarCluster(ArrayList<String> tweetmap) {
        double magnitude2 = (double) tweetmap.size();
        magnitude2 = Math.sqrt(magnitude2);//sqrt(a^2)

        double maxSim = 0.0;
        for (int i = 0; i < clusters.size(); i++) {
            HashMap<String, Double> clustermap = clusters.get(i);
            if (clustermap == null) continue;

            double similarity = cosineSimilarity.cosineSimilarityFromMap(clustermap, tweetmap, magnitude2);
            if (maxSim < similarity) maxSim = similarity;
            if (similarity > 0.5) {
                return i;
            }
        }
        return -1;
    }

    private void endOfRoundOperations(long round) {

        ArrayList<HashMap<String, Double>> clustersCopy = new ArrayList<>(clusters);
        this.collector.emit(new Values( round, country, clustersCopy));
        clusters.clear();

        TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + round + ".txt",
                "Detector bolt " + componentId + " end of round " + round + " at " + new Date());
        finalRound = round;
    }

    private void markComponentAsFinishedInCassandra(long round) {

        try {
            List<Object> values = new ArrayList<>();
            values.add(round);
            values.add(componentId);
            values.add(true);
            cassandraDao.insertIntoProcessed(values.toArray());
        } catch (Exception e) {
            e.printStackTrace();
        }
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
        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "Updated cluster........... " + clustermap  );

    }

    public void addNewCluster(ArrayList<String> tweetmap) throws Exception {
        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "New cluster..........."  );
        HashMap<String, Double> tweetMap = new HashMap<>();
        for(String key : tweetmap) {
            tweetMap.put(key, 1.0);
        }
        tweetMap.put("numTweets",1.0);

        clusters.add(tweetMap);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields( "round", "country", "clusters"));
    }
}
