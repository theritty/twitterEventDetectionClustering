package eventDetectionHybrid;

import algorithms.*;
import cassandraConnector.CassandraDaoHybrid;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import drawing.*;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import eventDetectionWithClustering.Cluster;
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
    private ArrayList<Cluster> clusters = new ArrayList<>();

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
        System.out.println( "clustering : " + componentId + " " + country );
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
                long tweetid = row.getLong("id");
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
                        addNewCluster(tweetmap, tweetid, round);
                    } else {
                        int indexOfSimilarCluster = getIndexOfSimilarCluster(tweetmap);
                        if(indexOfSimilarCluster == -1) addNewCluster(tweetmap, tweetid, round);
                        else updateCluster(clusters.get(indexOfSimilarCluster), tweetmap, indexOfSimilarCluster, tweetid);
                    }
                }
            }
            double maxNum=0.0;
            for(Cluster m:clusters) {
                if(m.currentnumtweets>maxNum) maxNum=m.currentnumtweets;
            }
            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "clustering end putting :::: " + componentId  + " num tweets for selection " + numTweets  + " max::::: " + maxNum);
            System.out.println("clustering end putting " + round + " :::: " + componentId  + " num tweets for selection " + numTweets  + " max::::: " + maxNum);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void updateCluster(Cluster cluster1, Cluster cluster2) {
        double numTweets1 = cluster1.currentnumtweets;
        double numTweets2 = cluster2.currentnumtweets;

        Iterator<Map.Entry<String, Double>> it = cluster1.cosinevector.entrySet().iterator();
        while(it.hasNext()) {
            Map.Entry<String, Double> entry = it.next();
            String key = entry.getKey();
            if(cluster2.cosinevector.containsKey(key)){
                double value1 = entry.getValue();
                double value2 = cluster2.cosinevector.get(key);
                double newValue = (value1 * numTweets1 + value2 * numTweets2) / (numTweets1+numTweets2);
                if(numTweets1+numTweets2>50 && newValue<0.03) {
                    it.remove();
                }
                else {
                    cluster1.cosinevector.put(key, newValue);
                }
                cluster2.cosinevector.remove(key);
            }
        }
        Iterator<Map.Entry<String, Double>> it2 = cluster2.cosinevector.entrySet().iterator();
        while(it2.hasNext()) {
            Map.Entry<String, Double> entry = it2.next();
            String key = entry.getKey();
            double value = entry.getValue();
            double newValue = (value * numTweets2) / (numTweets1+numTweets2);
            if(numTweets1+numTweets2<50 || newValue>0.03) {
                cluster1.cosinevector.put(key, newValue);
            }
        }

    }

    public void mergeClusters() {
        for(int i=0;i<clusters.size()-1;i++) {
            for(int j=i+1; j< clusters.size();){
                double similarity = cosineSimilarity.cosineSimilarityFromMap(clusters.get(j).cosinevector, clusters.get(i).cosinevector);
                if(similarity>0.5) {
                    updateCluster(clusters.get(i), clusters.get(j));
                    clusters.get(i).currentnumtweets += clusters.get(j).currentnumtweets;
                    clusters.get(i).tweetList.addAll(clusters.get(j).tweetList);
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
            HashMap<String, Double> clustermap = clusters.get(i).cosinevector;
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

        ArrayList<Cluster> clustersCopy = new ArrayList<>(clusters);
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


    public void updateCluster(Cluster clustermap, ArrayList<String> tweetmap, int index, long tweetId) throws Exception {

        double numTweets = clustermap.currentnumtweets;
        for(String key : tweetmap) {
            double value = 1.0;
            if(clustermap.cosinevector.get(key) != null)
                clustermap.cosinevector.put(key, ((clustermap.cosinevector.get(key)*numTweets ) + value) / numTweets);
            else
                clustermap.cosinevector.put(key, value / numTweets);
        }
        Iterator<Map.Entry<String, Double>> it = clustermap.cosinevector.entrySet().iterator();
        while(it.hasNext()) {
            Map.Entry<String, Double> entry = it.next();
            String key = entry.getKey();
            double value = entry.getValue();
            double newValue = value * numTweets / (numTweets+1.0);
            clustermap.cosinevector.put(key, newValue);
        }



        clustermap.currentnumtweets++;
        clustermap.tweetList.add(tweetId);

        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "Updated cluster........... " + clustermap  );

    }

    public void addNewCluster(ArrayList<String> tweetmap, long tweetId, long round) throws Exception {
        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", "New cluster..........."  );
        HashMap<String, Double> tweetMap = new HashMap<>();
        for(String key : tweetmap) {
            tweetMap.put(key, 1.0);
        }

        Cluster newCluster = new Cluster(country, null, tweetMap, 1, round, 0);
        newCluster.tweetList.add(tweetId);
        clusters.add(newCluster);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields( "round", "country", "clusters"));
    }
}
