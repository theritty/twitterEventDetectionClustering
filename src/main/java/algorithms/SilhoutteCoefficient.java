package algorithms;

import cassandraConnector.CassandraDao;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import topologyBuilder.TopologyHelper;

import java.io.IOException;
import java.util.*;

/**
 * Created by ceren on 03.12.2017.
 */
public class SilhoutteCoefficient {
    public static class Cluster  {
        public UUID id;
        public HashMap<String, Double> cosinevector;
        public List<HashMap<String, Double>> tweetList;
        public Cluster( UUID id, HashMap<String, Double> cosinevector) {
            this.id = id;
            this.cosinevector = cosinevector;
            this.tweetList = new ArrayList<>();
        }
    }

    public static void main(String[] args) {

        HashMap<Long, List<Cluster>> clusters = new HashMap<>();
        TopologyHelper topologyHelper = new TopologyHelper();
        Properties properties = null;
        try {
            properties = topologyHelper.loadProperties( "config.properties" );
        } catch (IOException e) {
            e.printStackTrace();
        }

        String TWEETS_TABLE = properties.getProperty("clustering.tweets.table");
        String EVENTS_WORDBASED_TABLE = properties.getProperty("clustering.events_wordbased.table");
        String EVENTS_TABLE1 =  properties.getProperty("clustering.events.table");
        String CLUSTER_TABLE = properties.getProperty("clustering.clusters.table");
        String PROCESSEDTWEET_TABLE = properties.getProperty("clustering.processed_tweets.table");
        String PROCESSTIMES_TABLE = properties.getProperty("clustering.processtimes.table");
        String TWEETSANDCLUSTER_TABLE = properties.getProperty("clustering.tweetsandcluster.table");

        CassandraDao cassandraDao;
        try {
            cassandraDao = new CassandraDao(TWEETS_TABLE, CLUSTER_TABLE, EVENTS_TABLE1, EVENTS_WORDBASED_TABLE, PROCESSEDTWEET_TABLE, PROCESSTIMES_TABLE, TWEETSANDCLUSTER_TABLE);
            try {
                ResultSet resultSetx = cassandraDao.getEvents("USA");
                HashMap<Long, Integer> roundCount = new HashMap<>();

                Iterator<Row> iteratorx = resultSetx.iterator();
                while(iteratorx.hasNext()) {
                    Row rowx = iteratorx.next();
                    long round = rowx.getLong("round");

                    if(roundCount.containsKey(round)) roundCount.put(round, roundCount.get(round)+1);
                    else roundCount.put(round,1);
                }

                ResultSet resultSet = cassandraDao.getEvents("USA");
                Iterator<Row> iterator = resultSet.iterator();
                while(iterator.hasNext()) {
                    Row row = iterator.next();
                    UUID clusterid = row.getUUID("clusterid");
                    long round = row.getLong("round");

                    if(roundCount.get(round)<2) continue;
                    HashMap<String, Double> cosinevector = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);

                    Cluster c = new Cluster(clusterid, cosinevector);

                    ResultSet resultSet2 = cassandraDao.getClusterTweets(round, clusterid);
                    Iterator<Row> iterator2 = resultSet2.iterator();
                    while (iterator2.hasNext()) {
                        Row row2 = iterator2.next();
                        long tweetid = row2.getLong("tweetid");

                        ResultSet resultSet3 = cassandraDao.getTweetsById(round, "USA", tweetid);
                        Iterator<Row> iterator3 = resultSet3.iterator();
                        while (iterator3.hasNext()) {
                            Row row3 = iterator3.next();
                            String tweet = row3.getString("tweet");
                            List<String> tweets = Arrays.asList(tweet.split(" "));
                            HashMap<String, Double> tweetMap = new HashMap<>();
                            for (String key : tweets) {
                                tweetMap.put(key, 1.0);
                            }
                            c.tweetList.add(tweetMap);
                        }
                    }
                    if (clusters.containsKey(round)) {
                        clusters.get(round).add(c);
                    } else {
                        List<Cluster> newRound = new ArrayList<>();
                        newRound.add(c);
                        clusters.put(round, newRound);
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        for(Map.Entry<Long, List<Cluster>> entry : clusters.entrySet()) {
            long key = entry.getKey();
            List<Cluster> value = entry.getValue();

            if(value.size()<2) continue;
            System.out.println("Round: "+ key + " -----------------");
            for(int i=0; i<value.size(); i++) {
                avgSilhoutteCoefForCluster(value, i);
            }
        }
        System.out.println("DONE");
    }

    public static double averageSimilarityInsideCluster(Cluster c, int objIndex, HashMap<String, Double> currentVector) {
        double sum = 0.0;
        int count = 0;
        CosineSimilarity sim = new CosineSimilarity();
        for(HashMap h : c.tweetList) {
            if(count++ != objIndex) {
                sum += sim.cosineSimilarityFromMap(currentVector, h);
            }
        }
        return sum/((double)count);
    }

    public static double maxSimilarityBetweenClusters(List<Cluster> clusters, int clusterIndex, int objIndex) {
        int ind = 0;
        double maxSim = 0.0;
        for(Cluster c : clusters) {
            if(ind++ != clusterIndex) {
                double clusterSim = averageSimilarityInsideCluster(c, -1, clusters.get(clusterIndex).tweetList.get(objIndex));
                if(maxSim < clusterSim) maxSim = clusterSim;
            }
        }
        return maxSim;
    }

    public static double silhoutteCoefForObject(List<Cluster> clusters, int clusterIndex, int objIndex) {
        double a = averageSimilarityInsideCluster(clusters.get(clusterIndex), objIndex, clusters.get(clusterIndex).tweetList.get(objIndex));
        double b = maxSimilarityBetweenClusters(clusters, clusterIndex, objIndex);

        //System.out.println("a: " + a + ", b: " + b);
        if(a > b) {
            return (a-b)/a;
        }
        else {
            return (a-b)/b;
        }
    }

    public static double avgSilhoutteCoefForCluster(List<Cluster> clusters, int clusterIndex) {
        double coeff = 0.0;
        double minCoeff = Double.MAX_VALUE;
        double maxCoeff = Double.MIN_VALUE;
        int count = 0;

        for(int i =0; i<clusters.get(clusterIndex).tweetList.size(); i++) {
            double curCoef = silhoutteCoefForObject(clusters, clusterIndex, i);
            coeff += curCoef;
            if(curCoef > maxCoeff) maxCoeff = curCoef;
            if(curCoef < minCoeff) minCoeff = curCoef;
            count++;
        }

        double avgCoeff = coeff/((double)count);
        System.out.println("ID: " + clusters.get(clusterIndex).id + ", maxCoeff: " + maxCoeff + ", minCoeff: " + minCoeff + ", avgCoeff: " + avgCoeff);
        return avgCoeff;
    }

}
