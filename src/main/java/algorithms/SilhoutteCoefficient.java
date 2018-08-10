package algorithms;

import cassandraConnector.CassandraDao;
import clojure.lang.IFn;
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

    public static double getMean(List<Double> data)
    {
        double sum = 0.0;
        for(double a : data)
            sum += a;
        return sum/data.size();
    }

    public static double getVariance(List<Double> data)
    {
        double mean = getMean(data);
        double temp = 0;
        for(double a :data)
            temp += (a-mean)*(a-mean);
        return temp/(data.size()-1);
    }

    public static double getStdDev(List<Double> data)
    {
        return Math.sqrt(getVariance(data));
    }


    public static void main(String[] args) {
        getResult("USA", "clustering");
        getResult("CAN", "clustering");
        getResult("USA", "hybrid");
        getResult("CAN", "hybrid");

    }
    public static void getResult(String country, String mode) {

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
        String EVENTS_TABLE1 =  properties.getProperty(mode + ".events.table");
        String CLUSTER_TABLE = properties.getProperty("clustering.clusters.table");
        String PROCESSEDTWEET_TABLE = properties.getProperty("clustering.processed_tweets.table");
        String PROCESSTIMES_TABLE = properties.getProperty("clustering.processtimes.table");
        String TWEETSANDCLUSTER_TABLE = properties.getProperty(mode + ".tweetsandcluster.table");
        List<Long> rounds = new ArrayList<>();
        CassandraDao cassandraDao;
        try {
            cassandraDao = new CassandraDao(TWEETS_TABLE, CLUSTER_TABLE, EVENTS_TABLE1, EVENTS_WORDBASED_TABLE, PROCESSEDTWEET_TABLE, PROCESSTIMES_TABLE, TWEETSANDCLUSTER_TABLE);
            try {
                ResultSet resultSetx = cassandraDao.getEvents(country);
                HashMap<Long, Integer> roundCount = new HashMap<>();

                Iterator<Row> iteratorx = resultSetx.iterator();
                while(iteratorx.hasNext()) {
                    Row rowx = iteratorx.next();
                    long round = rowx.getLong("round");

                    if(roundCount.containsKey(round)) roundCount.put(round, roundCount.get(round)+1);
                    else roundCount.put(round,1);
                }

                ResultSet resultSet = cassandraDao.getEvents(country);
                Iterator<Row> iterator = resultSet.iterator();
                while(iterator.hasNext()) {
                    Row row = iterator.next();
                    UUID clusterid = row.getUUID("clusterid");
                    long round = row.getLong("round");

                    if(!rounds.contains(round)) rounds.add(round);

//                    if(roundCount.get(round)<2) continue;
                    HashMap<String, Double> cosinevector = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);

                    Cluster c = new Cluster(clusterid, cosinevector);

                    ResultSet resultSet2 = cassandraDao.getClusterTweets(round);
                    Iterator<Row> iterator2 = resultSet2.iterator();
                    while (iterator2.hasNext()) {
                        Row row2 = iterator2.next();
                        long tweetid = row2.getLong("tweetid");
                        UUID clusteridx = row2.getUUID("clusterid");
                        if(!clusterid.equals(clusteridx)) continue;

                        ResultSet resultSet3 = cassandraDao.getTweetsById(round, country, tweetid);
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

        int clusterNum = 0;
        int oneClusterNum = 0;
        List<Double> coeffs = new ArrayList<>();
        Collections.sort(rounds);

        for(long r : rounds){
            long key = r;
            List<Cluster> value = clusters.get(r);
            clusterNum += value.size();


            if(value.size()<2) oneClusterNum++;

//            System.out.println("Round: "+ key + " -----------------");

            for(int i=0; i<value.size(); i++) {
                coeffs.add(avgSilhoutteCoefForCluster(value, i, key));
            }


        }

        double var = getStdDev(coeffs);
        double min = 100000000.0;
        double max = -100000000.0;
        double avg = 0;
        double avgMed = 0;
        int medCnt = 0;
        for(double t : coeffs) {
            avg+=t;
            if(t>0.0 && t<1.0) {
                medCnt++;
                avgMed+=t;
            }
            if(min > t) min = t;
            if(max < t) max = t;
        }
        avg = avg / clusterNum;
        avgMed = avgMed / medCnt;
        System.out.println(mode + "-" + country + " -> num clusters: " + clusterNum + ", number of rounds containing one cluster: " + oneClusterNum + ", avg: " + avg + ", avg of (0,1): " + avgMed + ", min: " + min + ", max: " + max + ", std-dev: " + var) ;
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


        double avgDistInsideCluster = 1 - a;
        double minDistToOtherClusters = 1 - b;

        return (minDistToOtherClusters - avgDistInsideCluster) / Math.max(minDistToOtherClusters,avgDistInsideCluster);
//        System.out.println(a + " - " + b + " : " + (a-b)/Math.max(a,b) + " AND " + avgDistInsideCluster + "  -  " + minDistToOtherClusters + " : " + (minDistToOtherClusters - avgDistInsideCluster) / Math.max(minDistToOtherClusters,avgDistInsideCluster));
//        return (a-b)/Math.max(a,b);
    }

    public static double avgSilhoutteCoefForCluster(List<Cluster> clusters, int clusterIndex, long round) {
        double coeff = 0.0;
        double minCoeff = Double.MAX_VALUE;
        double maxCoeff = Double.MIN_VALUE;
        int count = 0;
        double avgCoeff = 1.0;
        if(clusters.size()>1) {
            for (int i = 0; i < clusters.get(clusterIndex).tweetList.size(); i++) {
                double curCoef = silhoutteCoefForObject(clusters, clusterIndex, i);
                coeff += curCoef;
                if (curCoef > maxCoeff) maxCoeff = curCoef;
                if (curCoef < minCoeff) minCoeff = curCoef;
                count++;
            }
            avgCoeff = coeff / ((double) count);
        }

        List<String> commonWrds = new ArrayList<>();
        for (String word : clusters.get(clusterIndex).cosinevector.keySet()) {
            if(clusters.get(clusterIndex).cosinevector.get(word) > 0.3) commonWrds.add(word);
        }

        long factor = (long) Math.pow(10, 2);
        avgCoeff = avgCoeff * factor;
        long tmp = Math.round(avgCoeff);

        System.out.println( round + "&" + clusters.get(clusterIndex).id + " & " + (double) tmp / factor + " & " + clusters.get(clusterIndex).cosinevector + "& " + commonWrds + " &  \\\\ \\hline");
        return (double) tmp / factor;
    }

}
