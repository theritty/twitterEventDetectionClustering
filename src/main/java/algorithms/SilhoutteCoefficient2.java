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
public class SilhoutteCoefficient2 {
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
        System.out.println("1");
        getResults("USA",1);
        getResults("CAN",1);
        System.out.println("2");
        getResults("USA",2);
        getResults("CAN",2);
        System.out.println("3");
        getResults("USA",3);
        getResults("CAN",3);
        System.out.println("4");
        getResults("USA",4);
        getResults("CAN",4);
        System.out.println("5");
        getResults("USA",5);
        getResults("CAN",5);
        System.out.println("DONE");
    }


    public static  void getResults(String country, int x) {

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
        String EVENTS_TABLE1 = "clusteringEvents"+x;
        String CLUSTER_TABLE = properties.getProperty("clustering.clusters.table");
        String PROCESSEDTWEET_TABLE = properties.getProperty("clustering.processed_tweets.table");
        String PROCESSTIMES_TABLE = properties.getProperty("clustering.processtimes.table");
        String TWEETSANDCLUSTER_TABLE = "clusteringTweets"+x;

        CassandraDao cassandraDao;
        List<Long> roundList = new ArrayList<>();
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

                    if(!roundList.contains(round)) roundList.add(round);
                }

                ResultSet resultSet = cassandraDao.getEvents(country);
                Iterator<Row> iterator = resultSet.iterator();
                while(iterator.hasNext()) {
                    Row row = iterator.next();
                    UUID clusterid = row.getUUID("clusterid");
                    long round = row.getLong("round");

//                    if(roundCount.get(round)<2) continue;
                    HashMap<String, Double> cosinevector = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);

                    Cluster c = new Cluster(clusterid, cosinevector);

                    ResultSet resultSet2 = cassandraDao.getClusterTweets(round, clusterid);
                    Iterator<Row> iterator2 = resultSet2.iterator();
                    while (iterator2.hasNext()) {
                        Row row2 = iterator2.next();
                        long tweetid = row2.getLong("tweetid");

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
        Collections.sort(roundList);
        for(long r : roundList) {
            long key = r;
            List<Cluster> value = clusters.get(key);

//            if(value.size()<2) continue;
            System.out.println("Round: "+ key + " -----------------");

            System.out.println("\\begin{longtable}{|p{2cm}|p{1cm}|p{6cm}|p{1cm}|p{4cm}|} \\hline");
            System.out.println("cluster id&silhoutte coefficient &cosine vector & mostly used words & comment \\\\ \\hline");
            Map<String, Integer> counts = new HashMap<>();
            for(int i=0; i<value.size(); i++) {
                avgSilhoutteCoefForCluster(value, i);
                for (String word : value.get(i).cosinevector.keySet()) {
                    if(counts.containsKey(word)) counts.put(word, counts.get(word)+1);
                    else counts.put(word, 1);
                }
            }

            System.out.println("\\end{longtable}    \\newpage ");
//            for(int j = value.size(); j>=0; j--) {
//                System.out.printf("Words occured in " + j + " clusters: "  );
//                for (String word : counts.keySet()) {
//                    if(counts.get(word) == j) System.out.printf(word + ", ");
//                }
//            }
//            System.out.printf("\n");


        }
    }
    public static double averageDistanceInsideCluster(Cluster c, int objIndex, HashMap<String, Double> currentVector) {
        double sum = 0.0;
        int count = 0;
        CosineSimilarity sim = new CosineSimilarity();
        for(HashMap h : c.tweetList) {
            if(count++ != objIndex) {
                sum += 1-sim.cosineSimilarityFromMap(currentVector, h);
            }
        }
        return sum/((double)count);
    }

    public static double minDistanceBetweenClusters(List<Cluster> clusters, int clusterIndex, int objIndex) {
        int ind = 0;
        double minDist = Integer.MAX_VALUE;
        for(Cluster c : clusters) {
            if(ind++ != clusterIndex) {
                double clusterDistance = averageDistanceInsideCluster(c, -1, clusters.get(clusterIndex).tweetList.get(objIndex));
                if(minDist > clusterDistance) minDist = clusterDistance;
            }
        }
        return minDist;
    }

    public static double silhoutteCoefForObject(List<Cluster> clusters, int clusterIndex, int objIndex) {
        double a = averageDistanceInsideCluster(clusters.get(clusterIndex), objIndex, clusters.get(clusterIndex).tweetList.get(objIndex));
        double b = minDistanceBetweenClusters(clusters, clusterIndex, objIndex);
        return (b-a)/Math.max(a,b);
    }

    public static double avgSilhoutteCoefForCluster(List<Cluster> clusters, int clusterIndex) {
        double coeff = 0.0;
        double minCoeff = Double.MAX_VALUE;
        double maxCoeff = Double.MIN_VALUE;
        int count = 0;

        if(clusters.size()>1) {
            for (int i = 0; i < clusters.get(clusterIndex).tweetList.size(); i++) {
                double curCoef = silhoutteCoefForObject(clusters, clusterIndex, i);
                coeff += curCoef;
                if (curCoef > maxCoeff) maxCoeff = curCoef;
                if (curCoef < minCoeff) minCoeff = curCoef;
                count++;
            }
        }
        else {
            coeff = 1.0;
            count = 1;
        }

        double avgCoeff = coeff/((double)count);

        List<String> commonWrds = new ArrayList<>();
        for (String word : clusters.get(clusterIndex).cosinevector.keySet()) {
                    if(clusters.get(clusterIndex).cosinevector.get(word) > 0.3) commonWrds.add(word);
                }

        long factor = (long) Math.pow(10, 2);
        avgCoeff = avgCoeff * factor;
        long tmp = Math.round(avgCoeff);

        System.out.println( clusters.get(clusterIndex).id + " & " + (double) tmp / factor + " & " + clusters.get(clusterIndex).cosinevector + "& " + commonWrds + " &  \\\\ \\hline");
        return avgCoeff;
    }















//
//    public  static void main(String[] args) {
//
//        HashMap<Long, List<Cluster>> clusters = new HashMap<>();
//        TopologyHelper topologyHelper = new TopologyHelper();
//        Properties properties = null;
//        try {
//            properties = topologyHelper.loadProperties( "config.properties" );
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        String TWEETS_TABLE = properties.getProperty("clustering.tweets.table");
//        String EVENTS_WORDBASED_TABLE = properties.getProperty("clustering.events_wordbased.table");
//        String EVENTS_TABLE1 =  "eventclusterforexperiment_thesis";
//        String CLUSTER_TABLE = properties.getProperty("clustering.clusters.table");
//        String PROCESSEDTWEET_TABLE = properties.getProperty("clustering.processed_tweets.table");
//        String PROCESSTIMES_TABLE = properties.getProperty("clustering.processtimes.table");
//        String TWEETSANDCLUSTER_TABLE = properties.getProperty("clustering.tweetsandcluster.table");
//
//        CassandraDao cassandraDao;
//        try {
//            cassandraDao = new CassandraDao(TWEETS_TABLE, CLUSTER_TABLE, EVENTS_TABLE1, EVENTS_WORDBASED_TABLE, PROCESSEDTWEET_TABLE, PROCESSTIMES_TABLE, TWEETSANDCLUSTER_TABLE);
//
//            ResultSet resultSetx = cassandraDao.getEvents("USA");
//            List<Long> roundCount = new ArrayList<>();
//
//            Iterator<Row> iteratorx = resultSetx.iterator();
//            while (iteratorx.hasNext()) {
//                Row rowx = iteratorx.next();
//                long round = rowx.getLong("round");
//                if(!roundCount.contains(round)) roundCount.add(round);
//            }
//
//            Collections.sort(roundCount);
//            System.out.println(roundCount);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

}
