package algorithms;

import cassandraConnector.CassandraDao;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import topologyBuilder.TopologyHelper;

import java.util.*;

public class CompareMethodsClustering {
    public static void clusterPercentage(String c) throws Exception {
        TopologyHelper topologyHelper = new TopologyHelper();
        Properties properties = topologyHelper.loadProperties( "config.properties" );

        String TWEETS_TABLE = properties.getProperty("clustering.tweets.table");
        String EVENTS_TABLE = properties.getProperty("hybrid.events.table");
        String EVENTS_WORDBASED_TABLE = properties.getProperty("keybased.events.table");
        String CLUSTER_TABLE = properties.getProperty("clustering.clusters.table");
        String PROCESSEDTWEET_TABLE = properties.getProperty("clustering.processed_tweets.table");
        String PROCESSTIMES_TABLE = properties.getProperty("clustering.processtimes.table");
        String TWEETSANDCLUSTER_TABLE = properties.getProperty("clustering.tweetsandcluster.table");

        CassandraDao cassandraDao = new CassandraDao(TWEETS_TABLE, CLUSTER_TABLE, EVENTS_TABLE, EVENTS_WORDBASED_TABLE, PROCESSEDTWEET_TABLE, PROCESSTIMES_TABLE, TWEETSANDCLUSTER_TABLE);
        ResultSet resultSetClustering, resultSetWordBased ;
        HashMap<Long, HashMap<String, Integer>> wordNums = new HashMap<>();
//        Constants.lock.lock();
        try {
            resultSetClustering = cassandraDao.getEvents(c);
            Iterator<Row> iteratorClustering = resultSetClustering.iterator();
//            resultSetWordBased = cassandraDao.getEventsWordBased();
//            Iterator<Row> iteratorWordBased_init = resultSetWordBased.iterator();
//            while (iteratorWordBased_init.hasNext()) {
//                Row rowWordBased = iteratorWordBased_init.next();
//                String word = rowWordBased.getString("word").replace("#","").replace(".","");
//                String country = rowWordBased.getString("country");
//                long round = rowWordBased.getLong("round");
//                if(country.equals(c)) {
//                    HashMap<String, Integer> x = new HashMap<>();
//                    x.put(word,0);
//                    wordNums.put(round, x);
//                }
//            }

            int clusterNum = 0;
            int clusterNumIntersection = 0;
            while (iteratorClustering.hasNext()) {
                System.out.println("------------");
                Row row = iteratorClustering.next();
                clusterNum++;
                int total=0, intersection=0;
                HashMap<String, Double> cosinevector = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);
                cosinevector.remove("numTweets");
                UUID clusterid = row.getUUID("clusterid");
                long roundClustering = row.getLong("round");
//                System.out.println(clusterid + " " + cosinevector);
                Iterator<Map.Entry<String, Double>> it = cosinevector.entrySet().iterator();
                while(it.hasNext()) {
                    Map.Entry<String, Double> entry = it.next();
                    String key = entry.getKey();
                    total++;

                    ResultSet resultSetWordBasedX = cassandraDao.getEventsWordBased();
                    Iterator<Row> iteratorWordBased = resultSetWordBasedX.iterator();
                    while (iteratorWordBased.hasNext()) {
                        Row rowWordBased = iteratorWordBased.next();
                        String word = rowWordBased.getString("word").replace("#","").replace(".","");
                        String country = rowWordBased.getString("country");
                        long roundWordBased = rowWordBased.getLong("round");
                        if( country.equals(c) && (word.equals(key) || word.equals("#"+key))) {
                            System.out.println("Word: " + word + " clustering " + roundClustering + " keybased " + roundWordBased);
                            HashMap<String, Integer> x = new HashMap<>();
                            if(!wordNums.containsKey(roundClustering))
                                wordNums.put(roundClustering, new HashMap<>());

                            x = wordNums.get(roundClustering);
                            if(x.containsKey(word))
                                x.put(word, wordNums.get(roundClustering).get(word)+1);
                            else
                                x.put(word,1);
                            wordNums.put(roundClustering, x);
                            intersection++;
                            break;
                        }
                    }
                }
                if(intersection > 0) {
                    System.out.println("Round " + roundClustering + " cluster " + clusterid + " has " + total + " keys and " + intersection + " of them are also detected as event. Percentage: " + (double) intersection / (double) total);
                    if ((double) intersection / (double) total > 0.0) {
                        clusterNumIntersection++;
                    }
                    System.out.println("Cluster: " + cosinevector);
                }
                else {
//                    System.out.println(roundClustering + " Wtf Cluster: " + cosinevector);
                }
            }


            int wordNum = 0;
            ResultSet resultSetWordBasedX = cassandraDao.getEventsWordBased();
            Iterator<Row> iteratorWordBased = resultSetWordBasedX.iterator();
            while (iteratorWordBased.hasNext()) {
                Row rowWordBased = iteratorWordBased.next();
                String country = rowWordBased.getString("country");
                if(country.equals(c)) wordNum++;
            }
            int wordNumIntersection = 0;
            Iterator itF = wordNums.entrySet().iterator();
            while (itF.hasNext()) {
                Map.Entry<Long, HashMap<String, Integer>> mapp = (Map.Entry) itF.next();
                long rnd = mapp.getKey();
                HashMap<String, Integer> vals = mapp.getValue();
                Iterator it = vals.entrySet().iterator();
                while (it.hasNext()) {
                    wordNumIntersection++;
                    Map.Entry<String, Integer> pair = (Map.Entry) it.next();
                    System.out.println( pair.getKey() + " has occurred in " + pair.getValue() + " clusters. Percentage: " + (double) pair.getValue() / (double) clusterNum);
//                    System.out.println("Round " + rnd +  " word " + pair.getKey() + " has occurred in " + pair.getValue() + " clusters. Percentage: " + (double) pair.getValue() / (double) clusterNum);
                    it.remove(); // avoids a ConcurrentModificationException
                }
            }

            System.out.println("FINAL! ClusterNum: " + clusterNum + ", clusterNumIntersection: " + clusterNumIntersection + ", percentage: " + ((double)clusterNumIntersection/(double)clusterNum));
            System.out.println("FINAL! WordNum: " + wordNum + ", wordNumIntersection: " + wordNumIntersection + ", percentage: " + ((double)wordNumIntersection/(double)wordNum));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        try {
//            System.out.println("CAN_____________________________");
//            clusterPercentage("CAN");
            System.out.println("USA_____________________________");
            clusterPercentage("USA");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
