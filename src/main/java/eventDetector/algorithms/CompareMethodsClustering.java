package eventDetector.algorithms;

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
        String EVENTS_TABLE = properties.getProperty("clustering.events.table");
        String EVENTS_WORDBASED_TABLE = properties.getProperty("clustering.events_wordbased.table");
        String CLUSTER_TABLE = properties.getProperty("clustering.clusters.table");
        String CLUSTERANDTWEET_TABLE = properties.getProperty("clustering.clusterandtweets.table");
        String PROCESSEDTWEET_TABLE = properties.getProperty("clustering.processed_tweets.table");
        String PROCESSTIMES_TABLE = properties.getProperty("clustering.processtimes.table");

        CassandraDao cassandraDao = new CassandraDao(TWEETS_TABLE, CLUSTER_TABLE, CLUSTERANDTWEET_TABLE, EVENTS_TABLE, EVENTS_WORDBASED_TABLE, PROCESSEDTWEET_TABLE, PROCESSTIMES_TABLE);
        ResultSet resultSetClustering, resultSetWordBased ;
        HashMap<String, Integer> wordNums = new HashMap<>();
//        Constants.lock.lock();
        try {
            resultSetClustering = cassandraDao.getEvents(c);
            Iterator<Row> iteratorClustering = resultSetClustering.iterator();
            resultSetWordBased = cassandraDao.getEventsWordBased();
            Iterator<Row> iteratorWordBased_init = resultSetWordBased.iterator();
            while (iteratorWordBased_init.hasNext()) {
                Row rowWordBased = iteratorWordBased_init.next();
                String word = rowWordBased.getString("word").replace("#","").replace(".","");
                String country = rowWordBased.getString("country");
                if(country.equals(c)) wordNums.put(word,0);
            }

            int clusterNum = 0;
            while (iteratorClustering.hasNext()) {
                Row row = iteratorClustering.next();
                clusterNum++;
                int total=0, intersection=0;
                HashMap<String, Double> cosinevector = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);
                UUID clusterid = row.getUUID("clusterid");
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
                        if(country.equals(c) && word.equals(key)) {
                            wordNums.put(word, wordNums.get(word)+1);
                            intersection++;
                            break;
                        }
                    }
                }
                System.out.println("Cluster " + clusterid + " has " + total + " keys and " + intersection + " of them are also detected as event. Percentage: " + (double)intersection/(double)total);
            }


            Iterator it = wordNums.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Integer> pair = (Map.Entry)it.next();
                System.out.println("Word " + pair.getKey() + " has occurred in " + pair.getValue() + " clusters. Percentage: " + (double)pair.getValue()/(double)clusterNum);
                it.remove(); // avoids a ConcurrentModificationException
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        try {
            System.out.println("CAN_____________________________");
            clusterPercentage("CAN");
            System.out.println("USA_____________________________");
            clusterPercentage("USA");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
