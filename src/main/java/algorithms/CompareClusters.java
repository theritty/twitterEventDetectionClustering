package algorithms;

import cassandraConnector.CassandraDao;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import topologyBuilder.TopologyHelper;

import java.io.IOException;
import java.util.*;

public class CompareClusters {
    public static void clusterPercentage(String c, String EVENTS_TABLE1, String EVENTS_TABLE2, double perc) throws Exception {
        TopologyHelper topologyHelper = new TopologyHelper();
        Properties properties = topologyHelper.loadProperties( "config.properties" );

        String TWEETS_TABLE = properties.getProperty("clustering.tweets.table");
        String EVENTS_WORDBASED_TABLE = properties.getProperty("clustering.events_wordbased.table");
        String CLUSTER_TABLE = properties.getProperty("clustering.clusters.table");
        String PROCESSEDTWEET_TABLE = properties.getProperty("clustering.processed_tweets.table");
        String PROCESSTIMES_TABLE = properties.getProperty("clustering.processtimes.table");

        CassandraDao cassandraDao1 = new CassandraDao(TWEETS_TABLE, CLUSTER_TABLE, EVENTS_TABLE1, EVENTS_WORDBASED_TABLE, PROCESSEDTWEET_TABLE, PROCESSTIMES_TABLE);
        CassandraDao cassandraDao2 = new CassandraDao(TWEETS_TABLE, CLUSTER_TABLE, EVENTS_TABLE2, EVENTS_WORDBASED_TABLE, PROCESSEDTWEET_TABLE, PROCESSTIMES_TABLE);
        ResultSet resultSetClustering ;
        try {
            resultSetClustering = cassandraDao1.getEvents(c);
            Iterator<Row> iteratorClustering = resultSetClustering.iterator();
            int clusterNum= 0;
            int clusterNum2= 0;
            int clusterInter = 0;

            while (iteratorClustering.hasNext()) {
                Row row = iteratorClustering.next();
                int total=0, intersection=0;
                HashMap<String, Double> cosinevector = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);
                UUID clusterid = row.getUUID("clusterid");
                long round = row.getLong("round");
                ResultSet resultSetClustering2 = cassandraDao2.getEvents(c);
                Iterator<Row> iteratorClustering2 = resultSetClustering2.iterator() ;
                clusterNum++;
                clusterNum2=0;

                while(iteratorClustering2.hasNext()) {
                    clusterNum2++;
                    Row row2 = iteratorClustering2.next();
                    HashMap<String, Double> cosinevector2 = (HashMap<String, Double>) row2.getMap("cosinevector", String.class, Double.class);
                    UUID clusterid2 = row2.getUUID("clusterid");
                    long round2 = row2.getLong("round");
                    if(round == round2) {
                        Iterator<Map.Entry<String, Double>> it = cosinevector.entrySet().iterator();
                        ArrayList<String> ks = new ArrayList<>();
                        while(it.hasNext()) {
                            Map.Entry<String, Double> entry = it.next();
                            String key = entry.getKey();
                            total++;

                            Iterator<Map.Entry<String, Double>> it2 = cosinevector2.entrySet().iterator();
                            while (it2.hasNext()) {
                                Map.Entry<String, Double> entry2 = it2.next();
                                String key2 = entry2.getKey();

                                if(key.equals(key2)) {
                                    ks.add(key2);
                                    intersection++;
                                    break;
                                }
                            }
                        }
                        if((double)intersection/(double)total > perc) {
                            System.out.println("Similar found:  " + clusterid + " - " + clusterid2  + ". Percentage: " + (double)intersection/(double)total + ". Words: " + ks );
//                            System.out.println(cosinevector);
//                            System.out.println(cosinevector2);
                            clusterInter++;
                            break;
                        }
                        else {
//                            System.out.println("Similarity  " + clusterid + " - " + clusterid2  + ". Percentage: " + (double)intersection/(double)total );

                        }
                    }
                }
            }

            System.out.println("Final Intersection Percentage: " + (double)clusterInter/(double)clusterNum  + " and " + (double)clusterInter/(double)clusterNum2 + " for similarity percentage threshold of " + perc + " " + clusterInter + " " + clusterNum + " " + clusterNum2);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void xx(double perc) {
        try {
            String EVENTS_TABLE1 = "eventclusterForExperiment5";
            String EVENTS_TABLE2 = "eventsHybridForExperiment5";
            System.out.println("CAN_____________________________");
            clusterPercentage("CAN", EVENTS_TABLE1, EVENTS_TABLE2, perc);
            System.out.println("USA_____________________________");
            clusterPercentage("USA", EVENTS_TABLE1, EVENTS_TABLE2, perc);
            System.out.println("**********************************************************************");

            System.out.println("CAN_____________________________");
            clusterPercentage("CAN", EVENTS_TABLE2, EVENTS_TABLE1, perc);
            System.out.println("USA_____________________________");
            clusterPercentage("USA", EVENTS_TABLE2, EVENTS_TABLE1, perc);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
//    public static void main(String[] args) {
//        System.out.println("Percentage: 0.1************************************************************************************************************************************");
//        xx(0.1);
//        System.out.println("Percentage: 0.2************************************************************************************************************************************");
//        xx(0.2);
//        System.out.println("Percentage: 0.3************************************************************************************************************************************");
//        xx(0.3);
//        System.out.println("Percentage: 0.4************************************************************************************************************************************");
//        xx(0.4);
//        System.out.println("Percentage: 0.5************************************************************************************************************************************");
//        xx(0.5);
//        System.out.println("Percentage: 0.6************************************************************************************************************************************");
//        xx(0.6);
//        System.out.println("Percentage: 0.7************************************************************************************************************************************");
//        xx(0.7);
//    }
//    public static void main(String[] args) {
//
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
//        String EVENTS_TABLE1 = "eventclusterForExperiment5";
//        String CLUSTER_TABLE = properties.getProperty("clustering.clusters.table");
//        String PROCESSEDTWEET_TABLE = properties.getProperty("clustering.processed_tweets.table");
//        String PROCESSTIMES_TABLE = properties.getProperty("clustering.processtimes.table");
//
//        CassandraDao cassandraDao = null;
//        int count = 0;
//        try {
//            cassandraDao = new CassandraDao(TWEETS_TABLE, CLUSTER_TABLE, EVENTS_TABLE1, EVENTS_WORDBASED_TABLE, PROCESSEDTWEET_TABLE, PROCESSTIMES_TABLE);
//            ResultSet resultSet = null;
//            try {
//                for(long i=4068480; i<= 4070160; i++) {
//                    resultSet = cassandraDao.getTweetsByRound(i);
//                    Iterator<Row> iterator = resultSet.iterator();
//                    while(iterator.hasNext()) {
//                        iterator.next();
//                        count++;
//                    }
//                }
//                System.out.println("count " + count);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//
//
//    }
 public static void main(String[] args) {

        TopologyHelper topologyHelper = new TopologyHelper();
        Properties properties = null;
        try {
            properties = topologyHelper.loadProperties( "config.properties" );
        } catch (IOException e) {
            e.printStackTrace();
        }

        String TWEETS_TABLE = properties.getProperty("clustering.tweets.table");
        String EVENTS_WORDBASED_TABLE = properties.getProperty("clustering.events_wordbased.table");
        String EVENTS_TABLE1 = "eventsHybridForExperiment5";
        String CLUSTER_TABLE = properties.getProperty("clustering.clusters.table");
        String PROCESSEDTWEET_TABLE = properties.getProperty("clustering.processed_tweets.table");
        String PROCESSTIMES_TABLE = properties.getProperty("clustering.processtimes.table");

        CassandraDao cassandraDao;
        try {
            cassandraDao = new CassandraDao(TWEETS_TABLE, CLUSTER_TABLE, EVENTS_TABLE1, EVENTS_WORDBASED_TABLE, PROCESSEDTWEET_TABLE, PROCESSTIMES_TABLE);
            try {
                ResultSet resultSet = cassandraDao.getEvents("USA");
                    Iterator<Row> iterator = resultSet.iterator();
                    while(iterator.hasNext()) {
                        Row row = iterator.next();
                        HashMap<String, Double> cosinevector2 = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);
                        UUID clusterid2 = row.getUUID("clusterid");
                        long round2 = row.getLong("round");
                        Iterator<Map.Entry<String, Double>> it2 = cosinevector2.entrySet().iterator();
                        ArrayList<String> mostlyUsed = new ArrayList<>();
                        while (it2.hasNext()) {
                            Map.Entry<String, Double> entry2 = it2.next();
                            String key2 = entry2.getKey();
                            if(entry2.getValue()>0.2) mostlyUsed.add(key2+":"+entry2.getValue());
                        }
                        System.out.println(clusterid2 + " | " + round2 + " | "  + row.getString("country") + " | " + row.getInt("numtweet") + " | " + cosinevector2 + " | " + mostlyUsed );
                    }


            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                ResultSet resultSet = cassandraDao.getEvents("CAN");
                Iterator<Row> iterator = resultSet.iterator();
                while(iterator.hasNext()) {
                    Row row = iterator.next();
                    HashMap<String, Double> cosinevector2 = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);
                    UUID clusterid2 = row.getUUID("clusterid");
                    long round2 = row.getLong("round");
                    Iterator<Map.Entry<String, Double>> it2 = cosinevector2.entrySet().iterator();
                    ArrayList<String> mostlyUsed = new ArrayList<>();
                    while (it2.hasNext()) {
                        Map.Entry<String, Double> entry2 = it2.next();
                        String key2 = entry2.getKey();
                        if(entry2.getValue()>0.2) mostlyUsed.add(key2+":"+entry2.getValue());
                    }
                    System.out.println(clusterid2 + " | " + round2 + " | "  + row.getString("country") + " | " + row.getInt("numtweet") + " | " + cosinevector2 + " | " + mostlyUsed );
                }


            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }



    }


}
