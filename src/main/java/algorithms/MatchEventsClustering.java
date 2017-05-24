package algorithms;


import cassandraConnector.CassandraDao;
import topologyBuilder.TopologyHelper;

import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;

public class MatchEventsClustering {

  public static class Cluster {
    long round;
    UUID id;
    int number;
    HashMap<String, Double> cosinevector;
    Cluster(long round, UUID id, int number, HashMap<String, Double> cosinevector) {
      this.round = round;
      this.id = id;
      this.number = number;
      this.cosinevector = cosinevector;
    }
  }

  private static long start ;
  private static long end ;

  private static CassandraDao cassandraDao;
  public MatchEventsClustering() throws Exception {
    TopologyHelper topologyHelper = new TopologyHelper();
    Properties properties = topologyHelper.loadProperties( "config.properties" );
    String TWEETS_TABLE = properties.getProperty("clustering.tweets.table");
    String CLUSTER_TABLE = properties.getProperty("clustering.clusters.table");
    String CLUSTERANDTWEET_TABLE = properties.getProperty("clustering.clusterandtweets.table");
    String EVENTS_TABLE = properties.getProperty("clustering.events.table");
    String EVENTS_WORDBASED_TABLE = properties.getProperty("clustering.events_wordbased.table");
    String PROCESSEDTWEET_TABLE = properties.getProperty("clustering.processed_tweets.table");
    String PROCESSTIMES_TABLE = properties.getProperty("clustering.processtimes.table");
    start = Long.parseLong(properties.getProperty("clustering.start.round"));
    end = Long.parseLong(properties.getProperty("clustering.end.round"));
    cassandraDao = new CassandraDao(TWEETS_TABLE, CLUSTER_TABLE, CLUSTERANDTWEET_TABLE, EVENTS_TABLE, EVENTS_WORDBASED_TABLE, PROCESSEDTWEET_TABLE, PROCESSTIMES_TABLE);
  }

//    public static void main(String[] args) throws Exception {
//
//        String country = "CAN";
//        MatchEventsClustering m = new MatchEventsClustering();
//        for (long i=m.start; i <= m.end; i++) {
//
//            ResultSet resultSet = m.cassandraDao.getClusters(country);
//            Iterator<Row> iterator = resultSet.iterator();
//            List<Cluster> clusters = new ArrayList<>();
//            while (iterator.hasNext()) {
//                Row row = iterator.next();
//                HashMap<String, Double> cosinevector = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);
//                for(Map.Entry<String, Double> entry : cosinevector.entrySet()) {
//                    String key = entry.getKey();
//                    if(key.contains("muha") || key.contains("moha") || key.contains("ali") || key.contains("butterfly")
//                            || key.contains("bee") || key.contains("rip")) {
//                        System.out.println(row.getUUID("id") + " " + cosinevector);
//                    }
//
//
//                    // do what you have to do here
//                    // In your case, an other loop.
//                }
//            }
//
//            Collections.sort(clusters, new Comparator<Cluster>() {
//
//                public int compare(Cluster o1, Cluster o2) {
//                    return o1.number - o2.number;
//                }
//            });
//
//            for (Cluster p : clusters) {
//                if(p.number>=10 )
//                    System.out.println(p.round + "\t" + p.id + "\t" + p.number + "\t" + p.cosinevector);
//            }
//        }
//    }

//  public static void main(String[] args) throws Exception {
//
//    MatchEventsClustering m = new MatchEventsClustering();
//    for (long i=m.start; i <= m.end; i++) {
//      ResultSet resultSet = m.cassandraDao.getClusters();
//      Iterator<Row> iterator = resultSet.iterator();
//      List<Cluster> clusters = new ArrayList<>();
//      while (iterator.hasNext()) {
//        Row row = iterator.next();
//        List<Object> values = new ArrayList<>();
//        values.add(row.getUUID("id"));
//        HashMap<String, Double> cosinevector = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);
//        Cluster c = new Cluster(row.getLong("round"), row.getUUID("id"), row.getInt("numberoftweets"), cosinevector);
//        clusters.add(c);
//      }
//
//      Collections.sort(clusters, new Comparator<Cluster>() {
//
//        public int compare(Cluster o1, Cluster o2) {
//          return o1.number - o2.number;
//        }
//      });
//
//      for (Cluster p : clusters) {
//        if(p.number>=10 )
//          System.out.println(p.round + "\t" + p.id + "\t" + p.number + "\t" + p.cosinevector);
//      }
//    }
//  }
}