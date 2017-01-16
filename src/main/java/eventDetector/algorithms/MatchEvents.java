package eventDetector.algorithms;


import cassandraConnector.CassandraDao;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import topologyBuilder.TopologyHelper;

import java.util.*;

public class MatchEvents {

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
  public MatchEvents() throws Exception {
    TopologyHelper topologyHelper = new TopologyHelper();
    Properties properties = topologyHelper.loadProperties( "config.properties" );
    String TWEETS_TABLE = properties.getProperty("tweets.table");
    String CLUSTER_TABLE = properties.getProperty("clusters.table");
    String CLUSTERANDTWEET_TABLE = properties.getProperty("clusterandtweets.table");
    start = Long.parseLong(properties.getProperty("start.round"));
    end = Long.parseLong(properties.getProperty("end.round"));
    cassandraDao = new CassandraDao(TWEETS_TABLE, CLUSTER_TABLE, CLUSTERANDTWEET_TABLE);
  }

  public static void main(String[] args) throws Exception {

    MatchEvents m = new MatchEvents();
    for (long i=m.start; i <= m.end; i++) {
      ResultSet resultSet = m.cassandraDao.getClustersByRound(i);
      Iterator<Row> iterator = resultSet.iterator();
      List<Cluster> clusters = new ArrayList<>();
      while (iterator.hasNext()) {
        Row row = iterator.next();
        List<Object> values = new ArrayList<>();
        values.add(row.getUUID("id"));
        ResultSet resultSet2 = m.cassandraDao.getTweetsOfCluster(values.toArray());
        HashMap<String, Double> cosinevector = (HashMap<String, Double>) row.getMap("cosinevector", String.class, Double.class);
        Cluster c = new Cluster(row.getLong("round"), row.getUUID("id"), resultSet2.all().size(), cosinevector);
        clusters.add(c);
      }

      Collections.sort(clusters, new Comparator<Cluster>() {

        public int compare(Cluster o1, Cluster o2) {
          return o1.number - o2.number;
        }
      });

      for (Cluster p : clusters) {
        if(p.number>=10 )
          System.out.println(p.round + "\t" + p.id + "\t" + p.number + "\t" + p.cosinevector);
      }
    }
  }
}