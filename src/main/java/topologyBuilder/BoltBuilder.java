package topologyBuilder;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import eventDetector.bolts.*;
import cassandraConnector.CassandraDao;
import eventDetector.spout.CassandraSpout;

import java.util.Properties;


public class BoltBuilder {


    public static StormTopology prepareBoltsForCassandraSpout(Properties properties) throws Exception {
        int COUNT_THRESHOLD = Integer.parseInt(properties.getProperty("topology.count.threshold"));
        String FILENUM = properties.getProperty("topology.file.number");
        String TWEETS_TABLE = properties.getProperty("tweets.table");
        String EVENTS_TABLE = properties.getProperty("events.table");
        String CLUSTER_TABLE = properties.getProperty("clusters.table");
        String CLUSTERINFO_TABLE = properties.getProperty("clusterinfo.table");
        String CLUSTERANDTWEET_TABLE = properties.getProperty("clusterandtweets.table");
        long START_ROUND = Long.parseLong(properties.getProperty("start.round"));
        long END_ROUND = Long.parseLong(properties.getProperty("end.round"));

        System.out.println("Count threshold " + COUNT_THRESHOLD);
        TopologyHelper.createFolder(Constants.RESULT_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.IMAGES_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.TIMEBREAKDOWN_FILE_PATH + FILENUM);

        CassandraDao cassandraDao = new CassandraDao(TWEETS_TABLE, CLUSTER_TABLE, CLUSTERINFO_TABLE, CLUSTERANDTWEET_TABLE, EVENTS_TABLE);
        System.out.println("Preparing Bolts...");
        TopologyBuilder builder = new TopologyBuilder();

        CassandraSpout cassandraSpout = new CassandraSpout(cassandraDao, FILENUM, START_ROUND, END_ROUND);

        ClusteringBolt countBoltCAN = new ClusteringBolt( FILENUM, cassandraDao, "CAN");
        ClusteringBolt countBoltUSA = new ClusteringBolt( FILENUM, cassandraDao, "USA");
        EventDetectorBolt eventDetectorBoltCAN = new EventDetectorBolt(FILENUM, cassandraDao, "CAN");
        EventDetectorBolt eventDetectorBoltUSA = new EventDetectorBolt(FILENUM, cassandraDao, "USA");

        builder.setSpout(Constants.CASS_SPOUT_ID, cassandraSpout,1);
        builder.setBolt(Constants.COUNTRY2_COUNT_BOLT_ID, countBoltCAN,2).shuffleGrouping(Constants.CASS_SPOUT_ID, "CAN");
        builder.setBolt(Constants.COUNTRY1_COUNT_BOLT_ID, countBoltUSA,6).shuffleGrouping(Constants.CASS_SPOUT_ID, "USA");
        builder.setBolt(Constants.COUNTRY2_EVENTDETECTOR_BOLT_ID, eventDetectorBoltCAN,1).shuffleGrouping(Constants.COUNTRY2_COUNT_BOLT_ID);
        builder.setBolt(Constants.COUNTRY1_EVENTDETECTOR_BOLT_ID, eventDetectorBoltUSA,1).shuffleGrouping(Constants.COUNTRY1_COUNT_BOLT_ID);


        return builder.createTopology();
    }
}

