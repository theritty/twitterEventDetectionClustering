package topologyBuilder;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import eventDetector.bolts.*;
import cassandraConnector.CassandraDao;
import eventDetector.spout.CassandraSpout;

import java.util.Properties;


public class BoltBuilder {


    public static StormTopology prepareBoltsForCassandraSpout(Properties properties) throws Exception {
        int COUNT_THRESHOLD = Integer.parseInt(properties.getProperty("topology.count.threshold"));
        String FILENUM = properties.getProperty("topology.file.number");
        String TWEETS_TABLE = properties.getProperty("tweets.table");
        String CLUSTER_TABLE = properties.getProperty("clusters.table");
        String CLUSTERANDTWEET_TABLE = properties.getProperty("clusterandtweets.table");
        long START_ROUND = Long.parseLong(properties.getProperty("start.round"));
        long END_ROUND = Long.parseLong(properties.getProperty("end.round"));


        System.out.println("Count threshold " + COUNT_THRESHOLD);
        TopologyHelper.createFolder(Constants.RESULT_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.IMAGES_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.TIMEBREAKDOWN_FILE_PATH + FILENUM);

        CassandraDao cassandraDao = new CassandraDao(TWEETS_TABLE, CLUSTER_TABLE, CLUSTERANDTWEET_TABLE);
        System.out.println("Preparing Bolts...");
        TopologyBuilder builder = new TopologyBuilder();

        CassandraSpout cassandraSpout = new CassandraSpout(cassandraDao, Integer.parseInt(properties.getProperty("topology.train.size")),
                Integer.parseInt(properties.getProperty("topology.compare.size")), 20, FILENUM, START_ROUND, END_ROUND);

        WordCountBolt countBoltCAN = new WordCountBolt(COUNT_THRESHOLD, FILENUM, cassandraDao);

        builder.setSpout(Constants.CASS_SPOUT_ID, cassandraSpout,1);
        builder.setBolt(Constants.COUNTRY2_COUNT_BOLT_ID, countBoltCAN,5).fieldsGrouping(Constants.CASS_SPOUT_ID, "CAN", new Fields("tweetmap"));


        return builder.createTopology();
    }
}

