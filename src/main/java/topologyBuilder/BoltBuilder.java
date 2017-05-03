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
        String EVENTS_WORDBASED_TABLE = properties.getProperty("events_wordbased.table");
        String CLUSTER_TABLE = properties.getProperty("clusters.table");
        String CLUSTERINFO_TABLE = properties.getProperty("clusterinfo.table");
        String CLUSTERANDTWEET_TABLE = properties.getProperty("clusterandtweets.table");
        String PROCESSEDTWEET_TABLE = properties.getProperty("processed_tweets.table");
        String PROCESSTIMES_TABLE = properties.getProperty("processtimes.table");
        long START_ROUND = Long.parseLong(properties.getProperty("start.round"));
        long END_ROUND = Long.parseLong(properties.getProperty("end.round"));

        int CAN_TASK_NUM= Integer.parseInt(properties.getProperty("can.taskNum"));
        int USA_TASK_NUM= Integer.parseInt(properties.getProperty("usa.taskNum"));
        int NUM_WORKERS= Integer.parseInt(properties.getProperty("num.workers"));

        System.out.println("Count threshold " + COUNT_THRESHOLD);
        TopologyHelper.createFolder(Constants.RESULT_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.IMAGES_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.TIMEBREAKDOWN_FILE_PATH + FILENUM);

        CassandraDao cassandraDao = new CassandraDao(TWEETS_TABLE, CLUSTER_TABLE, CLUSTERINFO_TABLE, CLUSTERANDTWEET_TABLE, EVENTS_TABLE, EVENTS_WORDBASED_TABLE, PROCESSEDTWEET_TABLE, PROCESSTIMES_TABLE);
        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + FILENUM + "sout.txt", "Preparing Bolts...");
        TopologyBuilder builder = new TopologyBuilder();

        CassandraSpout cassandraSpout = new CassandraSpout(cassandraDao, FILENUM, START_ROUND, END_ROUND, CAN_TASK_NUM, USA_TASK_NUM, NUM_WORKERS);

        ClusteringBolt countBoltCAN = new ClusteringBolt( FILENUM, cassandraDao, "CAN", CAN_TASK_NUM, USA_TASK_NUM, NUM_WORKERS);
        ClusteringBolt countBoltUSA = new ClusteringBolt( FILENUM, cassandraDao, "USA", CAN_TASK_NUM, USA_TASK_NUM, NUM_WORKERS);
        EventDetectorBolt eventDetectorBoltCAN = new EventDetectorBolt(FILENUM, cassandraDao, "CAN", CAN_TASK_NUM);
        EventDetectorBolt eventDetectorBoltUSA = new EventDetectorBolt(FILENUM, cassandraDao, "USA", USA_TASK_NUM);

        builder.setSpout(Constants.CASS_SPOUT_ID, cassandraSpout,1);
//        builder.setBolt(Constants.COUNTRY2_CLUSTERING_BOLT_ID, countBoltCAN,5).shuffleGrouping(Constants.CASS_SPOUT_ID, "CAN");
//        builder.setBolt(Constants.COUNTRY1_CLUSTERING_BOLT_ID, countBoltUSA,15).shuffleGrouping(Constants.CASS_SPOUT_ID, "USA");
        builder.setBolt(Constants.COUNTRY2_CLUSTERING_BOLT_ID, countBoltCAN,CAN_TASK_NUM).directGrouping(Constants.CASS_SPOUT_ID);
        builder.setBolt(Constants.COUNTRY1_CLUSTERING_BOLT_ID, countBoltUSA,USA_TASK_NUM).directGrouping(Constants.CASS_SPOUT_ID);
        builder.setBolt(Constants.COUNTRY2_EVENTDETECTOR_BOLT_ID, eventDetectorBoltCAN,1).shuffleGrouping(Constants.COUNTRY2_CLUSTERING_BOLT_ID);
        builder.setBolt(Constants.COUNTRY1_EVENTDETECTOR_BOLT_ID, eventDetectorBoltUSA,1).shuffleGrouping(Constants.COUNTRY1_CLUSTERING_BOLT_ID);

        System.out.println("Bolts ready");

        return builder.createTopology();
    }
}

