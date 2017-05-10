package topologyBuilder;

import cassandraConnector.CassandraDao;
import cassandraConnector.CassandraDaoKeyBased;
import eventDetector.bolts.*;
import eventDetector.spout.CassandraSpoutClustering;
import eventDetector.spout.CassandraSpoutKeyBased;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Properties;


public class BoltBuilder {



    public static StormTopology prepareBoltsForCassandraSpoutKeyBased(Properties properties) throws Exception {
        int COUNT_THRESHOLD = Integer.parseInt(properties.getProperty("topology.count.threshold"));
        String FILENUM = properties.getProperty("topology.file.number");
        double TFIDF_EVENT_RATE = Double.parseDouble(properties.getProperty("topology.tfidf.event.rate"));
        String TWEETS_TABLE = properties.getProperty("tweets.table");
        String COUNTS_TABLE = properties.getProperty("counts.table");
        String EVENTS_TABLE = properties.getProperty("events.table");
        String PROCESSED_TABLE = properties.getProperty("processed.table");

        int CAN_TASK_NUM= Integer.parseInt(properties.getProperty("can.taskNum"));
        int USA_TASK_NUM= Integer.parseInt(properties.getProperty("usa.taskNum"));
        int NUM_WORKERS= Integer.parseInt(properties.getProperty("num.workers"));
        int NUM_DETECTORS= Integer.parseInt(properties.getProperty("num.detectors.per.country"));
        int NUM_COUNTRIES= Integer.parseInt(properties.getProperty("num.countries"));


        System.out.println("Count threshold " + COUNT_THRESHOLD);
        TopologyHelper.createFolder(Constants.RESULT_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.IMAGES_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.TIMEBREAKDOWN_FILE_PATH + FILENUM);

        System.out.println("Preparing Bolts...");
        TopologyBuilder builder = new TopologyBuilder();

        CassandraDaoKeyBased cassandraDao = new CassandraDaoKeyBased(TWEETS_TABLE, COUNTS_TABLE, EVENTS_TABLE, PROCESSED_TABLE);
        CassandraSpoutKeyBased cassandraSpout = new CassandraSpoutKeyBased(cassandraDao, FILENUM, USA_TASK_NUM, CAN_TASK_NUM, NUM_WORKERS, NUM_DETECTORS*NUM_COUNTRIES);
        WordCountBoltKeyBased countBoltUSA = new WordCountBoltKeyBased(COUNT_THRESHOLD, FILENUM, "USA", cassandraDao, NUM_DETECTORS, NUM_WORKERS+CAN_TASK_NUM+USA_TASK_NUM+3);
        WordCountBoltKeyBased countBoltCAN = new WordCountBoltKeyBased(COUNT_THRESHOLD, FILENUM, "CAN", cassandraDao, NUM_DETECTORS, NUM_WORKERS+CAN_TASK_NUM+USA_TASK_NUM+3+NUM_DETECTORS);
        EventDetectorWithCassandraBoltKeyBased eventDetectorBoltUSA = new EventDetectorWithCassandraBoltKeyBased(cassandraDao,
                Constants.RESULT_FILE_PATH, FILENUM, TFIDF_EVENT_RATE, Integer.parseInt(properties.getProperty("topology.compare.size")), "USA");
        EventDetectorWithCassandraBoltKeyBased eventDetectorBoltCAN = new EventDetectorWithCassandraBoltKeyBased(cassandraDao,
                Constants.RESULT_FILE_PATH, FILENUM, TFIDF_EVENT_RATE, Integer.parseInt(properties.getProperty("topology.compare.size")), "CAN");

        EventCompareBoltKeyBased eventCompareBolt = new EventCompareBoltKeyBased(cassandraDao, FILENUM);
        builder.setSpout(Constants.CASS_SPOUT_ID, cassandraSpout,1);

        builder.setBolt(Constants.COUNTRY1_COUNT_BOLT_ID, countBoltUSA,USA_TASK_NUM).directGrouping(Constants.CASS_SPOUT_ID);
        builder.setBolt(Constants.COUNTRY2_COUNT_BOLT_ID, countBoltCAN,CAN_TASK_NUM).directGrouping(Constants.CASS_SPOUT_ID);

        builder.setBolt( Constants.COUNTRY1_EVENT_DETECTOR_BOLT, eventDetectorBoltUSA,NUM_DETECTORS).directGrouping(Constants.COUNTRY1_COUNT_BOLT_ID);
        builder.setBolt( Constants.COUNTRY2_EVENT_DETECTOR_BOLT, eventDetectorBoltCAN,NUM_DETECTORS).directGrouping(Constants.COUNTRY2_COUNT_BOLT_ID);

        builder.setBolt( Constants.EVENT_COMPARE_BOLT, eventCompareBolt,1).
                globalGrouping(Constants.COUNTRY1_EVENT_DETECTOR_BOLT).
                globalGrouping(Constants.COUNTRY2_EVENT_DETECTOR_BOLT);

        return builder.createTopology();
    }


    public static StormTopology prepareBoltsForCassandraSpoutClustering(Properties properties) throws Exception {
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
        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + FILENUM + "/" + "sout.txt", "Preparing Bolts...");
        TopologyBuilder builder = new TopologyBuilder();

        CassandraSpoutClustering cassandraSpoutClustering = new CassandraSpoutClustering(cassandraDao, FILENUM, START_ROUND, END_ROUND, CAN_TASK_NUM, USA_TASK_NUM, NUM_WORKERS);

        ClusteringBolt countBoltCAN = new ClusteringBolt( FILENUM, cassandraDao, "CAN");
        ClusteringBolt countBoltUSA = new ClusteringBolt( FILENUM, cassandraDao, "USA");
        EventDetectorBoltClustering eventDetectorBoltClusteringCAN = new EventDetectorBoltClustering(FILENUM, cassandraDao, "CAN", CAN_TASK_NUM);
        EventDetectorBoltClustering eventDetectorBoltClusteringUSA = new EventDetectorBoltClustering(FILENUM, cassandraDao, "USA", USA_TASK_NUM);

        builder.setSpout(Constants.CASS_SPOUT_ID, cassandraSpoutClustering,1);
        builder.setBolt(Constants.COUNTRY2_CLUSTERING_BOLT_ID, countBoltCAN,CAN_TASK_NUM).directGrouping(Constants.CASS_SPOUT_ID);
        builder.setBolt(Constants.COUNTRY1_CLUSTERING_BOLT_ID, countBoltUSA,USA_TASK_NUM).directGrouping(Constants.CASS_SPOUT_ID);
        builder.setBolt(Constants.COUNTRY2_EVENTDETECTOR_BOLT_ID, eventDetectorBoltClusteringCAN,1).shuffleGrouping(Constants.COUNTRY2_CLUSTERING_BOLT_ID);
        builder.setBolt(Constants.COUNTRY1_EVENTDETECTOR_BOLT_ID, eventDetectorBoltClusteringUSA,1).shuffleGrouping(Constants.COUNTRY1_CLUSTERING_BOLT_ID);

        System.out.println("Bolts ready");

        return builder.createTopology();
    }
}

