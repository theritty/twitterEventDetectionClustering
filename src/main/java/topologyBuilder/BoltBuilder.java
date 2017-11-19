package topologyBuilder;

import cassandraConnector.CassandraDao;
import cassandraConnector.CassandraDaoHybrid;
import cassandraConnector.CassandraDaoKeyBased;
import eventDetectionHybrid.*;
import eventDetectionKeybased.EventCompareBoltKeyBased;
import eventDetectionKeybased.EventDetectorWithCassandraBoltKeyBased;
import eventDetectionKeybased.WordCountBoltKeyBased;
import eventDetectionKeybasedWithSleep.EventCompareBoltKeyBasedWithSleep;
import eventDetectionKeybasedWithSleep.EventDetectorWithCassandraBoltKeyBasedWithSleep;
import eventDetectionKeybasedWithSleep.WordCountBoltKeyBasedWithSleep;
import eventDetectionWithClustering.ClusteringBolt;
import eventDetectionWithClustering.EventDetectorBoltClustering;
import eventDetectionWithClustering.CassandraSpoutClustering;
import eventDetectionKeybased.CassandraSpoutKeyBased;
import eventDetectionKeybasedWithSleep.CassandraSpoutKeyBasedWithSleep;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import java.util.Properties;


public class BoltBuilder {

    public static StormTopology prepareBoltsForCassandraSpoutKeyBasedWithSleep(Properties properties) throws Exception {
        int COUNT_THRESHOLD = Integer.parseInt(properties.getProperty("keybasedsleep.count.threshold"));
        String FILENUM = properties.getProperty("keybasedsleep.file.number");
        double TFIDF_EVENT_RATE = Double.parseDouble(properties.getProperty("keybasedsleep.tfidf.event.rate"));
        String TWEETS_TABLE = properties.getProperty("keybasedsleep.tweets.table");
        String COUNTS_TABLE = properties.getProperty("keybasedsleep.counts.table");
        String EVENTS_TABLE = properties.getProperty("keybasedsleep.events.table");
        String PROCESSED_TABLE = properties.getProperty("keybasedsleep.processed.table");
        String PROCESSTIMES_TABLE = properties.getProperty("keybasedsleep.processtimes.table");

        int CAN_TASK_NUM= Integer.parseInt(properties.getProperty("keybasedsleep.can.taskNum"));
        int USA_TASK_NUM= Integer.parseInt(properties.getProperty("keybasedsleep.usa.taskNum"));
        int NUM_DETECTORS= Integer.parseInt(properties.getProperty("keybasedsleep.num.detectors.per.country"));



        System.out.println("Count threshold " + COUNT_THRESHOLD);
        TopologyHelper.createFolder(Constants.RESULT_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.IMAGES_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.TIMEBREAKDOWN_FILE_PATH + FILENUM);

        System.out.println("Preparing Bolts...");
        TopologyBuilder builder = new TopologyBuilder();


        CassandraDaoKeyBased cassandraDao = new CassandraDaoKeyBased(TWEETS_TABLE, COUNTS_TABLE, EVENTS_TABLE, PROCESSED_TABLE, PROCESSTIMES_TABLE);
        CassandraSpoutKeyBasedWithSleep cassandraSpout = new CassandraSpoutKeyBasedWithSleep(cassandraDao,
                Integer.parseInt(properties.getProperty("keybasedsleep.compare.size")), FILENUM,CAN_TASK_NUM+USA_TASK_NUM+NUM_DETECTORS*2+2);
        WordCountBoltKeyBasedWithSleep countBoltCAN = new WordCountBoltKeyBasedWithSleep(COUNT_THRESHOLD, FILENUM, cassandraDao);
        WordCountBoltKeyBasedWithSleep countBoltUSA = new WordCountBoltKeyBasedWithSleep(COUNT_THRESHOLD, FILENUM, cassandraDao);
        EventDetectorWithCassandraBoltKeyBasedWithSleep eventDetectorBolt1 = new EventDetectorWithCassandraBoltKeyBasedWithSleep(cassandraDao,
                Constants.RESULT_FILE_PATH, FILENUM, TFIDF_EVENT_RATE);
        EventDetectorWithCassandraBoltKeyBasedWithSleep eventDetectorBolt2 = new EventDetectorWithCassandraBoltKeyBasedWithSleep(cassandraDao,
                Constants.RESULT_FILE_PATH, FILENUM, TFIDF_EVENT_RATE);

        EventCompareBoltKeyBasedWithSleep eventCompareBolt = new EventCompareBoltKeyBasedWithSleep(cassandraDao, FILENUM);
        builder.setSpout(Constants.CASS_SPOUT_ID, cassandraSpout,1);

        builder.setBolt(Constants.COUNTRY1_COUNT_BOLT_ID, countBoltUSA,USA_TASK_NUM).
                fieldsGrouping(Constants.CASS_SPOUT_ID, "USA", new Fields("word"));
        builder.setBolt(Constants.COUNTRY2_COUNT_BOLT_ID, countBoltCAN,CAN_TASK_NUM).
                fieldsGrouping(Constants.CASS_SPOUT_ID, "CAN", new Fields("word"));

        builder.setBolt( Constants.COUNTRY1_EVENT_DETECTOR_BOLT, eventDetectorBolt1,NUM_DETECTORS).
                shuffleGrouping(Constants.COUNTRY1_COUNT_BOLT_ID);
        builder.setBolt( Constants.COUNTRY2_EVENT_DETECTOR_BOLT, eventDetectorBolt2,NUM_DETECTORS).
                shuffleGrouping(Constants.COUNTRY2_COUNT_BOLT_ID);

        builder.setBolt( Constants.EVENT_COMPARE_BOLT, eventCompareBolt,1).
                globalGrouping(Constants.COUNTRY1_EVENT_DETECTOR_BOLT).
                globalGrouping(Constants.COUNTRY2_EVENT_DETECTOR_BOLT);

        return builder.createTopology();
    }



    public static StormTopology prepareBoltsForCassandraSpoutKeyBased(Properties properties) throws Exception {
        int COUNT_THRESHOLD = Integer.parseInt(properties.getProperty("keybased.count.threshold"));
        String FILENUM = properties.getProperty("keybased.file.number");
        double TFIDF_EVENT_RATE = Double.parseDouble(properties.getProperty("keybased.tfidf.event.rate"));
        String TWEETS_TABLE = properties.getProperty("keybased.tweets.table");
        String COUNTS_TABLE = properties.getProperty("keybased.counts.table");
        String EVENTS_TABLE = properties.getProperty("keybased.events.table");
        String PROCESSED_TABLE = properties.getProperty("keybased.processed.table");
        String PROCESSTIMES_TABLE = properties.getProperty("keybased.processtimes.table");

        int CAN_TASK_NUM= Integer.parseInt(properties.getProperty("keybased.can.taskNum"));
        int USA_TASK_NUM= Integer.parseInt(properties.getProperty("keybased.usa.taskNum"));
        int NUM_WORKERS= Integer.parseInt(properties.getProperty("keybased.num.workers"));
        int NUM_DETECTORS= Integer.parseInt(properties.getProperty("keybased.num.detectors.per.country"));
        int NUM_COUNTRIES= Integer.parseInt(properties.getProperty("keybased.num.countries"));


        System.out.println("Count threshold " + COUNT_THRESHOLD);
        TopologyHelper.createFolder(Constants.RESULT_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.IMAGES_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.TIMEBREAKDOWN_FILE_PATH + FILENUM);

        System.out.println("Preparing Bolts...");
        TopologyBuilder builder = new TopologyBuilder();

        CassandraDaoKeyBased cassandraDao = new CassandraDaoKeyBased(TWEETS_TABLE, COUNTS_TABLE, EVENTS_TABLE, PROCESSED_TABLE, PROCESSTIMES_TABLE);
        CassandraSpoutKeyBased cassandraSpout = new CassandraSpoutKeyBased(cassandraDao, FILENUM, USA_TASK_NUM, CAN_TASK_NUM, NUM_WORKERS, NUM_DETECTORS*NUM_COUNTRIES,CAN_TASK_NUM+USA_TASK_NUM+NUM_DETECTORS*2+2);
        WordCountBoltKeyBased countBoltUSA = new WordCountBoltKeyBased(COUNT_THRESHOLD, FILENUM, "USA", cassandraDao, NUM_DETECTORS, NUM_WORKERS+CAN_TASK_NUM+USA_TASK_NUM+3);
        WordCountBoltKeyBased countBoltCAN = new WordCountBoltKeyBased(COUNT_THRESHOLD, FILENUM, "CAN", cassandraDao, NUM_DETECTORS, NUM_WORKERS+CAN_TASK_NUM+USA_TASK_NUM+3+NUM_DETECTORS);
        EventDetectorWithCassandraBoltKeyBased eventDetectorBoltUSA = new EventDetectorWithCassandraBoltKeyBased(cassandraDao,
                Constants.RESULT_FILE_PATH, FILENUM, TFIDF_EVENT_RATE, Integer.parseInt(properties.getProperty("keybased.compare.size")), "USA",USA_TASK_NUM);
        EventDetectorWithCassandraBoltKeyBased eventDetectorBoltCAN = new EventDetectorWithCassandraBoltKeyBased(cassandraDao,
                Constants.RESULT_FILE_PATH, FILENUM, TFIDF_EVENT_RATE, Integer.parseInt(properties.getProperty("keybased.compare.size")), "CAN",CAN_TASK_NUM);

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
        int COUNT_THRESHOLD = Integer.parseInt(properties.getProperty("clustering.count.threshold"));
        String FILENUM = properties.getProperty("clustering.file.number");
        String TWEETS_TABLE = properties.getProperty("clustering.tweets.table");
        String EVENTS_TABLE = properties.getProperty("clustering.events.table");
        String EVENTS_WORDBASED_TABLE = properties.getProperty("clustering.events_wordbased.table");
        String CLUSTER_TABLE = properties.getProperty("clustering.clusters.table");
        String PROCESSEDTWEET_TABLE = properties.getProperty("clustering.processed_tweets.table");
        String PROCESSTIMES_TABLE = properties.getProperty("clustering.processtimes.table");
        String TWEETSANDCLUSTER_TABLE = properties.getProperty("clustering.tweetsandcluster.table");
        long START_ROUND = Long.parseLong(properties.getProperty("clustering.start.round"));
        long END_ROUND = Long.parseLong(properties.getProperty("clustering.end.round"));

        int CAN_TASK_NUM= Integer.parseInt(properties.getProperty("clustering.can.taskNum"));
        int USA_TASK_NUM= Integer.parseInt(properties.getProperty("clustering.usa.taskNum"));
        int NUM_WORKERS= Integer.parseInt(properties.getProperty("clustering.num.workers"));

        System.out.println("Count threshold " + COUNT_THRESHOLD);
        TopologyHelper.createFolder(Constants.RESULT_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.IMAGES_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.TIMEBREAKDOWN_FILE_PATH + FILENUM);

        CassandraDao cassandraDao = new CassandraDao(TWEETS_TABLE, CLUSTER_TABLE, EVENTS_TABLE, EVENTS_WORDBASED_TABLE, PROCESSEDTWEET_TABLE, PROCESSTIMES_TABLE, TWEETSANDCLUSTER_TABLE);
        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + FILENUM + "/" + "sout.txt", "Preparing Bolts...");
        TopologyBuilder builder = new TopologyBuilder();

        CassandraSpoutClustering cassandraSpoutClustering = new CassandraSpoutClustering(cassandraDao, FILENUM, START_ROUND, END_ROUND, CAN_TASK_NUM, USA_TASK_NUM, NUM_WORKERS,CAN_TASK_NUM+USA_TASK_NUM+3);

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


    public static StormTopology prepareBoltsForCassandraSpoutHybrid(Properties properties) throws Exception {
        int COUNT_THRESHOLD = Integer.parseInt(properties.getProperty("hybrid.count.threshold"));
        String FILENUM = properties.getProperty("hybrid.file.number");
        String TWEETS_TABLE = properties.getProperty("hybrid.tweets.table");
        String COUNTS_TABLE = properties.getProperty("hybrid.counts.table");
        String EVENTS_TABLE = properties.getProperty("hybrid.events.table");
        String EVENTS_KEYBASED_TABLE = properties.getProperty("hybrid.eventskeybased.table");
        String CLUSTER_TABLE = properties.getProperty("hybrid.clusters.table");
        String PROCESSEDTWEET_TABLE = properties.getProperty("hybrid.processed.table");
        String PROCESSTIMES_TABLE = properties.getProperty("hybrid.processtimes.table");
        String TWEETSANDCLUSTER_TABLE = properties.getProperty("clustering.tweetsandcluster.table");

        double TFIDF_EVENT_RATE = Double.parseDouble(properties.getProperty("hybrid.tfidf.event.rate"));

        int CAN_TASK_NUM= Integer.parseInt(properties.getProperty("hybrid.can.taskNum"));
        int USA_TASK_NUM= Integer.parseInt(properties.getProperty("hybrid.usa.taskNum"));
        int NUM_WORKERS= Integer.parseInt(properties.getProperty("hybrid.num.workers"));
        int NUM_FINDERS= Integer.parseInt(properties.getProperty("hybrid.num.detectors.per.country"));

        System.out.println("Count threshold " + COUNT_THRESHOLD);
        TopologyHelper.createFolder(Constants.RESULT_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.IMAGES_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.TIMEBREAKDOWN_FILE_PATH + FILENUM);

        CassandraDaoHybrid cassandraDao = new CassandraDaoHybrid(TWEETS_TABLE, COUNTS_TABLE, EVENTS_TABLE, PROCESSEDTWEET_TABLE, PROCESSTIMES_TABLE, CLUSTER_TABLE, EVENTS_KEYBASED_TABLE, TWEETSANDCLUSTER_TABLE);

        TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + FILENUM + "/" + "sout.txt", "Preparing Bolts...");
        TopologyBuilder builder = new TopologyBuilder();

        CassandraSpoutHybrid cassandraSpoutClustering = new CassandraSpoutHybrid(cassandraDao, FILENUM, USA_TASK_NUM, CAN_TASK_NUM, NUM_WORKERS, NUM_FINDERS, CAN_TASK_NUM+USA_TASK_NUM+4+NUM_FINDERS*2);
        WordCountBoltHybrid countBoltUSA = new WordCountBoltHybrid(COUNT_THRESHOLD, FILENUM, "USA", cassandraDao, NUM_FINDERS, NUM_WORKERS+CAN_TASK_NUM+USA_TASK_NUM+5);
        WordCountBoltHybrid countBoltCAN = new WordCountBoltHybrid(COUNT_THRESHOLD, FILENUM, "CAN", cassandraDao, NUM_FINDERS, NUM_WORKERS+CAN_TASK_NUM+USA_TASK_NUM+5+NUM_FINDERS);

        EventCandidateFinderHybrid eventCandidateFinderHybridUSA = new EventCandidateFinderHybrid(cassandraDao,
                Constants.RESULT_FILE_PATH,FILENUM, TFIDF_EVENT_RATE, Integer.parseInt(properties.getProperty("hybrid.compare.size")), "USA", USA_TASK_NUM);
        EventCandidateFinderHybrid eventCandidateFinderHybridCAN = new EventCandidateFinderHybrid(cassandraDao,
                Constants.RESULT_FILE_PATH,FILENUM, TFIDF_EVENT_RATE, Integer.parseInt(properties.getProperty("hybrid.compare.size")), "CAN", CAN_TASK_NUM);

        ClusteringBoltHybrid clusteringBoltUSA = new ClusteringBoltHybrid(cassandraDao,
                Constants.RESULT_FILE_PATH, FILENUM, TFIDF_EVENT_RATE, Integer.parseInt(properties.getProperty("hybrid.compare.size")), "USA",NUM_FINDERS);
        ClusteringBoltHybrid clusteringBoltCAN = new ClusteringBoltHybrid(cassandraDao,
                Constants.RESULT_FILE_PATH, FILENUM, TFIDF_EVENT_RATE, Integer.parseInt(properties.getProperty("hybrid.compare.size")), "CAN",NUM_FINDERS);

        EventDetectorBoltHybrid eventDetectorBolt = new EventDetectorBoltHybrid(FILENUM, cassandraDao);

        builder.setSpout(Constants.CASS_SPOUT_ID, cassandraSpoutClustering,1);
        builder.setBolt(Constants.COUNTRY2_COUNT_BOLT_ID, countBoltCAN,CAN_TASK_NUM).directGrouping(Constants.CASS_SPOUT_ID);
        builder.setBolt(Constants.COUNTRY1_COUNT_BOLT_ID, countBoltUSA,USA_TASK_NUM).directGrouping(Constants.CASS_SPOUT_ID);

        builder.setBolt( Constants.COUNTRY1_EVENT_FINDER_BOLT, eventCandidateFinderHybridUSA,NUM_FINDERS).directGrouping(Constants.COUNTRY1_COUNT_BOLT_ID);
        builder.setBolt( Constants.COUNTRY2_EVENT_FINDER_BOLT, eventCandidateFinderHybridCAN,NUM_FINDERS).directGrouping(Constants.COUNTRY2_COUNT_BOLT_ID);

        builder.setBolt(Constants.COUNTRY2_CLUSTERING_BOLT_ID, clusteringBoltCAN, 1).shuffleGrouping(Constants.COUNTRY2_EVENT_FINDER_BOLT);
        builder.setBolt(Constants.COUNTRY1_CLUSTERING_BOLT_ID, clusteringBoltUSA, 1).shuffleGrouping(Constants.COUNTRY1_EVENT_FINDER_BOLT);
        builder.setBolt( Constants.COUNTRY1_EVENT_DETECTOR_BOLT, eventDetectorBolt,1).
                globalGrouping(Constants.COUNTRY1_CLUSTERING_BOLT_ID).
                globalGrouping(Constants.COUNTRY2_CLUSTERING_BOLT_ID);


        System.out.println("Bolts ready");

        return builder.createTopology();
    }
}

