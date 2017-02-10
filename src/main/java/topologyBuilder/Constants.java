package topologyBuilder;


import java.util.concurrent.locks.ReentrantLock;

public class Constants {
    public static final String COUNTRY1_COUNT_BOLT_ID = "count-bolt1";
    public static final String COUNTRY2_COUNT_BOLT_ID = "count-bolt2";
    public static final String COUNTRY1_EVENTDETECTOR_BOLT_ID = "eventdetector-bolt1";
    public static final String COUNTRY2_EVENTDETECTOR_BOLT_ID = "eventdetector-bolt2";
    public static ReentrantLock lock = new ReentrantLock();

//    public static final String IMAGES_FILE_PATH = "/home/ceren/Desktop/thesis/results/charts/";
//    public static final String RESULT_FILE_PATH = "/home/ceren/Desktop/thesis/results/";
//    public static final String TIMEBREAKDOWN_FILE_PATH = "/home/ceren/Desktop/thesis/results/timebreakdown/";

    public static final String IMAGES_FILE_PATH = "/Users/ozlemcerensahin/Desktop/thesis/results/charts/";
    public static final String RESULT_FILE_PATH = "/Users/ozlemcerensahin/Desktop/thesis/results/";
    public static final String TIMEBREAKDOWN_FILE_PATH = "/Users/ozlemcerensahin/Desktop/thesis/results/timebreakdown/";

    public static final String CASS_SPOUT_ID = "cassandraSpout";

}
